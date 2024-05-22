package limiters

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// ConcurrentBufferBackend wraps the Add and Remove methods.
type ConcurrentBufferBackend interface {
	// Add adds the request with the given key to the buffer and returns the total number of requests in it.
	Add(ctx context.Context, key string) (int64, error)
	// Remove removes the request from the buffer.
	Remove(ctx context.Context, key string) error
}

// ConcurrentBuffer implements a limiter that allows concurrent requests up to the given capacity.
type ConcurrentBuffer struct {
	locker   DistLocker
	backend  ConcurrentBufferBackend
	logger   Logger
	capacity int64
	mu       sync.Mutex
}

// NewConcurrentBuffer creates a new ConcurrentBuffer instance.
func NewConcurrentBuffer(locker DistLocker, concurrentStateBackend ConcurrentBufferBackend, capacity int64, logger Logger) *ConcurrentBuffer {
	return &ConcurrentBuffer{locker: locker, backend: concurrentStateBackend, capacity: capacity, logger: logger}
}

// Limit puts the request identified by the key in a buffer.
func (c *ConcurrentBuffer) Limit(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.locker.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := c.locker.Unlock(ctx); err != nil {
			c.logger.Log(err)
		}
	}()
	// Optimistically add the new request.
	counter, err := c.backend.Add(ctx, key)
	if err != nil {
		return err
	}
	if counter > c.capacity {
		// Rollback the Add() operation.
		if err = c.backend.Remove(ctx, key); err != nil {
			c.logger.Log(err)
		}
		return ErrLimitExhausted
	}
	return nil
}

// Done removes the request identified by the key from the buffer.
func (c *ConcurrentBuffer) Done(ctx context.Context, key string) error {
	return c.backend.Remove(ctx, key)
}

// ConcurrentBufferInMemory is an in-memory implementation of ConcurrentBufferBackend.
type ConcurrentBufferInMemory struct {
	clock    Clock
	ttl      time.Duration
	mu       sync.Mutex
	registry *Registry
}

// NewConcurrentBufferInMemory creates a new instance of ConcurrentBufferInMemory.
// When the TTL of a key exceeds the key is removed from the buffer. This is needed in case if the process that added
// that key to the buffer did not call Done() for some reason.
func NewConcurrentBufferInMemory(registry *Registry, ttl time.Duration, clock Clock) *ConcurrentBufferInMemory {
	return &ConcurrentBufferInMemory{clock: clock, ttl: ttl, registry: registry}
}

// Add adds the request with the given key to the buffer and returns the total number of requests in it.
// It also removes the keys with expired TTL.
func (c *ConcurrentBufferInMemory) Add(ctx context.Context, key string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	c.registry.DeleteExpired(now)
	c.registry.GetOrCreate(key, func() interface{} {
		return struct{}{}
	}, c.ttl, now)
	return int64(c.registry.Len()), ctx.Err()
}

// Remove removes the request from the buffer.
func (c *ConcurrentBufferInMemory) Remove(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registry.Delete(key)
	return nil
}

// ConcurrentBufferRedis implements ConcurrentBufferBackend in Redis.
type ConcurrentBufferRedis struct {
	clock Clock
	cli   redis.UniversalClient
	key   string
	ttl   time.Duration
}

// NewConcurrentBufferRedis creates a new instance of ConcurrentBufferRedis.
// When the TTL of a key exceeds the key is removed from the buffer. This is needed in case if the process that added
// that key to the buffer did not call Done() for some reason.
func NewConcurrentBufferRedis(cli redis.UniversalClient, key string, ttl time.Duration, clock Clock) *ConcurrentBufferRedis {
	return &ConcurrentBufferRedis{clock: clock, cli: cli, key: key, ttl: ttl}
}

// Add adds the request with the given key to the sorted set in Redis and returns the total number of requests in it.
// It also removes the keys with expired TTL.
func (c *ConcurrentBufferRedis) Add(ctx context.Context, key string) (int64, error) {
	var countCmd *redis.IntCmd
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = c.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
			// Remove expired items.
			now := c.clock.Now()
			pipeliner.ZRemRangeByScore(ctx, c.key, "-inf", fmt.Sprintf("%d", now.Add(-c.ttl).UnixNano()))
			pipeliner.ZAdd(ctx, c.key, redis.Z{
				Score:  float64(now.UnixNano()),
				Member: key,
			})
			countCmd = pipeliner.ZCard(ctx, c.key)
			return nil
		})
	}()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	case <-done:
		if err != nil {
			return 0, errors.Wrap(err, "failed to add an item to redis set")
		}
		return countCmd.Val(), nil
	}
}

// Remove removes the request identified by the key from the sorted set in Redis.
func (c *ConcurrentBufferRedis) Remove(ctx context.Context, key string) error {
	return errors.Wrap(c.cli.ZRem(ctx, c.key, key).Err(), "failed to remove an item from redis set")
}

// ConcurrentBufferMemcached implements ConcurrentBufferBackend in Memcached.
type ConcurrentBufferMemcached struct {
	clock Clock
	cli   *memcache.Client
	key   string
	ttl   time.Duration
}

// NewConcurrentBufferMemcached creates a new instance of ConcurrentBufferMemcached.
// When the TTL of a key exceeds the key is removed from the buffer. This is needed in case if the process that added
// that key to the buffer did not call Done() for some reason.
func NewConcurrentBufferMemcached(cli *memcache.Client, key string, ttl time.Duration, clock Clock) *ConcurrentBufferMemcached {
	return &ConcurrentBufferMemcached{clock: clock, cli: cli, key: key, ttl: ttl}
}

type SortedSetNode struct {
	CreatedAt int64
	Value     string
}

// Add adds the request with the given key to the slice in Memcached and returns the total number of requests in it.
// It also removes the keys with expired TTL.
func (c *ConcurrentBufferMemcached) Add(ctx context.Context, element string) (int64, error) {
	var err error
	done := make(chan struct{})
	now := c.clock.Now()
	var newNodes []SortedSetNode
	var casId uint64 = 0

	go func() {
		defer close(done)
		var item *memcache.Item
		item, err = c.cli.Get(c.key)
		if err != nil {
			if !errors.Is(err, memcache.ErrCacheMiss) {
				return
			}
		} else {
			casId = item.CasID
			b := bytes.NewBuffer(item.Value)
			var oldNodes []SortedSetNode
			_ = gob.NewDecoder(b).Decode(&oldNodes)
			for _, node := range oldNodes {
				if node.CreatedAt > now.UnixNano() && node.Value != element {
					newNodes = append(newNodes, node)
				}
			}
		}
		newNodes = append(newNodes, SortedSetNode{CreatedAt: now.Add(c.ttl).UnixNano(), Value: element})
		var b bytes.Buffer
		_ = gob.NewEncoder(&b).Encode(newNodes)
		item = &memcache.Item{
			Key:   c.key,
			Value: b.Bytes(),
			CasID: casId,
		}
		if casId > 0 {
			err = c.cli.CompareAndSwap(item)
		} else {
			err = c.cli.Add(item)
		}
	}()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	case <-done:
		if err != nil {
			if errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) || errors.Is(err, memcache.ErrCacheMiss) {
				return c.Add(ctx, element)
			}
			return 0, errors.Wrap(err, "failed to add in memcached")
		}
		return int64(len(newNodes)), nil
	}
}

// Remove removes the request identified by the key from the slice in Memcached.
func (c *ConcurrentBufferMemcached) Remove(ctx context.Context, key string) error {
	var err error
	now := c.clock.Now()
	var newNodes []SortedSetNode
	var casID uint64
	item, err := c.cli.Get(c.key)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil
		}
		return errors.Wrap(err, "failed to Get")
	}
	casID = item.CasID
	var oldNodes []SortedSetNode
	_ = gob.NewDecoder(bytes.NewBuffer(item.Value)).Decode(&oldNodes)
	for _, node := range oldNodes {
		if node.CreatedAt > now.UnixNano() && node.Value != key {
			newNodes = append(newNodes, node)
		}
	}

	var b bytes.Buffer
	_ = gob.NewEncoder(&b).Encode(newNodes)
	item = &memcache.Item{
		Key:   c.key,
		Value: b.Bytes(),
		CasID: casID,
	}
	err = c.cli.CompareAndSwap(item)
	if err != nil && (errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) || errors.Is(err, memcache.ErrCacheMiss)) {
		return c.Remove(ctx, key)
	}
	return errors.Wrap(err, "failed to CompareAndSwap")
}
