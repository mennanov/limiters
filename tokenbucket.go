package limiters

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// TokenBucketState represents a state of a token bucket.
type TokenBucketState struct {
	// RefillRate is the tokens refill rate.
	RefillRate time.Duration
	// Capacity is the bucket's capacity.
	Capacity int64
	// Last is the last time the state was updated (Unix timestamp in nanoseconds).
	Last int64
	// Available is the number of available tokens in the bucket.
	Available int64
}

// TokenBucketStateBackend interface encapsulates the logic of retrieving and persisting the state of a TokenBucket.
type TokenBucketStateBackend interface {
	// State gets the current state of the TokenBucket.
	State(ctx context.Context) (TokenBucketState, error)
	// SetState sets (persists) the current state of the TokenBucket.
	SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error
}

// TokenBucket implements the https://en.wikipedia.org/wiki/Token_bucket algorithm.
type TokenBucket struct {
	Locker
	TokenBucketStateBackend
	Clock
	Logger
	mu sync.Mutex
}

// NewTokenBucket creates a new instance of TokenBucket.
func NewTokenBucket(locker Locker, tokenBucketStateBackend TokenBucketStateBackend, clock Clock, logger Logger) *TokenBucket {
	return &TokenBucket{Locker: locker, TokenBucketStateBackend: tokenBucketStateBackend, Clock: clock, Logger: logger}
}

// Take takes tokens from the bucket.
// It returns a zero duration and a nil error if the bucket has sufficient amount of tokens.
// It returns ErrLimitExhausted if the amount of available tokens is less than requested. In this case the returned
// duration is the amount of time to wait to retry the request.
func (t *TokenBucket) Take(ctx context.Context, tokens int64) (time.Duration, error) {
	now := t.Clock.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	fencingToken, err := t.Lock(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := t.Unlock(); err != nil {
			t.Logger.Log(err)
		}
	}()
	state, err := t.TokenBucketStateBackend.State(ctx)
	if err != nil {
		return 0, err
	}
	// Refill the bucket.
	tokensToAdd := (now - state.Last) / int64(state.RefillRate)
	if tokensToAdd > 0 {
		state.Last = now
		if tokensToAdd+state.Available <= state.Capacity {
			state.Available += tokensToAdd
		} else {
			state.Available = state.Capacity
		}
	}

	if tokens > state.Available {
		return state.RefillRate * time.Duration(tokens-state.Available), ErrLimitExhausted
	}
	// Take the tokens from the bucket.
	state.Available -= tokens
	if err := t.SetState(ctx, state, fencingToken); err != nil {
		return 0, err
	}
	return 0, nil
}

// Limit takes 1 token from the bucket.
func (t *TokenBucket) Limit(ctx context.Context) (time.Duration, error) {
	return t.Take(ctx, 1)
}

// TokenBucketInMemory is an in-memory implementation of TokenBucketStateBackend.
type TokenBucketInMemory struct {
	state TokenBucketState
}

// NewTokenBucketInMemory creates a new instance of TokenBucketInMemory.
func NewTokenBucketInMemory(state TokenBucketState) *TokenBucketInMemory {
	return &TokenBucketInMemory{state: state}
}

// State returns the current bucket's state.
func (t *TokenBucketInMemory) State(ctx context.Context) (TokenBucketState, error) {
	return t.state, ctx.Err()
}

// SetState sets the current bucket's state.
func (t *TokenBucketInMemory) SetState(ctx context.Context, state TokenBucketState, _ int64) error {
	t.state = state
	return ctx.Err()
}

const (
	etcdKeyTBLease        = "lease"
	etcdKeyTBRefillRate   = "refill_rate"
	etcdKeyTBCapacity     = "capacity"
	etcdKeyTBAvailable    = "available"
	etcdKeyTBLast         = "last"
	etcdKeyTBFencingToken = "fencing_token"
)

// TokenBucketEtcd is an etcd implementation of a TokenBucketStateBackend.
// Since etcd is a persisted key-value datastore this implementation should only be used for infrequently accessed API
// endpoints where the reliability and consistency of the accesses is more important than a throughput of a rate
// limiter. Aggressive compaction and defragmentation has to be enabled in etcd to prevent the size of the storage
// to grow indefinitely: every change of the state of the bucket will create a new revision in etcd.
type TokenBucketEtcd struct {
	// Initial state of the bucket.
	state TokenBucketState
	// prefix is the etcd key prefix.
	prefix  string
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	ttl     time.Duration
}

// NewTokenBucketEtcd creates a new TokenBucketEtcd instance.
// Prefix is used as an etcd key prefix for all keys stored in etcd by this algorithm.
// TTL is a TTL of the etcd lease in seconds used to store all the keys: all they keys are automatically deleted after
// the TTL expires.
func NewTokenBucketEtcd(cli *clientv3.Client, prefix string, state TokenBucketState, ttl time.Duration) *TokenBucketEtcd {
	return &TokenBucketEtcd{
		state:  state,
		prefix: prefix,
		cli:    cli,
		ttl:    ttl,
	}
}

// etcdKey returns a full etcd key from the provided key and prefix.
func etcdKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

// parseEtcdInt64 parses the etcd value into int64.
func parseEtcdInt64(kv *mvccpb.KeyValue) (int64, error) {
	v, err := strconv.ParseInt(string(kv.Value), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse key '%s' as int64", string(kv.Key))
	}
	return v, nil
}

func incPrefix(p string) string {
	b := []byte(p)
	b[len(b)-1]++
	return string(b)
}

// State gets the bucket's current state from etcd.
// If there is no state available in etcd then the initial bucket's state is returned.
func (t *TokenBucketEtcd) State(ctx context.Context) (TokenBucketState, error) {
	// Get all the keys under the prefix in a single request.
	r, err := t.cli.Get(ctx, t.prefix, clientv3.WithRange(incPrefix(t.prefix)))
	if err != nil {
		return TokenBucketState{}, errors.Wrapf(err, "failed to get keys in range ['%s', '%s') from etcd", t.prefix, incPrefix(t.prefix))
	}
	if len(r.Kvs) < 6 {
		return t.state, nil
	}
	state := TokenBucketState{}
	parsed := 0
	for _, kv := range r.Kvs {
		switch string(kv.Key) {
		case etcdKey(t.prefix, etcdKeyTBAvailable):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Available = v
			parsed |= 1

		case etcdKey(t.prefix, etcdKeyTBCapacity):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Capacity = v
			parsed |= 2

		case etcdKey(t.prefix, etcdKeyTBLast):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Last = v
			parsed |= 4

		case etcdKey(t.prefix, etcdKeyTBRefillRate):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.RefillRate = time.Duration(v)
			parsed |= 8

		case etcdKey(t.prefix, etcdKeyTBLease):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			t.leaseID = clientv3.LeaseID(v)
			parsed |= 16
		}
	}
	if parsed != 31 {
		return TokenBucketState{}, errors.New("failed to get state from etcd: some keys are missing")
	}
	return state, nil
}

// createLease creates a new lease in etcd and updates the t.leaseID value.
func (t *TokenBucketEtcd) createLease(ctx context.Context) error {
	lease, err := t.cli.Grant(ctx, int64(t.ttl/time.Nanosecond))
	if err != nil {
		return errors.Wrap(err, "failed to create a new lease in etcd")
	}
	t.leaseID = lease.ID
	return nil
}

// save saves the state to etcd using the existing lease and the fencing token.
func (t *TokenBucketEtcd) save(ctx context.Context, fencingToken int64) error {
	// Put the keys only if the fencing token does not exist or its value is <= the given value.
	// TODO: figure out how to do that in 1 RPC instead of 3 (worst case). OR logic is needed in the IF stmt.
	for _, cs := range []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(etcdKey(t.prefix, etcdKeyTBFencingToken)), "=", fmt.Sprintf("%d", fencingToken)),
		clientv3.Compare(clientv3.CreateRevision(etcdKey(t.prefix, etcdKeyTBFencingToken)), "=", 0),
		clientv3.Compare(clientv3.Value(etcdKey(t.prefix, etcdKeyTBFencingToken)), "<", fmt.Sprintf("%d", fencingToken)),
	} {
		r, err := t.cli.Txn(ctx).If(cs).Then(
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBAvailable), fmt.Sprintf("%d", t.state.Available), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLast), fmt.Sprintf("%d", t.state.Last), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLease), fmt.Sprintf("%d", t.leaseID), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBRefillRate), fmt.Sprintf("%d", t.state.RefillRate), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBCapacity), fmt.Sprintf("%d", t.state.Capacity), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBFencingToken), fmt.Sprintf("%d", fencingToken), clientv3.WithLease(t.leaseID)),
		).Commit()
		if err != nil {
			return errors.Wrap(err, "failed to commit a transaction to etcd")
		}

		if r.Succeeded {
			return nil
		}
	}
	return errors.New("failed to save keys to etcd: fencing token expired")
}

// SetState updates the state of the bucket.
func (t *TokenBucketEtcd) SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error {
	t.state = state
	if t.leaseID == 0 {
		// Lease does not exist, create one.
		if err := t.createLease(ctx); err != nil {
			return err
		}
		// No need to send KeepAlive for the newly creates lease: save the state immediately.
		return t.save(ctx, fencingToken)
	}
	// Send the KeepAlive request to extend the existing lease.
	if _, err := t.cli.KeepAliveOnce(ctx, t.leaseID); err == rpctypes.ErrLeaseNotFound {
		// Create a new lease since the current one has expired.
		if err := t.createLease(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return errors.Wrapf(err, "failed to extend the lease '%d'", t.leaseID)
	}
	return t.save(ctx, fencingToken)
}

const (
	redisKeyTBAvailable    = "available"
	redisKeyTBCapacity     = "capacity"
	redisKeyTBRefillRate   = "refill_rate"
	redisKeyTBLast         = "last"
	redisKeyTBFencingToken = "fencing_token"
)

func redisKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

// TokenBucketRedis is a Redis implementation of a TokenBucketStateBackend.
type TokenBucketRedis struct {
	// Initial state of the bucket.
	state        TokenBucketState
	cli          *redis.Client
	prefix       string
	ttl          time.Duration
	fencingToken int64
}

// NewTokenBucketRedis creates a new TokenBucketRedis instance.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
// TTL is the TTL of the stored keys.
func NewTokenBucketRedis(cli *redis.Client, prefix string, state TokenBucketState, ttl time.Duration) *TokenBucketRedis {
	return &TokenBucketRedis{state: state, cli: cli, prefix: prefix, ttl: ttl}
}

// State gets the bucket's state from Redis.
func (t *TokenBucketRedis) State(ctx context.Context) (TokenBucketState, error) {
	var values []interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		values, err = t.cli.MGet(
			redisKey(t.prefix, redisKeyTBRefillRate),
			redisKey(t.prefix, redisKeyTBCapacity),
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
			redisKey(t.prefix, redisKeyTBFencingToken),
		).Result()
		done <- struct{}{}
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return TokenBucketState{}, ctx.Err()
	}

	if err != nil {
		return TokenBucketState{}, errors.Wrap(err, "failed to get keys from redis")
	}
	nilAny := false
	for _, v := range values {
		if v == nil {
			nilAny = true
			break
		}
	}
	if nilAny || err == redis.Nil {
		// Keys don't exist, return the initial state.
		return t.state, nil
	}

	refillRate, err := strconv.ParseInt(values[0].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	capacity, err := strconv.ParseInt(values[1].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	last, err := strconv.ParseInt(values[2].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	available, err := strconv.ParseInt(values[3].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	t.fencingToken, err = strconv.ParseInt(values[4].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	return TokenBucketState{
		RefillRate: time.Duration(refillRate),
		Capacity:   capacity,
		Last:       last,
		Available:  available,
	}, nil
}

// SetState updates the state in Redis.
func (t *TokenBucketRedis) SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error {
	t.state = state
	var result interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		result, err = t.cli.Eval(`
	local token = tonumber(redis.call('get', KEYS[1])) or 0
	if token <= tonumber(ARGV[1]) then
		return {
			redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[6]),
			redis.call('set', KEYS[2], ARGV[2], 'PX', ARGV[6]),
			redis.call('set', KEYS[3], ARGV[3], 'PX', ARGV[6]),
			redis.call('set', KEYS[4], ARGV[4], 'PX', ARGV[6]),
			redis.call('set', KEYS[5], ARGV[5], 'PX', ARGV[6]),
		}
	else
		return 0
	end
	`, []string{
			redisKey(t.prefix, redisKeyTBFencingToken),
			redisKey(t.prefix, redisKeyTBRefillRate),
			redisKey(t.prefix, redisKeyTBCapacity),
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
		},
			fencingToken,
			int64(state.RefillRate),
			state.Capacity,
			state.Last,
			state.Available,
			// TTL in milliseconds: 1e9/1e6.
			int64(t.ttl/time.Microsecond)).Result()
		done <- struct{}{}
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return ctx.Err()
	}

	if err != nil || !redisResponseOK(result, 5) {
		return errors.Wrap(err, "failed to save keys to redis")
	}
	return nil
}
