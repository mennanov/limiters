package limiters

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// SlidingWindowIncrementer wraps the Increment method.
type SlidingWindowIncrementer interface {
	// Increment increments the request counter for the current window and returns the counter values for the previous
	// window and the current one.
	// TTL is the time duration before the next window.
	Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (prevCount, currCount int64, err error)
}

// SlidingWindow implements a Sliding Window rate limiting algorithm.
//
// It does not require a distributed lock and uses a minimum amount of memory, however it will disallow all the requests
// in case when a client is flooding the service with requests.
// It's the client's responsibility to handle the disallowed request and wait before making a new request again.
type SlidingWindow struct {
	backend  SlidingWindowIncrementer
	clock    Clock
	rate     time.Duration
	capacity int64
	epsilon  float64
}

// NewSlidingWindow creates a new instance of SlidingWindow.
// Capacity is the maximum amount of requests allowed per window.
// Rate is the window size.
// Epsilon is the max-allowed range of difference when comparing the current weighted number of requests with capacity.
func NewSlidingWindow(capacity int64, rate time.Duration, slidingWindowIncrementer SlidingWindowIncrementer, clock Clock, epsilon float64) *SlidingWindow {
	return &SlidingWindow{backend: slidingWindowIncrementer, clock: clock, rate: rate, capacity: capacity, epsilon: epsilon}
}

// Limit returns the time duration to wait before the request can be processed.
// It returns ErrLimitExhausted if the request overflows the capacity.
func (s *SlidingWindow) Limit(ctx context.Context) (time.Duration, error) {
	now := s.clock.Now()
	currWindow := now.Truncate(s.rate)
	prevWindow := currWindow.Add(-s.rate)
	ttl := s.rate - now.Sub(currWindow)
	prev, curr, err := s.backend.Increment(ctx, prevWindow, currWindow, ttl+s.rate)
	if err != nil {
		return 0, err
	}

	total := float64(prev*int64(ttl))/float64(s.rate) + float64(curr)
	if total-float64(s.capacity) >= s.epsilon {
		var wait time.Duration
		if curr <= s.capacity-1 && prev > 0 {
			wait = ttl - time.Duration(float64(s.capacity-1-curr)/float64(prev)*float64(s.rate))
		} else {
			// If prev == 0.
			wait = ttl + time.Duration((1-float64(s.capacity-1)/float64(curr))*float64(s.rate))
		}
		return wait, ErrLimitExhausted
	}
	return 0, nil
}

// SlidingWindowInMemory is an in-memory implementation of SlidingWindowIncrementer.
type SlidingWindowInMemory struct {
	mu           sync.Mutex
	prevC, currC int64
	prevW, currW time.Time
}

// NewSlidingWindowInMemory creates a new instance of SlidingWindowInMemory.
func NewSlidingWindowInMemory() *SlidingWindowInMemory {
	return &SlidingWindowInMemory{}
}

// Increment increments the current window's counter and returns the number of requests in the previous window and the
// current one.
func (s *SlidingWindowInMemory) Increment(ctx context.Context, prev, curr time.Time, _ time.Duration) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if curr != s.currW {
		if prev == s.currW {
			s.prevW = s.currW
			s.prevC = s.currC
		} else {
			s.prevW = time.Time{}
			s.prevC = 0
		}
		s.currW = curr
		s.currC = 0
	}
	s.currC++
	return s.prevC, s.currC, ctx.Err()
}

// SlidingWindowRedis implements SlidingWindow in Redis.
type SlidingWindowRedis struct {
	cli    *redis.Client
	prefix string
}

// NewSlidingWindowRedis creates a new instance of SlidingWindowRedis.
func NewSlidingWindowRedis(cli *redis.Client, prefix string) *SlidingWindowRedis {
	return &SlidingWindowRedis{cli: cli, prefix: prefix}
}

// Increment increments the current window's counter in Redis and returns the number of requests in the previous window
// and the current one.
func (s *SlidingWindowRedis) Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (int64, int64, error) {
	var incr *redis.IntCmd
	var prevCountCmd *redis.StringCmd
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = s.cli.Pipelined(func(pipeliner redis.Pipeliner) error {
			currKey := fmt.Sprintf("%d", curr.UnixNano())
			incr = pipeliner.Incr(redisKey(s.prefix, currKey))
			pipeliner.PExpire(redisKey(s.prefix, currKey), ttl)
			prevCountCmd = pipeliner.Get(redisKey(s.prefix, fmt.Sprintf("%d", prev.UnixNano())))
			return nil
		})
	}()

	var prevCount int64
	select {
	case <-done:
		if err == redis.TxFailedErr {
			return 0, 0, errors.Wrap(err, "redis transaction failed")
		} else if err != nil && err.Error() == "redis: nil" { // TODO: is there an exported error of that type?
			prevCount = 0
		} else if err != nil {
			return 0, 0, errors.Wrap(err, "unexpected error from redis")
		} else {
			prevCount, err = strconv.ParseInt(prevCountCmd.Val(), 10, 64)
			if err != nil {
				return 0, 0, errors.Wrap(err, "failed to parse response from redis")
			}
		}
		return prevCount, incr.Val(), nil
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}
