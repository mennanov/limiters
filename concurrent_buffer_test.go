package limiters_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	l "github.com/mennanov/limiters"
)

func (s *LimitersTestSuite) concurrentBuffers(capacity int64, ttl time.Duration, clock l.Clock) []*l.ConcurrentBuffer {
	var buffers []*l.ConcurrentBuffer
	for _, locker := range s.lockers(true) {
		for _, b := range s.concurrentBufferBackends(ttl, clock) {
			buffers = append(buffers, l.NewConcurrentBuffer(locker, b, capacity, s.logger))
		}
	}

	return buffers
}

func (s *LimitersTestSuite) concurrentBufferBackends(ttl time.Duration, clock l.Clock) []l.ConcurrentBufferBackend {
	return []l.ConcurrentBufferBackend{
		l.NewConcurrentBufferInMemory(l.NewRegistry(), ttl, clock),
		l.NewConcurrentBufferRedis(s.redisClient, uuid.New().String(), ttl, clock),
	}
}

func (s *LimitersTestSuite) TestConcurrentBufferNoOverflow() {
	clock := newFakeClock()
	capacity := int64(10)
	ttl := time.Second
	for _, buffer := range s.concurrentBuffers(capacity, ttl, clock) {
		wg := sync.WaitGroup{}
		for i := int64(0); i < capacity; i++ {
			wg.Add(1)
			go func(i int64, buffer *l.ConcurrentBuffer) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", i)
				s.NoError(buffer.Limit(context.TODO(), key))
				s.NoError(buffer.Done(context.TODO(), key))
			}(i, buffer)
		}
		wg.Wait()
		s.NoError(buffer.Limit(context.TODO(), "last"))
		s.NoError(buffer.Done(context.TODO(), "last"))
	}
}

func (s *LimitersTestSuite) TestConcurrentBufferOverflow() {
	clock := newFakeClock()
	capacity := int64(3)
	ttl := time.Second
	for _, buffer := range s.concurrentBuffers(capacity, ttl, clock) {
		mu := sync.Mutex{}
		var errors []error
		wg := sync.WaitGroup{}
		for i := int64(0); i <= capacity; i++ {
			wg.Add(1)
			go func(i int64, buffer *l.ConcurrentBuffer) {
				defer wg.Done()
				if err := buffer.Limit(context.TODO(), fmt.Sprintf("key%d", i)); err != nil {
					mu.Lock()
					errors = append(errors, err)
					mu.Unlock()
				}
			}(i, buffer)
		}
		wg.Wait()
		s.Equal([]error{l.ErrLimitExhausted}, errors)
	}
}

func (s *LimitersTestSuite) TestConcurrentBufferExpiredKeys() {
	clock := newFakeClock()
	capacity := int64(2)
	ttl := time.Second
	for _, buffer := range s.concurrentBuffers(capacity, ttl, clock) {
		s.Require().NoError(buffer.Limit(context.TODO(), "key1"))
		clock.Sleep(ttl / 2)
		s.Require().NoError(buffer.Limit(context.TODO(), "key2"))
		clock.Sleep(ttl / 2)
		// No error is expected (despite the following request overflows the capacity) as the first key has already
		// expired by this time.
		s.NoError(buffer.Limit(context.TODO(), "key3"))
	}
}
