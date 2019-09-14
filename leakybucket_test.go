package limiters_test

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	l "github.com/mennanov/limiters"
)

// leakyBuckets returns all the possible leakyBuckets combinations.
func (s *LimitersTestSuite) leakyBuckets(capacity int64, rate time.Duration, clock l.Clock) []*l.LeakyBucket {
	var buckets []*l.LeakyBucket
	for _, locker := range s.lockers(true) {
		for _, backend := range s.leakyBucketBackends() {
			buckets = append(buckets, l.NewLeakyBucket(capacity, rate, locker, backend, clock, s.logger))
		}
	}
	return buckets
}

func (s *LimitersTestSuite) leakyBucketBackends() []l.LeakyBucketStateBackend {
	return []l.LeakyBucketStateBackend{
		l.NewLeakyBucketInMemory(),
		l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), time.Second, false),
		l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), time.Second, true),
		l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), time.Second, false),
		l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), time.Second, true),
	}
}

func (s *LimitersTestSuite) TestLeakyBucketRealClock() {
	capacity := int64(10)
	rate := time.Millisecond * 10
	clock := l.NewSystemClock()
	for _, requestRate := range []time.Duration{rate / 2} {
		for _, bucket := range s.leakyBuckets(capacity, rate, clock) {
			//start := clock.Now()
			wg := sync.WaitGroup{}
			mu := sync.Mutex{}
			var totalWait time.Duration
			for i := int64(0); i < capacity; i++ {
				// No pause for the first request.
				if i > 0 {
					clock.Sleep(requestRate)
				}
				wg.Add(1)
				go func(bucket *l.LeakyBucket) {
					defer wg.Done()
					wait, err := bucket.Limit(context.TODO())
					s.Require().NoError(err)
					if wait > 0 {
						mu.Lock()
						totalWait += wait
						mu.Unlock()
						clock.Sleep(wait)
					}
				}(bucket)
			}
			wg.Wait()
			expectedWait := time.Duration(0)
			if rate > requestRate {
				expectedWait = time.Duration(float64(rate-requestRate) * float64(capacity-1) / 2 * float64(capacity))
			}

			// Allow 5ms lag for each request.
			// TODO: figure out if this is enough for slow PCs and possibly avoid hard-coding it.
			delta := float64(time.Duration(capacity) * time.Millisecond * 20)
			s.InDelta(expectedWait, totalWait, delta, "request rate: %d, bucket: %v", requestRate, bucket)
		}
	}
}

func (s *LimitersTestSuite) TestLeakyBucketFakeClock() {
	capacity := int64(10)
	rate := time.Millisecond * 100
	clock := newFakeClock()
	for _, requestRate := range []time.Duration{rate * 2, rate, rate / 2, rate / 3, rate / 4, 0} {
		for _, bucket := range s.leakyBuckets(capacity, rate, clock) {
			clock.reset()
			start := clock.Now()
			for i := int64(0); i < capacity; i++ {
				// No pause for the first request.
				if i > 0 {
					clock.Sleep(requestRate)
				}
				wait, err := bucket.Limit(context.TODO())
				s.Require().NoError(err)
				clock.Sleep(wait)
			}
			interval := rate
			if requestRate > rate {
				interval = requestRate
			}
			s.Equal(interval*time.Duration(capacity-1), clock.Now().Sub(start), "request rate: %d, bucket: %v", requestRate, bucket)
		}
	}
}

func (s *LimitersTestSuite) TestLeakyBucketOverflow() {
	rate := time.Second
	capacity := int64(2)
	clock := newFakeClock()
	for _, bucket := range s.leakyBuckets(capacity, rate, clock) {
		clock.reset()
		// The first call has no wait since there were no calls before. It does not increment the queue size.
		wait, err := bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(time.Duration(0), wait)
		// The second call increments the queue size by 1.
		wait, err = bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(rate, wait)
		// The third call increments the queue size by 1.
		wait, err = bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(rate*2, wait)
		// The third call overflows the bucket capacity.
		wait, err = bucket.Limit(context.TODO())
		s.Require().Equal(l.ErrLimitExhausted, err)
		s.Equal(rate*3, wait)
		// Move the Clock 1 position forward.
		clock.Sleep(rate)
		// Retry the last call. This time it should succeed.
		wait, err = bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(rate*2, wait)
	}
}
