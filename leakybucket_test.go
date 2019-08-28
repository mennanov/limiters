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
	return []*l.LeakyBucket{
		l.NewLeakyBucket(l.NewLockerNoop(), l.NewLeakyBucketInMemory(l.LeakyBucketState{
			Capacity: capacity,
			Rate:     rate,
		}), clock, s.logger),
		l.NewLeakyBucket(
			l.NewLockerEtcd(s.etcdClient, uuid.New().String(), s.logger),
			l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), l.LeakyBucketState{
				Capacity: capacity,
				Rate:     rate,
			}, time.Second), clock, s.logger),
		l.NewLeakyBucket(
			l.NewLockerEtcd(s.etcdClient, uuid.New().String(), s.logger),
			l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), l.LeakyBucketState{
				Capacity: capacity,
				Rate:     rate,
			}, time.Second), clock, s.logger),
	}
}

func (s *LimitersTestSuite) TestLeakyRealClock() {
	capacity := int64(10)
	rate := time.Millisecond * 10
	clock := l.NewSystemClock()
	for _, requestRate := range []time.Duration{rate * 2, rate, rate / 2, rate / 3, rate / 4, 0} {
		for _, bucket := range s.leakyBuckets(capacity, rate, clock) {
			start := clock.Now()
			wg := sync.WaitGroup{}
			for i := int64(0); i < capacity; i++ {
				// No pause for the first request.
				if i > 0 {
					clock.Sleep(requestRate)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					wait, err := bucket.Limit(context.TODO())
					s.Require().NoError(err)
					if wait > 0 {
						clock.Sleep(wait)
					}
				}()
			}
			wg.Wait()
			interval := rate
			if requestRate > rate {
				interval = requestRate
			}
			// Allow 35ms lag for each request.
			// TODO: figure out if this is enough for slow PCs and possibly avoid hard-coding it.
			delta := float64(time.Duration(capacity) * time.Millisecond * 35)
			s.InDelta(interval*time.Duration(capacity-1), clock.Now().Sub(start), delta, "request rate: %d, bucket: %v", requestRate, bucket)
		}
	}
}

func (s *LimitersTestSuite) TestLeakyFakeClock() {
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

func (s *LimitersTestSuite) TestLeakyOverflow() {
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

func (s *LimitersTestSuite) TestLeakyContextCancelled() {
	clock := newFakeClock()
	for _, bucket := range s.leakyBuckets(1, 1, clock) {
		done1 := make(chan struct{})
		go func() {
			defer close(done1)
			// The context is expired shortly after it is created.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := bucket.Limit(ctx)
			s.Error(err, bucket)
		}()
		done2 := make(chan struct{})
		go func() {
			defer close(done2)
			<-done1
			ctx := context.Background()
			_, err := bucket.Limit(ctx)
			s.NoError(err)
		}()
		// Verify that the second go routine succeeded calling the Limit() method.
		<-done2
	}
}

