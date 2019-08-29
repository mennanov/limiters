package limiters_test

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	l "github.com/mennanov/limiters"
)

var tokenBucketUniformTestCases = []struct {
	capacity     int64
	refillRate   time.Duration
	requestCount int64
	requestRate  time.Duration
	missExpected int
	delta        float64
}{
	{
		5,
		time.Millisecond * 100,
		10,
		time.Millisecond * 25,
		3,
		1,
	},
	{
		10,
		time.Millisecond * 100,
		13,
		time.Millisecond * 25,
		0,
		1,
	},
	{
		10,
		time.Millisecond * 100,
		15,
		time.Millisecond * 33,
		2,
		1,
	},
	{
		10,
		time.Millisecond * 100,
		16,
		time.Millisecond * 33,
		2,
		1,
	},
	{
		10,
		time.Millisecond * 10,
		20,
		time.Millisecond * 10,
		0,
		1,
	},
	{
		1,
		time.Millisecond * 20,
		20,
		time.Millisecond * 10,
		10,
		2,
	},
}

// tokenBuckets returns all the possible TokenBucket combinations.
func (s *LimitersTestSuite) tokenBuckets(capacity int64, refillRate time.Duration, clock l.Clock) []*l.TokenBucket {
	var buckets []*l.TokenBucket
	for _, locker := range s.lockers() {
		for _, backend := range s.tokenBucketBackends(capacity, refillRate) {
			buckets = append(buckets, l.NewTokenBucket(locker, backend, clock, s.logger))
		}
	}
	return buckets
}

func (s *LimitersTestSuite) tokenBucketBackends(capacity int64, refillRate time.Duration) []l.TokenBucketStateBackend {
	return []l.TokenBucketStateBackend{
		l.NewTokenBucketInMemory(l.TokenBucketState{
			RefillRate: refillRate,
			Capacity:   capacity,
			Available:  capacity,
		}),
		l.NewTokenBucketEtcd(s.etcdClient, uuid.New().String(), l.TokenBucketState{
			RefillRate: refillRate,
			Capacity:   capacity,
			Available:  capacity,
		}, time.Second),
		l.NewTokenBucketRedis(s.redisClient, uuid.New().String(), l.TokenBucketState{
			RefillRate: refillRate,
			Capacity:   capacity,
			Available:  capacity,
		}, time.Second),
	}
}

func (s *LimitersTestSuite) TestTokenBucketRealClock() {
	clock := l.NewSystemClock()
	for _, testCase := range tokenBucketUniformTestCases {
		for _, bucket := range s.tokenBuckets(testCase.capacity, testCase.refillRate, clock) {
			wg := sync.WaitGroup{}
			// mu guards the miss variable below.
			var mu sync.Mutex
			miss := 0
			for i := int64(0); i < testCase.requestCount; i++ {
				// No pause for the first request.
				if i > 0 {
					clock.Sleep(testCase.requestRate)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := bucket.Limit(context.TODO()); err != nil {
						s.Equal(l.ErrLimitExhausted, err)
						mu.Lock()
						miss++
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			s.InDelta(testCase.missExpected, miss, testCase.delta, testCase)
		}
	}
}

func (s *LimitersTestSuite) TestTokenBucketFakeClock() {
	for _, testCase := range tokenBucketUniformTestCases {
		clock := newFakeClock()
		for _, bucket := range s.tokenBuckets(testCase.capacity, testCase.refillRate, clock) {
			clock.reset()
			miss := 0
			for i := int64(0); i < testCase.requestCount; i++ {
				// No pause for the first request.
				if i > 0 {
					clock.Sleep(testCase.requestRate)
				}
				if _, err := bucket.Limit(context.TODO()); err != nil {
					s.Equal(l.ErrLimitExhausted, err)
					miss++
				}
			}
			s.InDelta(testCase.missExpected, miss, testCase.delta, testCase)
		}
	}
}

func (s *LimitersTestSuite) TestTokenBucketOverflow() {
	clock := newFakeClock()
	rate := time.Second
	for _, bucket := range s.tokenBuckets(2, rate, clock) {
		clock.reset()
		wait, err := bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(time.Duration(0), wait)
		wait, err = bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(time.Duration(0), wait)
		// The third call should fail.
		wait, err = bucket.Limit(context.TODO())
		s.Require().Equal(l.ErrLimitExhausted, err)
		s.Equal(rate, wait)
		clock.Sleep(wait)
		// Retry the last call.
		wait, err = bucket.Limit(context.TODO())
		s.Require().NoError(err)
		s.Equal(time.Duration(0), wait)
	}
}

func (s *LimitersTestSuite) TestTokenBucketContextCancelled() {
	clock := newFakeClock()
	for _, bucket := range s.tokenBuckets(1, 1, clock) {
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

func (s *LimitersTestSuite) TestTokenBucketFencingToken() {
	state := l.TokenBucketState{
		RefillRate: time.Second,
		Capacity:   2,
		Last:       time.Now().UnixNano(),
		Available:  1,
	}
	for _, backend := range s.tokenBucketBackends(1, time.Second) {
		s.Require().NoError(backend.SetState(context.TODO(), state, 2))
		// Set state with an expired fencing token.
		s.Require().Error(backend.SetState(context.TODO(), l.TokenBucketState{}, 1), "%T", backend)
		st, err := backend.State(context.TODO())
		s.Require().NoError(err)
		s.Equal(state, st)
	}
}
