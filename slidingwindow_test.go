package limiters_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	l "github.com/mennanov/limiters"
)

// slidingWindows returns all the possible SlidingWindow combinations.
func (s *LimitersTestSuite) slidingWindows(capacity int64, rate time.Duration, clock l.Clock, epsilon float64) map[string]*l.SlidingWindow {
	windows := make(map[string]*l.SlidingWindow)
	for name, inc := range s.slidingWindowIncrementers() {
		windows[name] = l.NewSlidingWindow(capacity, rate, inc, clock, epsilon)
	}
	return windows
}

func (s *LimitersTestSuite) slidingWindowIncrementers() map[string]l.SlidingWindowIncrementer {
	return map[string]l.SlidingWindowIncrementer{
		"SlidingWindowInMemory":     l.NewSlidingWindowInMemory(),
		"SlidingWindowRedis":        l.NewSlidingWindowRedis(s.redisClient, uuid.New().String()),
		"SlidingWindowRedisCluster": l.NewSlidingWindowRedis(s.redisClusterClient, uuid.New().String()),
		"SlidingWindowMemcached":    l.NewSlidingWindowMemcached(s.memcacheClient, uuid.New().String()),
		"SlidingWindowDynamoDB":     l.NewSlidingWindowDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps),
	}
}

var slidingWindowTestCases = []struct {
	capacity int64
	rate     time.Duration
	epsilon  float64
	results  []struct {
		w time.Duration
		e error
	}
	requests int
	delta    float64
}{
	{
		capacity: 1,
		rate:     time.Second,
		epsilon:  1e-9,
		requests: 6,
		results: []struct {
			w time.Duration
			e error
		}{
			{
				0, nil,
			},
			{
				time.Second * 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second * 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second * 2, l.ErrLimitExhausted,
			},
		},
	},
	{
		capacity: 2,
		rate:     time.Second,
		epsilon:  3e-9,
		requests: 10,
		delta:    1,
		results: []struct {
			w time.Duration
			e error
		}{
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				time.Second + time.Second*2/3, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second/3 + time.Second/2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
		},
	},
	{
		capacity: 3,
		rate:     time.Second,
		epsilon:  1e-9,
		requests: 11,
		delta:    0,
		results: []struct {
			w time.Duration
			e error
		}{
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				time.Second + time.Second/2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
		},
	},
	{
		capacity: 4,
		rate:     time.Second,
		epsilon:  1e-9,
		requests: 17,
		delta:    0,
		results: []struct {
			w time.Duration
			e error
		}{
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				time.Second + time.Second*2/5, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second * 2 / 5, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second/5 + time.Second/4, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
		},
	},
	{
		capacity: 5,
		rate:     time.Second,
		epsilon:  3e-9,
		requests: 18,
		delta:    1,
		results: []struct {
			w time.Duration
			e error
		}{
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				0, nil,
			},
			{
				time.Second + time.Second/3, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second * 2 / 6, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second * 2 / 6, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
			{
				time.Second / 2, l.ErrLimitExhausted,
			},
			{
				0, nil,
			},
		},
	},
}

func (s *LimitersTestSuite) TestSlidingWindowOverflowAndWait() {
	clock := newFakeClockWithTime(time.Date(2019, 9, 3, 0, 0, 0, 0, time.UTC))
	for _, testCase := range slidingWindowTestCases {
		for name, bucket := range s.slidingWindows(testCase.capacity, testCase.rate, clock, testCase.epsilon) {
			s.Run(name, func() {
				clock.reset()
				for i := 0; i < testCase.requests; i++ {
					w, err := bucket.Limit(context.TODO())
					s.Require().LessOrEqual(i, len(testCase.results)-1)
					s.InDelta(testCase.results[i].w, w, testCase.delta, i)
					s.Equal(testCase.results[i].e, err, i)
					clock.Sleep(w)
				}
			})
		}
	}
}

func BenchmarkSlidingWindows(b *testing.B) {
	s := new(LimitersTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()
	capacity := int64(1)
	rate := time.Second
	clock := newFakeClock()
	epsilon := 1e-9
	windows := s.slidingWindows(capacity, rate, clock, epsilon)
	for name, window := range windows {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := window.Limit(context.TODO())
				s.Require().NoError(err)
			}
		})
	}
	s.TearDownSuite()
}
