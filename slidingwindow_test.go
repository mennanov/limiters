package limiters_test

import (
	"context"
	"time"

	"github.com/google/uuid"

	l "github.com/mennanov/limiters"
)

// slidingWindows returns all the possible SlidingWindow combinations.
func (s *LimitersTestSuite) slidingWindows(capacity int64, rate time.Duration, clock l.Clock, epsilon float64) []*l.SlidingWindow {
	var windows []*l.SlidingWindow
	for _, inc := range s.slidingWindowIncrementers() {
		windows = append(windows, l.NewSlidingWindow(capacity, rate, inc, clock, epsilon))
	}
	return windows
}

func (s *LimitersTestSuite) slidingWindowIncrementers() []l.SlidingWindowIncrementer {
	return []l.SlidingWindowIncrementer{
		l.NewSlidingWindowInMemory(),
		l.NewSlidingWindowRedis(s.redisClient, uuid.New().String()),
		l.NewSlidingWindowMemcached(s.memcacheClient, uuid.New().String()),
		l.NewSlidingWindowDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps),
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
		for _, bucket := range s.slidingWindows(testCase.capacity, testCase.rate, clock, testCase.epsilon) {
			clock.reset()
			for i := 0; i < testCase.requests; i++ {
				w, err := bucket.Limit(context.TODO())
				s.Require().LessOrEqual(i, len(testCase.results)-1)
				s.InDelta(testCase.results[i].w, w, testCase.delta, i)
				s.Equal(testCase.results[i].e, err, i)
				clock.Sleep(w)
			}
		}
	}
}
