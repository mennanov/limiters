package limiters_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	l "github.com/mennanov/limiters"
)

// fixedWindows returns all the possible FixedWindow combinations.
func (s *LimitersTestSuite) fixedWindows(capacity int64, rate time.Duration, clock l.Clock) map[string]*l.FixedWindow {
	windows := make(map[string]*l.FixedWindow)
	for name, inc := range s.fixedWindowIncrementers() {
		windows[name] = l.NewFixedWindow(capacity, rate, inc, clock)
	}
	return windows
}

func (s *LimitersTestSuite) fixedWindowIncrementers() map[string]l.FixedWindowIncrementer {
	return map[string]l.FixedWindowIncrementer{
		"FixedWindowInMemory":     l.NewFixedWindowInMemory(),
		"FixedWindowRedis":        l.NewFixedWindowRedis(s.redisClient, uuid.New().String()),
		"FixedWindowRedisCluster": l.NewFixedWindowRedis(s.redisClusterClient, uuid.New().String()),
		"FixedWindowMemcached":    l.NewFixedWindowMemcached(s.memcacheClient, uuid.New().String()),
		"FixedWindowDynamoDB":     l.NewFixedWindowDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps),
		"FixedWindowCosmosDB":     l.NewFixedWindowCosmosDB(s.cosmosContainerClient, uuid.New().String()),
	}
}

var fixedWindowTestCases = []struct {
	capacity     int64
	rate         time.Duration
	requestCount int
	requestRate  time.Duration
	missExpected int
}{
	{
		capacity:     2,
		rate:         time.Millisecond * 100,
		requestCount: 20,
		requestRate:  time.Millisecond * 25,
		missExpected: 10,
	},
	{
		capacity:     4,
		rate:         time.Millisecond * 100,
		requestCount: 20,
		requestRate:  time.Millisecond * 25,
		missExpected: 0,
	},
	{
		capacity:     2,
		rate:         time.Millisecond * 100,
		requestCount: 15,
		requestRate:  time.Millisecond * 33,
		missExpected: 5,
	},
}

func (s *LimitersTestSuite) TestFixedWindowFakeClock() {
	clock := newFakeClockWithTime(time.Date(2019, 8, 30, 0, 0, 0, 0, time.UTC))
	for _, testCase := range fixedWindowTestCases {
		for name, bucket := range s.fixedWindows(testCase.capacity, testCase.rate, clock) {
			s.Run(name, func() {
				clock.reset()
				miss := 0
				for i := 0; i < testCase.requestCount; i++ {
					// No pause for the first request.
					if i > 0 {
						clock.Sleep(testCase.requestRate)
					}
					if _, err := bucket.Limit(context.TODO()); err != nil {
						s.Equal(l.ErrLimitExhausted, err)
						miss++
					}
				}
				s.Equal(testCase.missExpected, miss, testCase)
			})
		}
	}
}

func (s *LimitersTestSuite) TestFixedWindowOverflow() {
	clock := newFakeClockWithTime(time.Date(2019, 8, 30, 0, 0, 0, 0, time.UTC))
	for name, bucket := range s.fixedWindows(2, time.Second, clock) {
		s.Run(name, func() {
			clock.reset()
			w, err := bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), w)
			w, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), w)
			w, err = bucket.Limit(context.TODO())
			s.Require().Equal(l.ErrLimitExhausted, err)
			s.Equal(time.Second, w)
			clock.Sleep(time.Second)
			w, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), w)
		})
	}
}

func (s *LimitersTestSuite) TestFixedWindowDynamoDBPartitionKey() {
	clock := newFakeClockWithTime(time.Date(2019, 8, 30, 0, 0, 0, 0, time.UTC))
	incrementor := l.NewFixedWindowDynamoDB(s.dynamodbClient, "partitionKey1", s.dynamoDBTableProps)
	window := l.NewFixedWindow(2, time.Millisecond*100, incrementor, clock)

	w, err := window.Limit(context.TODO())
	s.Require().NoError(err)
	s.Equal(time.Duration(0), w)
	w, err = window.Limit(context.TODO())
	s.Require().NoError(err)
	s.Equal(time.Duration(0), w)
	// The third call should fail for the "partitionKey1", but succeed for "partitionKey2".
	w, err = window.Limit(l.NewFixedWindowDynamoDBContext(context.Background(), "partitionKey2"))
	s.Require().NoError(err)
	s.Equal(time.Duration(0), w)
}

func BenchmarkFixedWindows(b *testing.B) {
	s := new(LimitersTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()
	capacity := int64(1)
	rate := time.Second
	clock := newFakeClock()
	windows := s.fixedWindows(capacity, rate, clock)
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
