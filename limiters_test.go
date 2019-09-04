package limiters_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	l "github.com/mennanov/limiters"
)

type fakeClock struct {
	mu      sync.Mutex
	initial time.Time
	t       time.Time
}

func newFakeClock() *fakeClock {
	now := time.Now()
	return &fakeClock{t: now, initial: now}
}

func newFakeClockWithTime(t time.Time) *fakeClock {
	return &fakeClock{t: t, initial: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Sleep(d time.Duration) {
	if d == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *fakeClock) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.initial
}

type LimitersTestSuite struct {
	suite.Suite
	etcdClient  *clientv3.Client
	redisClient *redis.Client
	logger      *l.StdLogger
}

func (s *LimitersTestSuite) SetupSuite() {
	var err error
	s.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","),
		DialTimeout: time.Second,
	})
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	s.logger = l.NewStdLogger()
}

func (s *LimitersTestSuite) TearDownSuite() {
	s.Assert().NoError(s.etcdClient.Close())
	s.Assert().NoError(s.redisClient.Close())
}

func TestBucketTestSuite(t *testing.T) {
	suite.Run(t, new(LimitersTestSuite))
}

func (s *LimitersTestSuite) lockers() []l.Locker {
	return []l.Locker{
		l.NewLockerNoop(),
		l.NewLockerEtcd(s.etcdClient, uuid.New().String(), s.logger),
	}
}

func (s *LimitersTestSuite) limiters(capacity int64, rate time.Duration, clock l.Clock, epsilon float64) []l.Limiter {
	var limiters []l.Limiter
	for _, b := range s.tokenBuckets(capacity, rate, clock) {
		limiters = append(limiters, b)
	}
	for _, b := range s.leakyBuckets(capacity, rate, clock) {
		limiters = append(limiters, b)
	}
	for _, w := range s.fixedWindows(capacity, rate, clock) {
		limiters = append(limiters, w)
	}
	for _, w := range s.slidingWindows(capacity, rate, clock, epsilon) {
		limiters = append(limiters, w)
	}
	return limiters
}

func (s *LimitersTestSuite) TestLimitContextCancelled() {
	clock := newFakeClock()
	for _, limiter := range s.limiters(2, time.Second, clock, 1e-9) {
		done1 := make(chan struct{})
		go func(limiter l.Limiter) {
			defer close(done1)
			// The context is expired shortly after it is created.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := limiter.Limit(ctx)
			s.Error(err, "%T", limiter)
		}(limiter)
		done2 := make(chan struct{})
		go func(limiter l.Limiter) {
			defer close(done2)
			<-done1
			ctx := context.Background()
			_, err := limiter.Limit(ctx)
			s.NoError(err, "%T", limiter)
		}(limiter)
		// Verify that the second go routine succeeded calling the Limit() method.
		<-done2
	}
}
