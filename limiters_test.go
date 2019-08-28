package limiters_test

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/go-redis/redis"
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
