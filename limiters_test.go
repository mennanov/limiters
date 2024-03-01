package limiters_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/redis/go-redis/v9"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

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
	etcdClient         *clientv3.Client
	redisClient        redis.UniversalClient
	consulClient       *api.Client
	zkConn             *zk.Conn
	logger             *l.StdLogger
	dynamodbClient     *dynamodb.Client
	dynamoDBTableProps l.DynamoDBTableProperties
	memcacheClient     *memcache.Client
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
	s.consulClient, err = api.NewClient(&api.Config{Address: os.Getenv("CONSUL_ADDR")})
	s.Require().NoError(err)
	s.zkConn, _, err = zk.Connect(strings.Split(os.Getenv("ZOOKEEPER_ENDPOINTS"), ","), time.Second)
	s.Require().NoError(err)
	s.logger = l.NewStdLogger()

	awsResolver := aws.EndpointResolverWithOptionsFunc(func(service string, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               fmt.Sprintf("http://%s", os.Getenv("AWS_ADDR")),
			SigningRegion:     "us-east-1",
			HostnameImmutable: true,
		}, nil
	})
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(awsResolver),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	s.Require().NoError(err)

	s.dynamodbClient = dynamodb.NewFromConfig(awsCfg)

	_ = DeleteTestDynamoDBTable(context.Background(), s.dynamodbClient)
	s.Require().NoError(CreateTestDynamoDBTable(context.Background(), s.dynamodbClient))
	s.dynamoDBTableProps, err = l.LoadDynamoDBTableProperties(context.Background(), s.dynamodbClient, testDynamoDBTableName)
	s.Require().NoError(err)

	s.memcacheClient = memcache.New(strings.Split(os.Getenv("MEMCACHED_ADDR"), ",")...)
	s.Require().NoError(s.memcacheClient.Ping())
}

func (s *LimitersTestSuite) TearDownSuite() {
	s.Assert().NoError(s.etcdClient.Close())
	s.Assert().NoError(s.redisClient.Close())
	s.Assert().NoError(DeleteTestDynamoDBTable(context.Background(), s.dynamodbClient))
	s.Assert().NoError(s.memcacheClient.Close())
}

func TestLimitersTestSuite(t *testing.T) {
	suite.Run(t, new(LimitersTestSuite))
}

// lockers returns all possible lockers (including noop).
func (s *LimitersTestSuite) lockers(generateKeys bool) map[string]l.DistLocker {
	lockers := s.distLockers(generateKeys)
	lockers["LockNoop"] = l.NewLockNoop()
	return lockers
}

// distLockers returns distributed lockers only.
func (s *LimitersTestSuite) distLockers(generateKeys bool) map[string]l.DistLocker {
	randomKey := uuid.New().String()
	consulKey := randomKey
	etcdKey := randomKey
	zkKey := "/" + randomKey
	redisKey := randomKey
	memcacheKey := randomKey
	if !generateKeys {
		consulKey = "dist_locker"
		etcdKey = "dist_locker"
		zkKey = "/dist_locker"
		redisKey = "dist_locker"
		memcacheKey = "dist_locker"
	}
	consulLock, err := s.consulClient.LockKey(consulKey)
	s.Require().NoError(err)
	return map[string]l.DistLocker{
		"LockEtcd":      l.NewLockEtcd(s.etcdClient, etcdKey, s.logger),
		"LockConsul":    l.NewLockConsul(consulLock),
		"LockZookeeper": l.NewLockZookeeper(zk.NewLock(s.zkConn, zkKey, zk.WorldACL(zk.PermAll))),
		"LockRedis":     l.NewLockRedis(goredis.NewPool(s.redisClient), redisKey),
		"LockMemcached": l.NewLockMemcached(s.memcacheClient, memcacheKey),
	}
}

func (s *LimitersTestSuite) TestLimitContextCancelled() {
	clock := newFakeClock()
	capacity := int64(2)
	rate := time.Second
	limiters := make(map[string]interface{})
	for n, b := range s.tokenBuckets(capacity, rate, clock) {
		limiters[n] = b
	}
	for n, b := range s.leakyBuckets(capacity, rate, clock) {
		limiters[n] = b
	}
	for n, w := range s.fixedWindows(capacity, rate, clock) {
		limiters[n] = w
	}
	for n, w := range s.slidingWindows(capacity, rate, clock, 1e-9) {
		limiters[n] = w
	}
	for n, b := range s.concurrentBuffers(capacity, rate, clock) {
		limiters[n] = b
	}
	type rateLimiter interface {
		Limit(context.Context) (time.Duration, error)
	}
	type concurrentLimiter interface {
		Limit(context.Context, string) error
	}

	for name, limiter := range limiters {
		s.Run(name, func() {
			done1 := make(chan struct{})
			go func(limiter interface{}) {
				defer close(done1)
				// The context is expired shortly after it is created.
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				switch lim := limiter.(type) {
				case rateLimiter:
					_, err := lim.Limit(ctx)
					s.Error(err, "%T", limiter)

				case concurrentLimiter:
					s.Error(lim.Limit(ctx, "key"), "%T", limiter)
				}

			}(limiter)
			done2 := make(chan struct{})
			go func(limiter interface{}) {
				defer close(done2)
				<-done1
				ctx := context.Background()
				switch lim := limiter.(type) {
				case rateLimiter:
					_, err := lim.Limit(ctx)
					s.NoError(err, "%T", limiter)

				case concurrentLimiter:
					s.NoError(lim.Limit(ctx, "key"), "%T", limiter)
				}
			}(limiter)
			// Verify that the second go routine succeeded calling the Limit() method.
			<-done2
		})
	}
}
