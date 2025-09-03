package limiters_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	l "github.com/mennanov/limiters"
	"github.com/redis/go-redis/v9"
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
		time.Millisecond * 200,
		10,
		time.Millisecond * 100,
		5,
		2,
	},
}

// tokenBuckets returns all the possible TokenBucket combinations.
func (s *LimitersTestSuite) tokenBuckets(capacity int64, refillRate, ttl time.Duration, clock l.Clock) map[string]*l.TokenBucket {
	buckets := make(map[string]*l.TokenBucket)
	for lockerName, locker := range s.lockers(true) {
		for backendName, backend := range s.tokenBucketBackends(ttl) {
			buckets[lockerName+":"+backendName] = l.NewTokenBucket(capacity, refillRate, locker, backend, clock, s.logger)
		}
	}

	return buckets
}

func (s *LimitersTestSuite) tokenBucketBackends(ttl time.Duration) map[string]l.TokenBucketStateBackend {
	return map[string]l.TokenBucketStateBackend{
		"TokenBucketInMemory":                  l.NewTokenBucketInMemory(),
		"TokenBucketEtcdNoRaceCheck":           l.NewTokenBucketEtcd(s.etcdClient, uuid.New().String(), ttl, false),
		"TokenBucketEtcdWithRaceCheck":         l.NewTokenBucketEtcd(s.etcdClient, uuid.New().String(), ttl, true),
		"TokenBucketRedisNoRaceCheck":          l.NewTokenBucketRedis(s.redisClient, uuid.New().String(), ttl, false),
		"TokenBucketRedisWithRaceCheck":        l.NewTokenBucketRedis(s.redisClient, uuid.New().String(), ttl, true),
		"TokenBucketRedisClusterNoRaceCheck":   l.NewTokenBucketRedis(s.redisClusterClient, uuid.New().String(), ttl, false),
		"TokenBucketRedisClusterWithRaceCheck": l.NewTokenBucketRedis(s.redisClusterClient, uuid.New().String(), ttl, true),
		"TokenBucketMemcachedNoRaceCheck":      l.NewTokenBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, false),
		"TokenBucketMemcachedWithRaceCheck":    l.NewTokenBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, true),
		"TokenBucketDynamoDBNoRaceCheck":       l.NewTokenBucketDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps, ttl, false),
		"TokenBucketDynamoDBWithRaceCheck":     l.NewTokenBucketDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps, ttl, true),
		"TokenBucketCosmosDBNoRaceCheck":       l.NewTokenBucketCosmosDB(s.cosmosContainerClient, uuid.New().String(), ttl, false),
		"TokenBucketCosmosDBWithRaceCheck":     l.NewTokenBucketCosmosDB(s.cosmosContainerClient, uuid.New().String(), ttl, true),
	}
}

func (s *LimitersTestSuite) TestTokenBucketRealClock() {
	clock := l.NewSystemClock()
	for _, testCase := range tokenBucketUniformTestCases {
		for name, bucket := range s.tokenBuckets(testCase.capacity, testCase.refillRate, time.Second, clock) {
			s.Run(name, func() {
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
					go func(bucket *l.TokenBucket) {
						defer wg.Done()
						if _, err := bucket.Limit(context.TODO()); err != nil {
							s.Equal(l.ErrLimitExhausted, err, "%T %v", bucket, bucket)
							mu.Lock()
							miss++
							mu.Unlock()
						}
					}(bucket)
				}
				wg.Wait()
				s.InDelta(testCase.missExpected, miss, testCase.delta, testCase)
			})
		}
	}
}

func (s *LimitersTestSuite) TestTokenBucketFakeClock() {
	for _, testCase := range tokenBucketUniformTestCases {
		clock := newFakeClock()
		for name, bucket := range s.tokenBuckets(testCase.capacity, testCase.refillRate, time.Second, clock) {
			s.Run(name, func() {
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
			})
		}
	}
}

func (s *LimitersTestSuite) TestTokenBucketOverflow() {
	clock := newFakeClock()
	rate := 100 * time.Millisecond // should be shorter than TTL
	for name, bucket := range s.tokenBuckets(2, rate, time.Second, clock) {
		s.Run(name, func() {
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
		})
	}
}

func (s *LimitersTestSuite) TestTokenBucketReset() {
	clock := newFakeClock()
	rate := 100 * time.Millisecond // should be shorter than TTL
	for name, bucket := range s.tokenBuckets(2, rate, time.Second, clock) {
		s.Run(name, func() {
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
			err = bucket.Reset(context.TODO())
			s.Require().NoError(err)
			// Retry the last call.
			wait, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), wait)
		})
	}
}

func (s *LimitersTestSuite) TestTokenBucketRefill() {
	for name, backend := range s.tokenBucketBackends(time.Second) {
		s.Run(name, func() {
			clock := newFakeClock()

			bucket := l.NewTokenBucket(4, time.Millisecond*100, l.NewLockNoop(), backend, clock, s.logger)
			sleepDurations := []int{150, 90, 50, 70}
			desiredAvailable := []int64{3, 2, 2, 2}

			_, err := bucket.Limit(context.Background())
			s.Require().NoError(err)

			_, err = backend.State(context.Background())
			s.Require().NoError(err, "unable to retrieve backend state")

			for i := range sleepDurations {
				clock.Sleep(time.Millisecond * time.Duration(sleepDurations[i]))

				_, err := bucket.Limit(context.Background())
				s.Require().NoError(err)

				state, err := backend.State(context.Background())
				s.Require().NoError(err, "unable to retrieve backend state")

				s.Require().Equal(desiredAvailable[i], state.Available)
			}
		})
	}
}

// TestTokenBucketMemcachedExpiry verifies Memcached TTL expiry for TokenBucketMemcached (no race check).
func (s *LimitersTestSuite) TestTokenBucketMemcachedExpiry() {
	ttl := time.Second
	backend := l.NewTokenBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, false)
	bucket := l.NewTokenBucket(2, time.Second, l.NewLockNoop(), backend, l.NewSystemClock(), s.logger)
	ctx := context.Background()
	_, err := bucket.Limit(ctx)
	s.Require().NoError(err)
	_, err = bucket.Limit(ctx)
	s.Require().NoError(err)
	state, err := backend.State(ctx)
	s.Require().NoError(err)
	s.Equal(int64(0), state.Available, "Tokens should be depleted after two takes")
	nextTs := time.Now().Add(ttl).Truncate(time.Second).Add(time.Second)
	time.Sleep(nextTs.Sub(time.Now()))
	state, err = backend.State(ctx)
	s.Require().NoError(err)
	s.True(state.Last == 0 && state.Available == 0, "State should be zero after expiry, got: %+v", state)
}

// TestTokenBucketMemcachedExpiryWithRaceCheck verifies Memcached TTL expiry for TokenBucketMemcached (race check enabled).
func (s *LimitersTestSuite) TestTokenBucketMemcachedExpiryWithRaceCheck() {
	ttl := time.Second
	backend := l.NewTokenBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, true)
	bucket := l.NewTokenBucket(2, time.Second, l.NewLockNoop(), backend, l.NewSystemClock(), s.logger)
	ctx := context.Background()
	_, err := bucket.Limit(ctx)
	s.Require().NoError(err)
	_, err = bucket.Limit(ctx)
	s.Require().NoError(err)
	state, err := backend.State(ctx)
	s.Require().NoError(err)
	s.Equal(int64(0), state.Available, "Tokens should be depleted after two takes")
	time.Sleep(ttl + 1500*time.Millisecond)
	state, err = backend.State(ctx)
	s.Require().NoError(err)
	s.True(state.Last == 0 && state.Available == 0, "State should be zero after expiry, got: %+v", state)
}

// setTokenBucketStateInOldFormat is a test utility method for writing state in the old format to Redis.
func setTokenBucketStateInOldFormat(ctx context.Context, cli *redis.Client, prefix string, state l.TokenBucketState, ttl time.Duration) error {
	_, err := cli.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		if err := pipeliner.Set(ctx, fmt.Sprintf("{%s}last", prefix), state.Last, ttl).Err(); err != nil {
			return err
		}
		if err := pipeliner.Set(ctx, fmt.Sprintf("{%s}available", prefix), state.Available, ttl).Err(); err != nil {
			return err
		}

		return nil
	})

	return err
}

// TestTokenBucketRedisBackwardCompatibility tests that the new State method can read data written in the old format.
func (s *LimitersTestSuite) TestTokenBucketRedisBackwardCompatibility() {
	// Create a new TokenBucketRedis instance
	prefix := uuid.New().String()
	backend := l.NewTokenBucketRedis(s.redisClient, prefix, time.Second, false)

	// Write state using old format
	ctx := context.Background()
	expectedState := l.TokenBucketState{
		Last:      12345,
		Available: 67890,
	}

	// Write directly to Redis using old format
	err := setTokenBucketStateInOldFormat(ctx, s.redisClient, prefix, expectedState, time.Second)
	s.Require().NoError(err, "Failed to set state using old format")

	// Read state using new format (State)
	actualState, err := backend.State(ctx)
	s.Require().NoError(err, "Failed to get state using new format")

	// Verify the state is correctly read
	s.Equal(expectedState.Last, actualState.Last, "Last values should match")
	s.Equal(expectedState.Available, actualState.Available, "Available values should match")
}

func (s *LimitersTestSuite) TestTokenBucketNoExpiration() {
	clock := l.NewSystemClock()
	buckets := s.tokenBuckets(1, 3*time.Second, 0, clock)

	// Take all capacity from all buckets
	for _, bucket := range buckets {
		_, _ = bucket.Limit(context.TODO())
	}

	// Wait for 2 seconds to check if it treats 0 TTL as infinite
	clock.Sleep(2 * time.Second)

	// Expect all buckets to be still filled
	for name, bucket := range buckets {
		s.Run(name, func() {
			_, err := bucket.Limit(context.TODO())
			s.Require().Equal(l.ErrLimitExhausted, err)
		})
	}
}

func BenchmarkTokenBuckets(b *testing.B) {
	s := new(LimitersTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()
	capacity := int64(1)
	rate := 100 * time.Millisecond // should be shorter than TTL
	clock := newFakeClock()
	buckets := s.tokenBuckets(capacity, rate, time.Second, clock)
	for name, bucket := range buckets {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := bucket.Limit(context.TODO())
				s.Require().NoError(err)
			}
		})
	}
	s.TearDownSuite()
}
