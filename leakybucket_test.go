package limiters_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	l "github.com/mennanov/limiters"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// leakyBuckets returns all the possible leakyBuckets combinations.
func (s *LimitersTestSuite) leakyBuckets(capacity int64, rate, ttl time.Duration, clock l.Clock) map[string]*l.LeakyBucket {
	buckets := make(map[string]*l.LeakyBucket)

	for lockerName, locker := range s.lockers(true) {
		for backendName, backend := range s.leakyBucketBackends(ttl) {
			buckets[lockerName+":"+backendName] = l.NewLeakyBucket(capacity, rate, locker, backend, clock, s.logger)
		}
	}

	return buckets
}

func (s *LimitersTestSuite) leakyBucketBackends(ttl time.Duration) map[string]l.LeakyBucketStateBackend {
	return map[string]l.LeakyBucketStateBackend{
		"LeakyBucketInMemory":                  l.NewLeakyBucketInMemory(),
		"LeakyBucketEtcdNoRaceCheck":           l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), ttl, false),
		"LeakyBucketEtcdWithRaceCheck":         l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), ttl, true),
		"LeakyBucketRedisNoRaceCheck":          l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), ttl, false),
		"LeakyBucketRedisWithRaceCheck":        l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), ttl, true),
		"LeakyBucketRedisClusterNoRaceCheck":   l.NewLeakyBucketRedis(s.redisClusterClient, uuid.New().String(), ttl, false),
		"LeakyBucketRedisClusterWithRaceCheck": l.NewLeakyBucketRedis(s.redisClusterClient, uuid.New().String(), ttl, true),
		"LeakyBucketMemcachedNoRaceCheck":      l.NewLeakyBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, false),
		"LeakyBucketMemcachedWithRaceCheck":    l.NewLeakyBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, true),
		"LeakyBucketDynamoDBNoRaceCheck":       l.NewLeakyBucketDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps, ttl, false),
		"LeakyBucketDynamoDBWithRaceCheck":     l.NewLeakyBucketDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps, ttl, true),
		"LeakyBucketCosmosDBNoRaceCheck":       l.NewLeakyBucketCosmosDB(s.cosmosContainerClient, uuid.New().String(), ttl, false),
		"LeakyBucketCosmosDBWithRaceCheck":     l.NewLeakyBucketCosmosDB(s.cosmosContainerClient, uuid.New().String(), ttl, true),
	}
}

func (s *LimitersTestSuite) TestLeakyBucketRealClock() {
	capacity := int64(10)
	rate := time.Millisecond * 10

	clock := l.NewSystemClock()
	for _, requestRate := range []time.Duration{rate / 2} {
		for name, bucket := range s.leakyBuckets(capacity, rate, 0, clock) {
			s.Run(name, func() {
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
				delta := float64(time.Duration(capacity) * time.Millisecond * 25)
				s.InDelta(expectedWait, totalWait, delta, "request rate: %d, bucket: %v", requestRate, bucket)
			})
		}
	}
}

func (s *LimitersTestSuite) TestLeakyBucketFakeClock() {
	capacity := int64(10)
	rate := time.Millisecond * 100

	clock := newFakeClock()
	for _, requestRate := range []time.Duration{rate * 2, rate, rate / 2, rate / 3, rate / 4, 0} {
		for name, bucket := range s.leakyBuckets(capacity, rate, 0, clock) {
			s.Run(name, func() {
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
			})
		}
	}
}

func (s *LimitersTestSuite) TestLeakyBucketOverflow() {
	rate := 100 * time.Millisecond
	capacity := int64(2)

	clock := newFakeClock()
	for name, bucket := range s.leakyBuckets(capacity, rate, 0, clock) {
		s.Run(name, func() {
			clock.reset()
			// The first call has no wait since there were no calls before.
			wait, err := bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), wait)
			// The second call increments the queue size by 1.
			wait, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(rate, wait)
			// The third call overflows the bucket capacity.
			wait, err = bucket.Limit(context.TODO())
			s.Require().Equal(l.ErrLimitExhausted, err)
			s.Equal(rate*2, wait)
			// Move the Clock 1 position forward.
			clock.Sleep(rate)
			// Retry the last call. This time it should succeed.
			wait, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(rate, wait)
		})
	}
}

func (s *LimitersTestSuite) TestLeakyBucketNoExpiration() {
	clock := l.NewSystemClock()
	buckets := s.leakyBuckets(1, time.Minute, 0, clock)

	// Take all capacity from all buckets
	for _, bucket := range buckets {
		_, _ = bucket.Limit(context.TODO())
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

func (s *LimitersTestSuite) TestLeakyBucketTTLExpiration() {
	clock := l.NewSystemClock()
	buckets := s.leakyBuckets(1, time.Minute, time.Second, clock)

	// Ignore in-memory bucket, as it has no expiration,
	// ignore DynamoDB, as amazon/dynamodb-local doesn't support TTLs.
	for k := range buckets {
		if strings.Contains(k, "BucketInMemory") || strings.Contains(k, "BucketDynamoDB") {
			delete(buckets, k)
		}
	}

	// Take all capacity from all buckets
	for _, bucket := range buckets {
		_, _ = bucket.Limit(context.TODO())
		_, _ = bucket.Limit(context.TODO())
	}

	// Wait for 3 seconds to check if the items have been deleted successfully
	clock.Sleep(3 * time.Second)

	// Expect all buckets to be empty (as the data expired)
	for name, bucket := range buckets {
		s.Run(name, func() {
			wait, err := bucket.Limit(context.TODO())
			s.Require().Equal(time.Duration(0), wait)
			s.Require().NoError(err)
		})
	}
}

func (s *LimitersTestSuite) TestLeakyBucketReset() {
	rate := 100 * time.Millisecond
	capacity := int64(2)

	clock := newFakeClock()
	for name, bucket := range s.leakyBuckets(capacity, rate, 0, clock) {
		s.Run(name, func() {
			clock.reset()
			// The first call has no wait since there were no calls before.
			wait, err := bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), wait)
			// The second call increments the queue size by 1.
			wait, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(rate, wait)
			// The third call overflows the bucket capacity.
			wait, err = bucket.Limit(context.TODO())
			s.Require().Equal(l.ErrLimitExhausted, err)
			s.Equal(rate*2, wait)
			// Reset the bucket
			err = bucket.Reset(context.TODO())
			s.Require().NoError(err)
			// Retry the last call. This time it should succeed.
			wait, err = bucket.Limit(context.TODO())
			s.Require().NoError(err)
			s.Equal(time.Duration(0), wait)
		})
	}
}

// TestLeakyBucketMemcachedExpiry verifies Memcached TTL expiry for LeakyBucketMemcached (no race check).
func (s *LimitersTestSuite) TestLeakyBucketMemcachedExpiry() {
	ttl := time.Second
	backend := l.NewLeakyBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, false)
	bucket := l.NewLeakyBucket(2, time.Second, l.NewLockNoop(), backend, l.NewSystemClock(), s.logger)
	ctx := context.Background()
	_, err := bucket.Limit(ctx)
	s.Require().NoError(err)
	_, err = bucket.Limit(ctx)
	s.Require().NoError(err)
	state, err := backend.State(ctx)
	s.Require().NoError(err)
	s.NotEqual(int64(0), state.Last, "Last should be set after token takes")
	time.Sleep(ttl + 1500*time.Millisecond)
	state, err = backend.State(ctx)
	s.Require().NoError(err)
	s.Equal(int64(0), state.Last, "State should be zero after expiry, got: %+v", state)
}

// TestLeakyBucketMemcachedExpiryWithRaceCheck verifies Memcached TTL expiry for LeakyBucketMemcached (race check enabled).
func (s *LimitersTestSuite) TestLeakyBucketMemcachedExpiryWithRaceCheck() {
	ttl := time.Second
	backend := l.NewLeakyBucketMemcached(s.memcacheClient, uuid.New().String(), ttl, true)
	bucket := l.NewLeakyBucket(2, time.Second, l.NewLockNoop(), backend, l.NewSystemClock(), s.logger)
	ctx := context.Background()
	_, err := bucket.Limit(ctx)
	s.Require().NoError(err)
	_, err = bucket.Limit(ctx)
	s.Require().NoError(err)
	state, err := backend.State(ctx)
	s.Require().NoError(err)
	s.NotEqual(int64(0), state.Last, "Last should be set after token takes")
	time.Sleep(ttl + 1500*time.Millisecond)

	state, err = backend.State(ctx)
	s.Require().NoError(err)
	s.Equal(int64(0), state.Last, "State should be zero after expiry, got: %+v", state)
}

func TestLeakyBucket_ZeroCapacity_ReturnsError(t *testing.T) {
	capacity := int64(0)
	rate := time.Hour
	logger := l.NewStdLogger()
	bucket := l.NewLeakyBucket(capacity, rate, l.NewLockNoop(), l.NewLeakyBucketInMemory(), newFakeClock(), logger)
	wait, err := bucket.Limit(context.TODO())
	require.Equal(t, l.ErrLimitExhausted, err)
	require.Equal(t, time.Duration(0), wait)
}

func BenchmarkLeakyBuckets(b *testing.B) {
	s := new(LimitersTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()

	capacity := int64(1)
	rate := 100 * time.Millisecond
	clock := newFakeClock()

	buckets := s.leakyBuckets(capacity, rate, 0, clock)
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

// setStateInOldFormat is a test utility method for writing state in the old format to Redis.
func setStateInOldFormat(ctx context.Context, cli *redis.Client, prefix string, state l.LeakyBucketState, ttl time.Duration) error {
	_, err := cli.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		err := pipeliner.Set(ctx, fmt.Sprintf("{%s}last", prefix), state.Last, ttl).Err()
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

// TestLeakyBucketRedisBackwardCompatibility tests that the new State method can read data written in the old format.
func (s *LimitersTestSuite) TestLeakyBucketRedisBackwardCompatibility() {
	// Create a new LeakyBucketRedis instance
	prefix := uuid.New().String()
	backend := l.NewLeakyBucketRedis(s.redisClient, prefix, time.Minute, false)

	// Write state using old format
	ctx := context.Background()
	expectedState := l.LeakyBucketState{
		Last: 12345,
	}

	// Write directly to Redis using old format
	err := setStateInOldFormat(ctx, s.redisClient, prefix, expectedState, time.Minute)
	s.Require().NoError(err, "Failed to set state using old format")

	// Read state using new format (State)
	actualState, err := backend.State(ctx)
	s.Require().NoError(err, "Failed to get state using new format")

	// Verify the state is correctly read
	s.Equal(expectedState.Last, actualState.Last, "Last values should match")
}
