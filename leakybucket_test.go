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
	"github.com/stretchr/testify/require"
)

// leakyBuckets returns all the possible leakyBuckets combinations.
func (s *LimitersTestSuite) leakyBuckets(capacity int64, rate time.Duration, clock l.Clock) map[string]*l.LeakyBucket {
	buckets := make(map[string]*l.LeakyBucket)
	for lockerName, locker := range s.lockers(true) {
		for backendName, backend := range s.leakyBucketBackends() {
			buckets[lockerName+":"+backendName] = l.NewLeakyBucket(capacity, rate, locker, backend, clock, s.logger)
		}
	}
	return buckets
}

func (s *LimitersTestSuite) leakyBucketBackends() map[string]l.LeakyBucketStateBackend {
	return map[string]l.LeakyBucketStateBackend{
		"LeakyBucketInMemory":                  l.NewLeakyBucketInMemory(),
		"LeakyBucketEtcdNoRaceCheck":           l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), time.Second, false),
		"LeakyBucketEtcdWithRaceCheck":         l.NewLeakyBucketEtcd(s.etcdClient, uuid.New().String(), time.Second, true),
		"LeakyBucketRedisNoRaceCheck":          l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), time.Second, false),
		"LeakyBucketRedisWithRaceCheck":        l.NewLeakyBucketRedis(s.redisClient, uuid.New().String(), time.Second, true),
		"LeakyBucketRedisClusterNoRaceCheck":   l.NewLeakyBucketRedis(s.redisClusterClient, uuid.New().String(), time.Second, false),
		"LeakyBucketRedisClusterWithRaceCheck": l.NewLeakyBucketRedis(s.redisClusterClient, uuid.New().String(), time.Second, true),
		"LeakyBucketMemcachedNoRaceCheck":      l.NewLeakyBucketMemcached(s.memcacheClient, uuid.New().String(), time.Second, false),
		"LeakyBucketMemcachedWithRaceCheck":    l.NewLeakyBucketMemcached(s.memcacheClient, uuid.New().String(), time.Second, true),
		"LeakyBucketDynamoDBNoRaceCheck":       l.NewLeakyBucketDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps, time.Second, false),
		"LeakyBucketDynamoDBWithRaceCheck":     l.NewLeakyBucketDynamoDB(s.dynamodbClient, uuid.New().String(), s.dynamoDBTableProps, time.Second, true),
		"LeakyBucketCosmosDBNoRaceCheck":       l.NewLeakyBucketCosmosDB(s.cosmosContainerClient, uuid.New().String(), time.Second, false),
		"LeakyBucketCosmosDBWithRaceCheck":     l.NewLeakyBucketCosmosDB(s.cosmosContainerClient, uuid.New().String(), time.Second, true),
	}
}

func (s *LimitersTestSuite) TestLeakyBucketRealClock() {
	capacity := int64(10)
	rate := time.Millisecond * 10
	clock := l.NewSystemClock()
	for _, requestRate := range []time.Duration{rate / 2} {
		for name, bucket := range s.leakyBuckets(capacity, rate, clock) {
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
		for name, bucket := range s.leakyBuckets(capacity, rate, clock) {
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
	rate := time.Second
	capacity := int64(2)
	clock := newFakeClock()
	for name, bucket := range s.leakyBuckets(capacity, rate, clock) {
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

func (s *LimitersTestSuite) TestLeakyBucketReset() {
	rate := time.Second
	capacity := int64(2)
	clock := newFakeClock()
	for name, bucket := range s.leakyBuckets(capacity, rate, clock) {
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
	rate := time.Second
	clock := newFakeClock()
	buckets := s.leakyBuckets(capacity, rate, clock)
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

// setStateInOldFormat is a test utility method for writing state in the old format to Redis
func setStateInOldFormat(ctx context.Context, cli *redis.Client, prefix string, state l.LeakyBucketState, ttl time.Duration) error {
	_, err := cli.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		if err := pipeliner.Set(ctx, fmt.Sprintf("{%s}last", prefix), state.Last, ttl).Err(); err != nil {
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
	backend := l.NewLeakyBucketRedis(s.redisClient, prefix, time.Second, false)

	// Write state using old format
	ctx := context.Background()
	expectedState := l.LeakyBucketState{
		Last: 12345,
	}

	// Write directly to Redis using old format
	err := setStateInOldFormat(ctx, s.redisClient, prefix, expectedState, time.Second)
	s.Require().NoError(err, "Failed to set state using old format")

	// Read state using new format (State)
	actualState, err := backend.State(ctx)
	s.Require().NoError(err, "Failed to get state using new format")

	// Verify the state is correctly read
	s.Equal(expectedState.Last, actualState.Last, "Last values should match")
}
