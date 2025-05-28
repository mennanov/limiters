package limiters

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// SlidingWindowIncrementer wraps the Increment method.
type SlidingWindowIncrementer interface {
	// Increment increments the request counter for the current window and returns the counter values for the previous
	// window and the current one.
	// TTL is the time duration before the next window.
	Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (prevCount, currCount int64, err error)
}

// SlidingWindow implements a Sliding Window rate limiting algorithm.
//
// It does not require a distributed lock and uses a minimum amount of memory, however it will disallow all the requests
// in case when a client is flooding the service with requests.
// It's the client's responsibility to handle the disallowed request and wait before making a new request again.
type SlidingWindow struct {
	backend  SlidingWindowIncrementer
	clock    Clock
	rate     time.Duration
	capacity int64
	epsilon  float64
}

// NewSlidingWindow creates a new instance of SlidingWindow.
// Capacity is the maximum amount of requests allowed per window.
// Rate is the window size.
// Epsilon is the max-allowed range of difference when comparing the current weighted number of requests with capacity.
func NewSlidingWindow(capacity int64, rate time.Duration, slidingWindowIncrementer SlidingWindowIncrementer, clock Clock, epsilon float64) *SlidingWindow {
	return &SlidingWindow{backend: slidingWindowIncrementer, clock: clock, rate: rate, capacity: capacity, epsilon: epsilon}
}

// Limit returns the time duration to wait before the request can be processed.
// It returns ErrLimitExhausted if the request overflows the capacity.
func (s *SlidingWindow) Limit(ctx context.Context) (time.Duration, error) {
	now := s.clock.Now()
	currWindow := now.Truncate(s.rate)
	prevWindow := currWindow.Add(-s.rate)
	ttl := s.rate - now.Sub(currWindow)
	prev, curr, err := s.backend.Increment(ctx, prevWindow, currWindow, ttl+s.rate)
	if err != nil {
		return 0, err
	}

	// "prev" and "curr" are capped at "s.capacity + s.epsilon" using math.Ceil to round up any fractional values,
	// ensuring that in the worst case, "total" can be slightly greater than "s.capacity".
	prev = int64(math.Min(float64(prev), math.Ceil(float64(s.capacity)+s.epsilon)))
	curr = int64(math.Min(float64(curr), math.Ceil(float64(s.capacity)+s.epsilon)))

	total := float64(prev*int64(ttl))/float64(s.rate) + float64(curr)
	if total-float64(s.capacity) >= s.epsilon {
		var wait time.Duration
		if curr <= s.capacity-1 && prev > 0 {
			wait = ttl - time.Duration(float64(s.capacity-1-curr)/float64(prev)*float64(s.rate))
		} else {
			// If prev == 0.
			wait = ttl + time.Duration((1-float64(s.capacity-1)/float64(curr))*float64(s.rate))
		}

		return wait, ErrLimitExhausted
	}

	return 0, nil
}

// SlidingWindowInMemory is an in-memory implementation of SlidingWindowIncrementer.
type SlidingWindowInMemory struct {
	mu           sync.Mutex
	prevC, currC int64
	prevW, currW time.Time
}

// NewSlidingWindowInMemory creates a new instance of SlidingWindowInMemory.
func NewSlidingWindowInMemory() *SlidingWindowInMemory {
	return &SlidingWindowInMemory{}
}

// Increment increments the current window's counter and returns the number of requests in the previous window and the
// current one.
func (s *SlidingWindowInMemory) Increment(ctx context.Context, prev, curr time.Time, _ time.Duration) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if curr != s.currW {
		if prev.Equal(s.currW) {
			s.prevW = s.currW
			s.prevC = s.currC
		} else {
			s.prevW = time.Time{}
			s.prevC = 0
		}
		s.currW = curr
		s.currC = 0
	}
	s.currC++

	return s.prevC, s.currC, ctx.Err()
}

// SlidingWindowRedis implements SlidingWindow in Redis.
type SlidingWindowRedis struct {
	cli    redis.UniversalClient
	prefix string
}

// NewSlidingWindowRedis creates a new instance of SlidingWindowRedis.
func NewSlidingWindowRedis(cli redis.UniversalClient, prefix string) *SlidingWindowRedis {
	return &SlidingWindowRedis{cli: cli, prefix: prefix}
}

// Increment increments the current window's counter in Redis and returns the number of requests in the previous window
// and the current one.
func (s *SlidingWindowRedis) Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (int64, int64, error) {
	var incr *redis.IntCmd
	var prevCountCmd *redis.StringCmd
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = s.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
			currKey := fmt.Sprintf("%d", curr.UnixNano())
			incr = pipeliner.Incr(ctx, redisKey(s.prefix, currKey))
			pipeliner.PExpire(ctx, redisKey(s.prefix, currKey), ttl)
			prevCountCmd = pipeliner.Get(ctx, redisKey(s.prefix, fmt.Sprintf("%d", prev.UnixNano())))

			return nil
		})
	}()

	var prevCount int64
	select {
	case <-done:
		if errors.Is(err, redis.TxFailedErr) {
			return 0, 0, errors.Wrap(err, "redis transaction failed")
		} else if errors.Is(err, redis.Nil) {
			prevCount = 0
		} else if err != nil {
			return 0, 0, errors.Wrap(err, "unexpected error from redis")
		} else {
			prevCount, err = strconv.ParseInt(prevCountCmd.Val(), 10, 64)
			if err != nil {
				return 0, 0, errors.Wrap(err, "failed to parse response from redis")
			}
		}

		return prevCount, incr.Val(), nil
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// SlidingWindowMemcached implements SlidingWindow in Memcached.
type SlidingWindowMemcached struct {
	cli    *memcache.Client
	prefix string
}

// NewSlidingWindowMemcached creates a new instance of SlidingWindowMemcached.
func NewSlidingWindowMemcached(cli *memcache.Client, prefix string) *SlidingWindowMemcached {
	return &SlidingWindowMemcached{cli: cli, prefix: prefix}
}

// Increment increments the current window's counter in Memcached and returns the number of requests in the previous window
// and the current one.
func (s *SlidingWindowMemcached) Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (int64, int64, error) {
	var prevCount uint64
	var currCount uint64
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)

		var item *memcache.Item
		prevKey := fmt.Sprintf("%s:%d", s.prefix, prev.UnixNano())
		item, err = s.cli.Get(prevKey)
		if err != nil {
			if errors.Is(err, memcache.ErrCacheMiss) {
				err = nil
				prevCount = 0
			} else {
				return
			}
		} else {
			prevCount, err = strconv.ParseUint(string(item.Value), 10, 64)
			if err != nil {
				return
			}
		}

		currKey := fmt.Sprintf("%s:%d", s.prefix, curr.UnixNano())
		currCount, err = s.cli.Increment(currKey, 1)
		if err != nil && errors.Is(err, memcache.ErrCacheMiss) {
			currCount = 1
			item = &memcache.Item{
				Key:   currKey,
				Value: []byte(strconv.FormatUint(currCount, 10)),
			}
			err = s.cli.Add(item)
		}
	}()

	select {
	case <-done:
		if err != nil {
			if errors.Is(err, memcache.ErrNotStored) {
				return s.Increment(ctx, prev, curr, ttl)
			}

			return 0, 0, err
		}

		return int64(prevCount), int64(currCount), nil
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// SlidingWindowDynamoDB implements SlidingWindow in DynamoDB.
type SlidingWindowDynamoDB struct {
	client       *dynamodb.Client
	partitionKey string
	tableProps   DynamoDBTableProperties
}

// NewSlidingWindowDynamoDB creates a new instance of SlidingWindowDynamoDB.
// PartitionKey is the key used to store all the this implementation in DynamoDB.
//
// TableProps describe the table that this backend should work with. This backend requires the following on the table:
// * SortKey
// * TTL.
func NewSlidingWindowDynamoDB(client *dynamodb.Client, partitionKey string, props DynamoDBTableProperties) *SlidingWindowDynamoDB {
	return &SlidingWindowDynamoDB{
		client:       client,
		partitionKey: partitionKey,
		tableProps:   props,
	}
}

// Increment increments the current window's counter in DynamoDB and returns the number of requests in the previous window
// and the current one.
func (s *SlidingWindowDynamoDB) Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (int64, int64, error) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	var currentCount int64
	var currentErr error
	go func() {
		defer wg.Done()
		resp, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			Key: map[string]types.AttributeValue{
				s.tableProps.PartitionKeyName: &types.AttributeValueMemberS{Value: s.partitionKey},
				s.tableProps.SortKeyName:      &types.AttributeValueMemberS{Value: strconv.FormatInt(curr.UnixNano(), 10)},
			},
			UpdateExpression: aws.String(fixedWindowDynamoDBUpdateExpression),
			ExpressionAttributeNames: map[string]string{
				"#TTL": s.tableProps.TTLFieldName,
				"#C":   dynamodbWindowCountKey,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)},
				":def": &types.AttributeValueMemberN{Value: "0"},
				":inc": &types.AttributeValueMemberN{Value: "1"},
			},
			TableName:    &s.tableProps.TableName,
			ReturnValues: types.ReturnValueAllNew,
		})
		if err != nil {
			currentErr = errors.Wrap(err, "dynamodb get item failed")

			return
		}

		var tmp float64
		err = attributevalue.Unmarshal(resp.Attributes[dynamodbWindowCountKey], &tmp)
		if err != nil {
			currentErr = errors.Wrap(err, "unmarshal of dynamodb attribute value failed")

			return
		}

		currentCount = int64(tmp)
	}()

	var priorCount int64
	var priorErr error
	go func() {
		defer wg.Done()
		resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.tableProps.TableName),
			Key: map[string]types.AttributeValue{
				s.tableProps.PartitionKeyName: &types.AttributeValueMemberS{Value: s.partitionKey},
				s.tableProps.SortKeyName:      &types.AttributeValueMemberS{Value: strconv.FormatInt(prev.UnixNano(), 10)},
			},
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			priorCount, priorErr = 0, errors.Wrap(err, "dynamodb get item failed")

			return
		}

		if len(resp.Item) == 0 {
			priorCount = 0

			return
		}

		var count float64
		err = attributevalue.Unmarshal(resp.Item[dynamodbWindowCountKey], &count)
		if err != nil {
			priorCount, priorErr = 0, errors.Wrap(err, "unmarshal of dynamodb attribute value failed")

			return
		}

		priorCount = int64(count)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}

	if currentErr != nil {
		return 0, 0, errors.Wrap(currentErr, "failed to update current count")
	} else if priorErr != nil {
		return 0, 0, errors.Wrap(priorErr, "failed to get previous count")
	}

	return priorCount, currentCount, nil
}

// SlidingWindowCosmosDB implements SlidingWindow in Azure Cosmos DB.
type SlidingWindowCosmosDB struct {
	client       *azcosmos.ContainerClient
	partitionKey string
}

// NewSlidingWindowCosmosDB creates a new instance of SlidingWindowCosmosDB.
// PartitionKey is the key used to store all the this implementation in Cosmos.
func NewSlidingWindowCosmosDB(client *azcosmos.ContainerClient, partitionKey string) *SlidingWindowCosmosDB {
	return &SlidingWindowCosmosDB{
		client:       client,
		partitionKey: partitionKey,
	}
}

// Increment increments the current window's counter in Cosmos and returns the number of requests in the previous window
// and the current one.
func (s *SlidingWindowCosmosDB) Increment(ctx context.Context, prev, curr time.Time, ttl time.Duration) (int64, int64, error) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	var currentCount int64
	var currentErr error
	go func() {
		defer wg.Done()

		id := strconv.FormatInt(curr.UnixNano(), 10)
		tmp := cosmosItem{
			ID:           id,
			PartitionKey: s.partitionKey,
			Count:        1,
			TTL:          int32(ttl),
		}

		ops := azcosmos.PatchOperations{}
		ops.AppendIncrement(`/Count`, 1)

		patchResp, err := s.client.PatchItem(ctx, azcosmos.NewPartitionKey().AppendString(s.partitionKey), id, ops, &azcosmos.ItemOptions{
			EnableContentResponseOnWrite: true,
		})
		if err == nil {
			// value exists and was updated
			err = json.Unmarshal(patchResp.Value, &tmp)
			if err != nil {
				currentErr = errors.Wrap(err, "unmarshal of cosmos value current failed")

				return
			}
			currentCount = tmp.Count

			return
		}

		var respErr *azcore.ResponseError
		if !errors.As(err, &respErr) || respErr.StatusCode != http.StatusNotFound {
			currentErr = errors.Wrap(err, `patch of cosmos value current failed`)

			return
		}

		newValue, err := json.Marshal(tmp)
		if err != nil {
			currentErr = errors.Wrap(err, "marshal of cosmos value current failed")

			return
		}

		_, err = s.client.CreateItem(ctx, azcosmos.NewPartitionKey().AppendString(s.partitionKey), newValue, &azcosmos.ItemOptions{
			SessionToken: patchResp.SessionToken,
			IfMatchEtag:  &patchResp.ETag,
		})
		if err != nil {
			currentErr = errors.Wrap(err, "upsert of cosmos value current failed")

			return
		}

		currentCount = tmp.Count
	}()

	var priorCount int64
	var priorErr error
	go func() {
		defer wg.Done()

		id := strconv.FormatInt(prev.UnixNano(), 10)
		resp, err := s.client.ReadItem(ctx, azcosmos.NewPartitionKey().AppendString(s.partitionKey), id, &azcosmos.ItemOptions{})
		if err != nil {
			var azerr *azcore.ResponseError
			if errors.As(err, &azerr) && azerr.StatusCode == http.StatusNotFound {
				priorCount, priorErr = 0, nil

				return
			}
			priorErr = errors.Wrap(err, "cosmos get item prior failed")

			return
		}

		var tmp cosmosItem
		err = json.Unmarshal(resp.Value, &tmp)
		if err != nil {
			priorErr = errors.Wrap(err, "unmarshal of cosmos value prior failed")

			return
		}

		priorCount = tmp.Count
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}

	if currentErr != nil {
		return 0, 0, errors.Wrap(currentErr, "failed to update current count")
	} else if priorErr != nil {
		return 0, 0, errors.Wrap(priorErr, "failed to get previous count")
	}

	return priorCount, currentCount, nil
}
