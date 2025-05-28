package limiters

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

// FixedWindowIncrementer wraps the Increment method.
type FixedWindowIncrementer interface {
	// Increment increments the request counter for the window and returns the counter value.
	// TTL is the time duration before the next window.
	Increment(ctx context.Context, window time.Time, ttl time.Duration) (int64, error)
}

// FixedWindow implements a Fixed Window rate limiting algorithm.
//
// Simple and memory efficient algorithm that does not need a distributed lock.
// However it may be lenient when there are many requests around the boundary between 2 adjacent windows.
type FixedWindow struct {
	backend  FixedWindowIncrementer
	clock    Clock
	rate     time.Duration
	capacity int64
	mu       sync.Mutex
	window   time.Time
	overflow bool
}

// NewFixedWindow creates a new instance of FixedWindow.
// Capacity is the maximum amount of requests allowed per window.
// Rate is the window size.
func NewFixedWindow(capacity int64, rate time.Duration, fixedWindowIncrementer FixedWindowIncrementer, clock Clock) *FixedWindow {
	return &FixedWindow{backend: fixedWindowIncrementer, clock: clock, rate: rate, capacity: capacity}
}

// Limit returns the time duration to wait before the request can be processed.
// It returns ErrLimitExhausted if the request overflows the window's capacity.
func (f *FixedWindow) Limit(ctx context.Context) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	window := now.Truncate(f.rate)
	if f.window != window {
		f.window = window
		f.overflow = false
	}
	ttl := f.rate - now.Sub(window)
	if f.overflow {
		// If the window is already overflowed don't increment the counter.
		return ttl, ErrLimitExhausted
	}
	c, err := f.backend.Increment(ctx, window, ttl)
	if err != nil {
		return 0, err
	}
	if c > f.capacity {
		f.overflow = true

		return ttl, ErrLimitExhausted
	}

	return 0, nil
}

// FixedWindowInMemory is an in-memory implementation of FixedWindowIncrementer.
type FixedWindowInMemory struct {
	mu     sync.Mutex
	c      int64
	window time.Time
}

// NewFixedWindowInMemory creates a new instance of FixedWindowInMemory.
func NewFixedWindowInMemory() *FixedWindowInMemory {
	return &FixedWindowInMemory{}
}

// Increment increments the window's counter.
func (f *FixedWindowInMemory) Increment(ctx context.Context, window time.Time, _ time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if window != f.window {
		f.c = 0
		f.window = window
	}
	f.c++

	return f.c, ctx.Err()
}

// FixedWindowRedis implements FixedWindow in Redis.
type FixedWindowRedis struct {
	cli    redis.UniversalClient
	prefix string
}

// NewFixedWindowRedis returns a new instance of FixedWindowRedis.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
func NewFixedWindowRedis(cli redis.UniversalClient, prefix string) *FixedWindowRedis {
	return &FixedWindowRedis{cli: cli, prefix: prefix}
}

// Increment increments the window's counter in Redis.
func (f *FixedWindowRedis) Increment(ctx context.Context, window time.Time, ttl time.Duration) (int64, error) {
	var incr *redis.IntCmd
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = f.cli.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
			key := fmt.Sprintf("%d", window.UnixNano())
			incr = pipeliner.Incr(ctx, redisKey(f.prefix, key))
			pipeliner.PExpire(ctx, redisKey(f.prefix, key), ttl)

			return nil
		})
	}()

	select {
	case <-done:
		if err != nil {
			return 0, errors.Wrap(err, "redis transaction failed")
		}

		return incr.Val(), incr.Err()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// FixedWindowMemcached implements FixedWindow in Memcached.
type FixedWindowMemcached struct {
	cli    *memcache.Client
	prefix string
}

// NewFixedWindowMemcached returns a new instance of FixedWindowMemcached.
// Prefix is the key prefix used to store all the keys used in this implementation in Memcached.
func NewFixedWindowMemcached(cli *memcache.Client, prefix string) *FixedWindowMemcached {
	return &FixedWindowMemcached{cli: cli, prefix: prefix}
}

// Increment increments the window's counter in Memcached.
func (f *FixedWindowMemcached) Increment(ctx context.Context, window time.Time, ttl time.Duration) (int64, error) {
	var newValue uint64
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		key := fmt.Sprintf("%s:%d", f.prefix, window.UnixNano())
		newValue, err = f.cli.Increment(key, 1)
		if err != nil && errors.Is(err, memcache.ErrCacheMiss) {
			newValue = 1
			item := &memcache.Item{
				Key:   key,
				Value: []byte(strconv.FormatUint(newValue, 10)),
			}
			err = f.cli.Add(item)
		}
	}()

	select {
	case <-done:
		if err != nil {
			if errors.Is(err, memcache.ErrNotStored) {
				return f.Increment(ctx, window, ttl)
			}

			return 0, errors.Wrap(err, "failed to Increment or Add")
		}

		return int64(newValue), err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// FixedWindowDynamoDB implements FixedWindow in DynamoDB.
type FixedWindowDynamoDB struct {
	client       *dynamodb.Client
	partitionKey string
	tableProps   DynamoDBTableProperties
}

// NewFixedWindowDynamoDB creates a new instance of FixedWindowDynamoDB.
// PartitionKey is the key used to store all the this implementation in DynamoDB.
//
// TableProps describe the table that this backend should work with. This backend requires the following on the table:
// * SortKey
// * TTL.
func NewFixedWindowDynamoDB(client *dynamodb.Client, partitionKey string, props DynamoDBTableProperties) *FixedWindowDynamoDB {
	return &FixedWindowDynamoDB{
		client:       client,
		partitionKey: partitionKey,
		tableProps:   props,
	}
}

type contextKey int

var fixedWindowDynamoDBPartitionKey contextKey

// NewFixedWindowDynamoDBContext creates a context for FixedWindowDynamoDB with a partition key.
//
// This context can be used to control the partition key per-request.
//
// DEPRECATED: NewFixedWindowDynamoDBContext is deprecated and will be removed in future versions.
// Separate FixedWindow rate limiters should be used for different partition keys instead.
// Consider using the `Registry` to manage multiple FixedWindow instances with different partition keys.
func NewFixedWindowDynamoDBContext(ctx context.Context, partitionKey string) context.Context {
	log.Printf("DEPRECATED: NewFixedWindowDynamoDBContext is deprecated and will be removed in future versions.")

	return context.WithValue(ctx, fixedWindowDynamoDBPartitionKey, partitionKey)
}

const (
	fixedWindowDynamoDBUpdateExpression = "SET #C = if_not_exists(#C, :def) + :inc, #TTL = :ttl"
	dynamodbWindowCountKey              = "Count"
)

// Increment increments the window's counter in DynamoDB.
func (f *FixedWindowDynamoDB) Increment(ctx context.Context, window time.Time, ttl time.Duration) (int64, error) {
	var resp *dynamodb.UpdateItemOutput
	var err error

	done := make(chan struct{})
	go func() {
		defer close(done)
		partitionKey := f.partitionKey
		if key, ok := ctx.Value(fixedWindowDynamoDBPartitionKey).(string); ok {
			partitionKey = key
		}
		resp, err = f.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			Key: map[string]types.AttributeValue{
				f.tableProps.PartitionKeyName: &types.AttributeValueMemberS{Value: partitionKey},
				f.tableProps.SortKeyName:      &types.AttributeValueMemberS{Value: strconv.FormatInt(window.UnixNano(), 10)},
			},
			UpdateExpression: aws.String(fixedWindowDynamoDBUpdateExpression),
			ExpressionAttributeNames: map[string]string{
				"#TTL": f.tableProps.TTLFieldName,
				"#C":   dynamodbWindowCountKey,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)},
				":def": &types.AttributeValueMemberN{Value: "0"},
				":inc": &types.AttributeValueMemberN{Value: "1"},
			},
			TableName:    &f.tableProps.TableName,
			ReturnValues: types.ReturnValueAllNew,
		})
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if err != nil {
		return 0, errors.Wrap(err, "dynamodb update item failed")
	}

	var count float64
	err = attributevalue.Unmarshal(resp.Attributes[dynamodbWindowCountKey], &count)
	if err != nil {
		return 0, errors.Wrap(err, "unmarshal of dynamodb attribute value failed")
	}

	return int64(count), nil
}

// FixedWindowCosmosDB implements FixedWindow in CosmosDB.
type FixedWindowCosmosDB struct {
	client       *azcosmos.ContainerClient
	partitionKey string
}

// NewFixedWindowCosmosDB creates a new instance of FixedWindowCosmosDB.
// PartitionKey is the key used for partitioning data into multiple partitions.
func NewFixedWindowCosmosDB(client *azcosmos.ContainerClient, partitionKey string) *FixedWindowCosmosDB {
	return &FixedWindowCosmosDB{
		client:       client,
		partitionKey: partitionKey,
	}
}

func (f *FixedWindowCosmosDB) Increment(ctx context.Context, window time.Time, ttl time.Duration) (int64, error) {
	id := strconv.FormatInt(window.UnixNano(), 10)
	tmp := cosmosItem{
		ID:           id,
		PartitionKey: f.partitionKey,
		Count:        1,
		TTL:          int32(ttl),
	}

	ops := azcosmos.PatchOperations{}
	ops.AppendIncrement(`/Count`, 1)

	patchResp, err := f.client.PatchItem(ctx, azcosmos.NewPartitionKey().AppendString(f.partitionKey), id, ops, &azcosmos.ItemOptions{
		EnableContentResponseOnWrite: true,
	})
	if err == nil {
		// value exists and was updated
		err = json.Unmarshal(patchResp.Value, &tmp)
		if err != nil {
			return 0, errors.Wrap(err, "unmarshal of cosmos value failed")
		}

		return tmp.Count, nil
	}

	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) || respErr.StatusCode != http.StatusNotFound {
		return 0, errors.Wrap(err, `patch of cosmos value failed`)
	}

	newValue, err := json.Marshal(tmp)
	if err != nil {
		return 0, errors.Wrap(err, "marshal of cosmos value failed")
	}

	_, err = f.client.CreateItem(ctx, azcosmos.NewPartitionKey().AppendString(f.partitionKey), newValue, &azcosmos.ItemOptions{
		SessionToken: patchResp.SessionToken,
		IfMatchEtag:  &patchResp.ETag,
	})
	if err != nil {
		return 0, errors.Wrap(err, "upsert of cosmos value failed")
	}

	return tmp.Count, nil
}
