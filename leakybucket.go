package limiters

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
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
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// LeakyBucketState represents the state of a LeakyBucket.
type LeakyBucketState struct {
	// Last is the Unix timestamp in nanoseconds of the most recent request.
	Last int64
}

// IzZero returns true if the bucket state is zero valued.
func (s LeakyBucketState) IzZero() bool {
	return s.Last == 0
}

// LeakyBucketStateBackend interface encapsulates the logic of retrieving and persisting the state of a LeakyBucket.
type LeakyBucketStateBackend interface {
	// State gets the current state of the LeakyBucket.
	State(ctx context.Context) (LeakyBucketState, error)
	// SetState sets (persists) the current state of the LeakyBucket.
	SetState(ctx context.Context, state LeakyBucketState) error
	// Reset resets (persists) the current state of the LeakyBucket.
	Reset(ctx context.Context) error
}

// LeakyBucket implements the https://en.wikipedia.org/wiki/Leaky_bucket#As_a_queue algorithm.
type LeakyBucket struct {
	locker  DistLocker
	backend LeakyBucketStateBackend
	clock   Clock
	logger  Logger
	// Capacity is the maximum allowed number of tokens in the bucket.
	capacity int64
	// Rate is the output rate: 1 request per the rate duration (in nanoseconds).
	rate int64
	mu   sync.Mutex
}

// NewLeakyBucket creates a new instance of LeakyBucket.
func NewLeakyBucket(capacity int64, rate time.Duration, locker DistLocker, leakyBucketStateBackend LeakyBucketStateBackend, clock Clock, logger Logger) *LeakyBucket {
	return &LeakyBucket{
		locker:   locker,
		backend:  leakyBucketStateBackend,
		clock:    clock,
		logger:   logger,
		capacity: capacity,
		rate:     int64(rate),
	}
}

// Limit returns the time duration to wait before the request can be processed.
// It returns ErrLimitExhausted if the request overflows the bucket's capacity. In this case the returned duration
// means how long it would have taken to wait for the request to be processed if the bucket was not overflowed.
func (t *LeakyBucket) Limit(ctx context.Context) (time.Duration, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.locker.Lock(ctx); err != nil {
		return 0, err
	}
	defer func() {
		if err := t.locker.Unlock(ctx); err != nil {
			t.logger.Log(err)
		}
	}()
	state, err := t.backend.State(ctx)
	if err != nil {
		return 0, err
	}
	now := t.clock.Now().UnixNano()
	if now < state.Last {
		// The queue has requests in it: move the current request to the last position + 1.
		state.Last += t.rate
	} else {
		// The queue is empty.
		// The offset is the duration to wait in case the last request happened less than rate duration ago.
		var offset int64
		delta := now - state.Last
		if delta < t.rate {
			offset = t.rate - delta
		}
		state.Last = now + offset
	}

	wait := state.Last - now
	if wait/t.rate >= t.capacity {
		return time.Duration(wait), ErrLimitExhausted
	}
	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, err
	}

	return time.Duration(wait), nil
}

// Reset resets the bucket.
func (t *LeakyBucket) Reset(ctx context.Context) error {
	return t.backend.Reset(ctx)
}

// LeakyBucketInMemory is an in-memory implementation of LeakyBucketStateBackend.
type LeakyBucketInMemory struct {
	state LeakyBucketState
}

// NewLeakyBucketInMemory creates a new instance of LeakyBucketInMemory.
func NewLeakyBucketInMemory() *LeakyBucketInMemory {
	return &LeakyBucketInMemory{}
}

// State gets the current state of the bucket.
func (l *LeakyBucketInMemory) State(ctx context.Context) (LeakyBucketState, error) {
	return l.state, ctx.Err()
}

// SetState sets the current state of the bucket.
func (l *LeakyBucketInMemory) SetState(ctx context.Context, state LeakyBucketState) error {
	l.state = state

	return ctx.Err()
}

// Reset resets the current state of the bucket.
func (l *LeakyBucketInMemory) Reset(ctx context.Context) error {
	state := LeakyBucketState{
		Last: 0,
	}

	return l.SetState(ctx, state)
}

const (
	etcdKeyLBLease = "lease"
	etcdKeyLBLast  = "last"
)

// LeakyBucketEtcd is an etcd implementation of a LeakyBucketStateBackend.
// See the TokenBucketEtcd description for the details on etcd usage.
type LeakyBucketEtcd struct {
	// prefix is the etcd key prefix.
	prefix      string
	cli         *clientv3.Client
	leaseID     clientv3.LeaseID
	ttl         time.Duration
	raceCheck   bool
	lastVersion int64
}

// NewLeakyBucketEtcd creates a new LeakyBucketEtcd instance.
// Prefix is used as an etcd key prefix for all keys stored in etcd by this algorithm.
// TTL is a TTL of the etcd lease used to store all the keys.
//
// If raceCheck is true and the keys in etcd are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewLeakyBucketEtcd(cli *clientv3.Client, prefix string, ttl time.Duration, raceCheck bool) *LeakyBucketEtcd {
	return &LeakyBucketEtcd{
		prefix:    prefix,
		cli:       cli,
		ttl:       ttl,
		raceCheck: raceCheck,
	}
}

// State gets the bucket's current state from etcd.
// If there is no state available in etcd then the initial bucket's state is returned.
func (l *LeakyBucketEtcd) State(ctx context.Context) (LeakyBucketState, error) {
	// Reset the lease ID as it will be updated by the successful Get operation below.
	l.leaseID = 0
	// Get all the keys under the prefix in a single request.
	r, err := l.cli.Get(ctx, l.prefix, clientv3.WithRange(incPrefix(l.prefix)))
	if err != nil {
		return LeakyBucketState{}, errors.Wrapf(err, "failed to get keys in range ['%s', '%s') from etcd", l.prefix, incPrefix(l.prefix))
	}
	if len(r.Kvs) == 0 {
		return LeakyBucketState{}, nil
	}
	state := LeakyBucketState{}
	parsed := 0
	var v int64
	for _, kv := range r.Kvs {
		switch string(kv.Key) {
		case etcdKey(l.prefix, etcdKeyLBLast):
			v, err = parseEtcdInt64(kv)
			if err != nil {
				return LeakyBucketState{}, err
			}
			state.Last = v
			parsed |= 1
			l.lastVersion = kv.Version

		case etcdKey(l.prefix, etcdKeyLBLease):
			v, err = parseEtcdInt64(kv)
			if err != nil {
				return LeakyBucketState{}, err
			}
			l.leaseID = clientv3.LeaseID(v)
			parsed |= 2
		}
	}
	if parsed != 3 {
		return LeakyBucketState{}, errors.New("failed to get state from etcd: some keys are missing")
	}

	return state, nil
}

// createLease creates a new lease in etcd and updates the t.leaseID value.
func (l *LeakyBucketEtcd) createLease(ctx context.Context) error {
	lease, err := l.cli.Grant(ctx, int64(l.ttl/time.Nanosecond))
	if err != nil {
		return errors.Wrap(err, "failed to create a new lease in etcd")
	}
	l.leaseID = lease.ID

	return nil
}

// save saves the state to etcd using the existing lease.
func (l *LeakyBucketEtcd) save(ctx context.Context, state LeakyBucketState) error {
	if !l.raceCheck {
		if _, err := l.cli.Txn(ctx).Then(
			clientv3.OpPut(etcdKey(l.prefix, etcdKeyLBLast), fmt.Sprintf("%d", state.Last), clientv3.WithLease(l.leaseID)),
			clientv3.OpPut(etcdKey(l.prefix, etcdKeyLBLease), fmt.Sprintf("%d", l.leaseID), clientv3.WithLease(l.leaseID)),
		).Commit(); err != nil {
			return errors.Wrap(err, "failed to commit a transaction to etcd")
		}

		return nil
	}
	// Put the keys only if they have not been modified since the most recent read.
	r, err := l.cli.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(etcdKey(l.prefix, etcdKeyLBLast)), ">", l.lastVersion),
	).Else(
		clientv3.OpPut(etcdKey(l.prefix, etcdKeyLBLast), fmt.Sprintf("%d", state.Last), clientv3.WithLease(l.leaseID)),
		clientv3.OpPut(etcdKey(l.prefix, etcdKeyLBLease), fmt.Sprintf("%d", l.leaseID), clientv3.WithLease(l.leaseID)),
	).Commit()
	if err != nil {
		return errors.Wrap(err, "failed to commit a transaction to etcd")
	}

	if !r.Succeeded {
		return nil
	}

	return ErrRaceCondition
}

// SetState updates the state of the bucket in etcd.
func (l *LeakyBucketEtcd) SetState(ctx context.Context, state LeakyBucketState) error {
	if l.leaseID == 0 {
		// Lease does not exist, create one.
		if err := l.createLease(ctx); err != nil {
			return err
		}
		// No need to send KeepAlive for the newly creates lease: save the state immediately.
		return l.save(ctx, state)
	}
	// Send the KeepAlive request to extend the existing lease.
	if _, err := l.cli.KeepAliveOnce(ctx, l.leaseID); errors.Is(err, rpctypes.ErrLeaseNotFound) {
		// Create a new lease since the current one has expired.
		if err = l.createLease(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return errors.Wrapf(err, "failed to extend the lease '%d'", l.leaseID)
	}

	return l.save(ctx, state)
}

// Reset resets the state of the bucket in etcd.
func (l *LeakyBucketEtcd) Reset(ctx context.Context) error {
	state := LeakyBucketState{
		Last: 0,
	}

	return l.SetState(ctx, state)
}

// Deprecated: These legacy keys will be removed in a future version.
// The state is now stored in a single JSON document under the "state" key.
const (
	redisKeyLBLast    = "last"
	redisKeyLBVersion = "version"
)

// LeakyBucketRedis is a Redis implementation of a LeakyBucketStateBackend.
type LeakyBucketRedis struct {
	cli         redis.UniversalClient
	prefix      string
	ttl         time.Duration
	raceCheck   bool
	lastVersion int64
}

// NewLeakyBucketRedis creates a new LeakyBucketRedis instance.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
// TTL is the TTL of the stored keys.
//
// If raceCheck is true and the keys in Redis are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewLeakyBucketRedis(cli redis.UniversalClient, prefix string, ttl time.Duration, raceCheck bool) *LeakyBucketRedis {
	return &LeakyBucketRedis{cli: cli, prefix: prefix, ttl: ttl, raceCheck: raceCheck}
}

// Deprecated: Legacy format support will be removed in a future version.
func (t *LeakyBucketRedis) oldState(ctx context.Context) (LeakyBucketState, error) {
	var values []interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		keys := []string{
			redisKey(t.prefix, redisKeyLBLast),
		}
		if t.raceCheck {
			keys = append(keys, redisKey(t.prefix, redisKeyLBVersion))
		}
		values, err = t.cli.MGet(ctx, keys...).Result()
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return LeakyBucketState{}, ctx.Err()
	}

	if err != nil {
		return LeakyBucketState{}, errors.Wrap(err, "failed to get keys from redis")
	}
	nilAny := false
	for _, v := range values {
		if v == nil {
			nilAny = true

			break
		}
	}
	if nilAny || errors.Is(err, redis.Nil) {
		// Keys don't exist, return an empty state.
		return LeakyBucketState{}, nil
	}

	last, err := strconv.ParseInt(values[0].(string), 10, 64)
	if err != nil {
		return LeakyBucketState{}, err
	}
	if t.raceCheck {
		t.lastVersion, err = strconv.ParseInt(values[1].(string), 10, 64)
		if err != nil {
			return LeakyBucketState{}, err
		}
	}

	return LeakyBucketState{
		Last: last,
	}, nil
}

// State gets the bucket's state from Redis.
func (t *LeakyBucketRedis) State(ctx context.Context) (LeakyBucketState, error) {
	var err error
	done := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	var state LeakyBucketState

	if t.raceCheck {
		// reset in a case of returning an empty LeakyBucketState
		t.lastVersion = 0
	}

	go func() {
		defer close(done)
		key := redisKey(t.prefix, "state")
		value, err := t.cli.Get(ctx, key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			errCh <- err

			return
		}

		if errors.Is(err, redis.Nil) {
			state, err = t.oldState(ctx)
			errCh <- err

			return
		}

		// Try new format
		var item struct {
			State   LeakyBucketState `json:"state"`
			Version int64            `json:"version"`
		}
		if err = json.Unmarshal([]byte(value), &item); err != nil {
			errCh <- err

			return
		}

		state = item.State
		if t.raceCheck {
			t.lastVersion = item.Version
		}
		errCh <- nil
	}()

	select {
	case <-done:
		err = <-errCh
	case <-ctx.Done():
		return LeakyBucketState{}, ctx.Err()
	}

	if err != nil {
		return LeakyBucketState{}, errors.Wrap(err, "failed to get state from redis")
	}

	return state, nil
}

// SetState updates the state in Redis.
func (t *LeakyBucketRedis) SetState(ctx context.Context, state LeakyBucketState) error {
	var err error
	done := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(done)
		key := redisKey(t.prefix, "state")
		item := struct {
			State   LeakyBucketState `json:"state"`
			Version int64            `json:"version"`
		}{
			State:   state,
			Version: t.lastVersion + 1,
		}

		value, err := json.Marshal(item)
		if err != nil {
			errCh <- err

			return
		}

		if !t.raceCheck {
			errCh <- t.cli.Set(ctx, key, value, t.ttl).Err()

			return
		}

		script := `
			local current = redis.call('get', KEYS[1])
			if current then
				local data = cjson.decode(current)
				if data.version > tonumber(ARGV[2]) then
					return 'RACE_CONDITION'
				end
			end
			redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[3])
			return 'OK'
		`
		result, err := t.cli.Eval(ctx, script, []string{key}, value, t.lastVersion, int64(t.ttl/time.Millisecond)).Result()
		if err != nil {
			errCh <- err

			return
		}
		if result == "RACE_CONDITION" {
			errCh <- ErrRaceCondition

			return
		}
		errCh <- nil
	}()

	select {
	case <-done:
		err = <-errCh
	case <-ctx.Done():
		return ctx.Err()
	}

	if err != nil {
		return errors.Wrap(err, "failed to save state to redis")
	}

	return nil
}

// Reset resets the state in Redis.
func (t *LeakyBucketRedis) Reset(ctx context.Context) error {
	state := LeakyBucketState{
		Last: 0,
	}

	return t.SetState(ctx, state)
}

// LeakyBucketMemcached is a Memcached implementation of a LeakyBucketStateBackend.
type LeakyBucketMemcached struct {
	cli       *memcache.Client
	key       string
	ttl       time.Duration
	raceCheck bool
	casId     uint64
}

// NewLeakyBucketMemcached creates a new LeakyBucketMemcached instance.
// Key is the key used to store all the keys used in this implementation in Memcached.
// TTL is the TTL of the stored keys.
//
// If raceCheck is true and the keys in Memcached are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewLeakyBucketMemcached(cli *memcache.Client, key string, ttl time.Duration, raceCheck bool) *LeakyBucketMemcached {
	return &LeakyBucketMemcached{cli: cli, key: key, ttl: ttl, raceCheck: raceCheck}
}

// State gets the bucket's state from Memcached.
func (t *LeakyBucketMemcached) State(ctx context.Context) (LeakyBucketState, error) {
	var item *memcache.Item
	var err error
	state := LeakyBucketState{}
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		item, err = t.cli.Get(t.key)
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return state, ctx.Err()
	}

	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			// Keys don't exist, return an empty state.
			return state, nil
		}

		return state, errors.Wrap(err, "failed to get keys from memcached")
	}
	b := bytes.NewBuffer(item.Value)
	err = gob.NewDecoder(b).Decode(&state)
	if err != nil {
		return state, errors.Wrap(err, "failed to Decode")
	}
	t.casId = item.CasID

	return state, nil
}

// SetState updates the state in Memcached.
// The provided fencing token is checked on the Memcached side before saving the keys.
func (t *LeakyBucketMemcached) SetState(ctx context.Context, state LeakyBucketState) error {
	var err error
	done := make(chan struct{}, 1)
	var b bytes.Buffer
	err = gob.NewEncoder(&b).Encode(state)
	if err != nil {
		return errors.Wrap(err, "failed to Encode")
	}
	go func() {
		defer close(done)
		item := &memcache.Item{
			Key:   t.key,
			Value: b.Bytes(),
			CasID: t.casId,
		}
		if t.raceCheck && t.casId > 0 {
			err = t.cli.CompareAndSwap(item)
		} else {
			err = t.cli.Set(item)
		}
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return ctx.Err()
	}

	if err != nil && (errors.Is(err, memcache.ErrCASConflict) || errors.Is(err, memcache.ErrNotStored) || errors.Is(err, memcache.ErrCacheMiss)) {
		return ErrRaceCondition
	}

	return errors.Wrap(err, "failed to save keys to memcached")
}

// Reset resets the state in Memcached.
func (t *LeakyBucketMemcached) Reset(ctx context.Context) error {
	state := LeakyBucketState{
		Last: 0,
	}
	t.casId = 0

	return t.SetState(ctx, state)
}

// LeakyBucketDynamoDB is a DyanamoDB implementation of a LeakyBucketStateBackend.
type LeakyBucketDynamoDB struct {
	client        *dynamodb.Client
	tableProps    DynamoDBTableProperties
	partitionKey  string
	ttl           time.Duration
	raceCheck     bool
	latestVersion int64
	keys          map[string]types.AttributeValue
}

// NewLeakyBucketDynamoDB creates a new LeakyBucketDynamoDB instance.
// PartitionKey is the key used to store all the this implementation in DynamoDB.
//
// TableProps describe the table that this backend should work with. This backend requires the following on the table:
// * TTL
//
// TTL is the TTL of the stored item.
//
// If raceCheck is true and the item in DynamoDB are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewLeakyBucketDynamoDB(client *dynamodb.Client, partitionKey string, tableProps DynamoDBTableProperties, ttl time.Duration, raceCheck bool) *LeakyBucketDynamoDB {
	keys := map[string]types.AttributeValue{
		tableProps.PartitionKeyName: &types.AttributeValueMemberS{Value: partitionKey},
	}

	if tableProps.SortKeyUsed {
		keys[tableProps.SortKeyName] = &types.AttributeValueMemberS{Value: partitionKey}
	}

	return &LeakyBucketDynamoDB{
		client:       client,
		partitionKey: partitionKey,
		tableProps:   tableProps,
		ttl:          ttl,
		raceCheck:    raceCheck,
		keys:         keys,
	}
}

// State gets the bucket's state from DynamoDB.
func (t *LeakyBucketDynamoDB) State(ctx context.Context) (LeakyBucketState, error) {
	resp, err := dynamoDBGetItem(ctx, t.client, t.getGetItemInput())
	if err != nil {
		return LeakyBucketState{}, err
	}

	return t.loadStateFromDynamoDB(resp)
}

// SetState updates the state in DynamoDB.
func (t *LeakyBucketDynamoDB) SetState(ctx context.Context, state LeakyBucketState) error {
	input := t.getPutItemInputFromState(state)

	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = dynamoDBputItem(ctx, t.client, input)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	return err
}

// Reset resets the state in DynamoDB.
func (t *LeakyBucketDynamoDB) Reset(ctx context.Context) error {
	state := LeakyBucketState{
		Last: 0,
	}

	return t.SetState(ctx, state)
}

const (
	dynamodbBucketRaceConditionExpression = "Version <= :version"
	dynamoDBBucketLastKey                 = "Last"
	dynamoDBBucketVersionKey              = "Version"
)

func (t *LeakyBucketDynamoDB) getPutItemInputFromState(state LeakyBucketState) *dynamodb.PutItemInput {
	item := map[string]types.AttributeValue{}
	for k, v := range t.keys {
		item[k] = v
	}

	item[dynamoDBBucketLastKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(state.Last, 10)}
	item[dynamoDBBucketVersionKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(t.latestVersion+1, 10)}
	item[t.tableProps.TTLFieldName] = &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(t.ttl).Unix(), 10)}

	input := &dynamodb.PutItemInput{
		TableName: &t.tableProps.TableName,
		Item:      item,
	}

	if t.raceCheck && t.latestVersion > 0 {
		input.ConditionExpression = aws.String(dynamodbBucketRaceConditionExpression)
		input.ExpressionAttributeValues = map[string]types.AttributeValue{
			":version": &types.AttributeValueMemberN{Value: strconv.FormatInt(t.latestVersion, 10)},
		}
	}

	return input
}

func (t *LeakyBucketDynamoDB) getGetItemInput() *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{
		TableName: &t.tableProps.TableName,
		Key:       t.keys,
	}
}

func (t *LeakyBucketDynamoDB) loadStateFromDynamoDB(resp *dynamodb.GetItemOutput) (LeakyBucketState, error) {
	state := LeakyBucketState{}
	err := attributevalue.Unmarshal(resp.Item[dynamoDBBucketLastKey], &state.Last)
	if err != nil {
		return state, fmt.Errorf("unmarshal dynamodb Last attribute failed: %w", err)
	}

	if t.raceCheck {
		err = attributevalue.Unmarshal(resp.Item[dynamoDBBucketVersionKey], &t.latestVersion)
		if err != nil {
			return state, fmt.Errorf("unmarshal dynamodb Version attribute failed: %w", err)
		}
	}

	return state, nil
}

// CosmosDBLeakyBucketItem represents a document in CosmosDB for LeakyBucket.
type CosmosDBLeakyBucketItem struct {
	ID           string           `json:"id"`
	PartitionKey string           `json:"partitionKey"`
	State        LeakyBucketState `json:"state"`
	Version      int64            `json:"version"`
	TTL          int64            `json:"ttl"`
}

// LeakyBucketCosmosDB is a CosmosDB implementation of a LeakyBucketStateBackend.
type LeakyBucketCosmosDB struct {
	client        *azcosmos.ContainerClient
	partitionKey  string
	id            string
	ttl           time.Duration
	raceCheck     bool
	latestVersion int64
}

// NewLeakyBucketCosmosDB creates a new LeakyBucketCosmosDB instance.
// PartitionKey is the key used to store all the implementation in CosmosDB.
// TTL is the TTL of the stored item.
//
// If raceCheck is true and the item in CosmosDB is modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewLeakyBucketCosmosDB(client *azcosmos.ContainerClient, partitionKey string, ttl time.Duration, raceCheck bool) *LeakyBucketCosmosDB {
	return &LeakyBucketCosmosDB{
		client:       client,
		partitionKey: partitionKey,
		id:           "leaky-bucket-" + partitionKey,
		ttl:          ttl,
		raceCheck:    raceCheck,
	}
}

func (t *LeakyBucketCosmosDB) State(ctx context.Context) (LeakyBucketState, error) {
	var item CosmosDBLeakyBucketItem
	resp, err := t.client.ReadItem(ctx, azcosmos.NewPartitionKey().AppendString(t.partitionKey), t.id, &azcosmos.ItemOptions{})
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return LeakyBucketState{}, nil
		}

		return LeakyBucketState{}, err
	}

	err = json.Unmarshal(resp.Value, &item)
	if err != nil {
		return LeakyBucketState{}, errors.Wrap(err, "failed to decode state from Cosmos DB")
	}

	if time.Now().Unix() > item.TTL {
		return LeakyBucketState{}, nil
	}

	if t.raceCheck {
		t.latestVersion = item.Version
	}

	return item.State, nil
}

func (t *LeakyBucketCosmosDB) SetState(ctx context.Context, state LeakyBucketState) error {
	var err error
	done := make(chan struct{}, 1)

	item := CosmosDBLeakyBucketItem{
		ID:           t.id,
		PartitionKey: t.partitionKey,
		State:        state,
		Version:      t.latestVersion + 1,
		TTL:          time.Now().Add(t.ttl).Unix(),
	}

	value, err := json.Marshal(item)
	if err != nil {
		return errors.Wrap(err, "failed to encode state to JSON")
	}

	go func() {
		defer close(done)
		_, err = t.client.UpsertItem(ctx, azcosmos.NewPartitionKey().AppendString(t.partitionKey), value, &azcosmos.ItemOptions{})
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusConflict && t.raceCheck {
			return ErrRaceCondition
		}

		return errors.Wrap(err, "failed to save keys to Cosmos DB")
	}

	return nil
}

func (t *LeakyBucketCosmosDB) Reset(ctx context.Context) error {
	state := LeakyBucketState{
		Last: 0,
	}

	return t.SetState(ctx, state)
}
