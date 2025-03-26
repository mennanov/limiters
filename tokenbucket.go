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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TokenBucketState represents a state of a token bucket.
type TokenBucketState struct {
	// Last is the last time the state was updated (Unix timestamp in nanoseconds).
	Last int64
	// Available is the number of available tokens in the bucket.
	Available int64
}

// isZero returns true if the bucket state is zero valued.
func (s TokenBucketState) isZero() bool {
	return s.Last == 0 && s.Available == 0
}

// TokenBucketStateBackend interface encapsulates the logic of retrieving and persisting the state of a TokenBucket.
type TokenBucketStateBackend interface {
	// State gets the current state of the TokenBucket.
	State(ctx context.Context) (TokenBucketState, error)
	// SetState sets (persists) the current state of the TokenBucket.
	SetState(ctx context.Context, state TokenBucketState) error
	// Reset resets (persists) the current state of the TokenBucket.
	Reset(ctx context.Context) error
}

// TokenBucket implements the https://en.wikipedia.org/wiki/Token_bucket algorithm.
type TokenBucket struct {
	locker  DistLocker
	backend TokenBucketStateBackend
	clock   Clock
	logger  Logger
	// refillRate is the tokens refill rate (1 token per duration).
	refillRate time.Duration
	// capacity is the bucket's capacity.
	capacity int64
	mu       sync.Mutex
}

// NewTokenBucket creates a new instance of TokenBucket.
func NewTokenBucket(capacity int64, refillRate time.Duration, locker DistLocker, tokenBucketStateBackend TokenBucketStateBackend, clock Clock, logger Logger) *TokenBucket {
	return &TokenBucket{
		locker:     locker,
		backend:    tokenBucketStateBackend,
		clock:      clock,
		logger:     logger,
		refillRate: refillRate,
		capacity:   capacity,
	}
}

// Take takes tokens from the bucket.
//
// It returns a zero duration and a nil error if the bucket has sufficient amount of tokens.
//
// It returns ErrLimitExhausted if the amount of available tokens is less than requested. In this case the returned
// duration is the amount of time to wait to retry the request.
func (t *TokenBucket) Take(ctx context.Context, tokens int64) (time.Duration, error) {
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
	if state.isZero() {
		// Initially the bucket is full.
		state.Available = t.capacity
	}
	now := t.clock.Now().UnixNano()
	// Refill the bucket.
	tokensToAdd := (now - state.Last) / int64(t.refillRate)
	partialTime := (now - state.Last) % int64(t.refillRate)
	if tokensToAdd > 0 {
		if tokensToAdd+state.Available < t.capacity {
			state.Available += tokensToAdd
			state.Last = now - partialTime
		} else {
			state.Available = t.capacity
			state.Last = now
		}
	}

	if tokens > state.Available {
		return t.refillRate * time.Duration(tokens-state.Available), ErrLimitExhausted
	}
	// Take the tokens from the bucket.
	state.Available -= tokens
	if err = t.backend.SetState(ctx, state); err != nil {
		return 0, err
	}

	return 0, nil
}

// Limit takes 1 token from the bucket.
func (t *TokenBucket) Limit(ctx context.Context) (time.Duration, error) {
	return t.Take(ctx, 1)
}

// Reset resets the bucket.
func (t *TokenBucket) Reset(ctx context.Context) error {
	return t.backend.Reset(ctx)
}

// TokenBucketInMemory is an in-memory implementation of TokenBucketStateBackend.
//
// The state is not shared nor persisted so it won't survive restarts or failures.
// Due to the local nature of the state the rate at which some endpoints are accessed can't be reliably predicted or
// limited.
//
// Although it can be used as a global rate limiter with a round-robin load-balancer.
type TokenBucketInMemory struct {
	state TokenBucketState
}

// NewTokenBucketInMemory creates a new instance of TokenBucketInMemory.
func NewTokenBucketInMemory() *TokenBucketInMemory {
	return &TokenBucketInMemory{}
}

// State returns the current bucket's state.
func (t *TokenBucketInMemory) State(ctx context.Context) (TokenBucketState, error) {
	return t.state, ctx.Err()
}

// SetState sets the current bucket's state.
func (t *TokenBucketInMemory) SetState(ctx context.Context, state TokenBucketState) error {
	t.state = state

	return ctx.Err()
}

// Reset resets the current bucket's state.
func (t *TokenBucketInMemory) Reset(ctx context.Context) error {
	state := TokenBucketState{
		Last:      0,
		Available: 0,
	}

	return t.SetState(ctx, state)
}

const (
	etcdKeyTBLease     = "lease"
	etcdKeyTBAvailable = "available"
	etcdKeyTBLast      = "last"
)

// TokenBucketEtcd is an etcd implementation of a TokenBucketStateBackend.
//
// See https://github.com/etcd-io/etcd/blob/master/Documentation/learning/data_model.md
//
// etcd is designed to reliably store infrequently updated data, thus it should only be used for the API endpoints which
// are accessed less frequently than it can be processed by the rate limiter.
//
// Aggressive compaction and defragmentation has to be enabled in etcd to prevent the size of the storage
// to grow indefinitely: every change of the state of the bucket (every access) will create a new revision in etcd.
//
// It probably makes it impractical for the high load cases, but can be used to reliably and precisely rate limit an
// access to the business critical endpoints where each access must be reliably logged.
type TokenBucketEtcd struct {
	// prefix is the etcd key prefix.
	prefix      string
	cli         *clientv3.Client
	leaseID     clientv3.LeaseID
	ttl         time.Duration
	raceCheck   bool
	lastVersion int64
}

// NewTokenBucketEtcd creates a new TokenBucketEtcd instance.
// Prefix is used as an etcd key prefix for all keys stored in etcd by this algorithm.
// TTL is a TTL of the etcd lease in seconds used to store all the keys: all the keys are automatically deleted after
// the TTL expires.
//
// If raceCheck is true and the keys in etcd are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
// It does not add any significant overhead as it can be trivially checked on etcd side before updating the keys.
func NewTokenBucketEtcd(cli *clientv3.Client, prefix string, ttl time.Duration, raceCheck bool) *TokenBucketEtcd {
	return &TokenBucketEtcd{
		prefix:    prefix,
		cli:       cli,
		ttl:       ttl,
		raceCheck: raceCheck,
	}
}

// etcdKey returns a full etcd key from the provided key and prefix.
func etcdKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

// parseEtcdInt64 parses the etcd value into int64.
func parseEtcdInt64(kv *mvccpb.KeyValue) (int64, error) {
	v, err := strconv.ParseInt(string(kv.Value), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse key '%s' as int64", string(kv.Key))
	}

	return v, nil
}

func incPrefix(p string) string {
	b := []byte(p)
	b[len(b)-1]++

	return string(b)
}

// State gets the bucket's current state from etcd.
// If there is no state available in etcd then the initial bucket's state is returned.
func (t *TokenBucketEtcd) State(ctx context.Context) (TokenBucketState, error) {
	// Get all the keys under the prefix in a single request.
	r, err := t.cli.Get(ctx, t.prefix, clientv3.WithRange(incPrefix(t.prefix)))
	if err != nil {
		return TokenBucketState{}, errors.Wrapf(err, "failed to get keys in range ['%s', '%s') from etcd", t.prefix, incPrefix(t.prefix))
	}
	if len(r.Kvs) == 0 {
		// State not found, return zero valued state.
		return TokenBucketState{}, nil
	}
	state := TokenBucketState{}
	parsed := 0
	var v int64
	for _, kv := range r.Kvs {
		switch string(kv.Key) {
		case etcdKey(t.prefix, etcdKeyTBAvailable):
			v, err = parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Available = v
			parsed |= 1

		case etcdKey(t.prefix, etcdKeyTBLast):
			v, err = parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Last = v
			parsed |= 2
			t.lastVersion = kv.Version

		case etcdKey(t.prefix, etcdKeyTBLease):
			v, err = parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			t.leaseID = clientv3.LeaseID(v)
			parsed |= 4
		}
	}
	if parsed != 7 {
		return TokenBucketState{}, errors.New("failed to get state from etcd: some keys are missing")
	}

	return state, nil
}

// createLease creates a new lease in etcd and updates the t.leaseID value.
func (t *TokenBucketEtcd) createLease(ctx context.Context) error {
	lease, err := t.cli.Grant(ctx, int64(t.ttl/time.Nanosecond))
	if err != nil {
		return errors.Wrap(err, "failed to create a new lease in etcd")
	}
	t.leaseID = lease.ID

	return nil
}

// save saves the state to etcd using the existing lease and the fencing token.
func (t *TokenBucketEtcd) save(ctx context.Context, state TokenBucketState) error {
	if !t.raceCheck {
		if _, err := t.cli.Txn(ctx).Then(
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBAvailable), fmt.Sprintf("%d", state.Available), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLast), fmt.Sprintf("%d", state.Last), clientv3.WithLease(t.leaseID)),
			clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLease), fmt.Sprintf("%d", t.leaseID), clientv3.WithLease(t.leaseID)),
		).Commit(); err != nil {
			return errors.Wrap(err, "failed to commit a transaction to etcd")
		}

		return nil
	}
	// Put the keys only if they have not been modified since the most recent read.
	r, err := t.cli.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(etcdKey(t.prefix, etcdKeyTBLast)), ">", t.lastVersion),
	).Else(
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBAvailable), fmt.Sprintf("%d", state.Available), clientv3.WithLease(t.leaseID)),
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLast), fmt.Sprintf("%d", state.Last), clientv3.WithLease(t.leaseID)),
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLease), fmt.Sprintf("%d", t.leaseID), clientv3.WithLease(t.leaseID)),
	).Commit()
	if err != nil {
		return errors.Wrap(err, "failed to commit a transaction to etcd")
	}

	if !r.Succeeded {
		return nil
	}

	return ErrRaceCondition
}

// SetState updates the state of the bucket.
func (t *TokenBucketEtcd) SetState(ctx context.Context, state TokenBucketState) error {
	if t.leaseID == 0 {
		// Lease does not exist, create one.
		if err := t.createLease(ctx); err != nil {
			return err
		}
		// No need to send KeepAlive for the newly created lease: save the state immediately.
		return t.save(ctx, state)
	}
	// Send the KeepAlive request to extend the existing lease.
	if _, err := t.cli.KeepAliveOnce(ctx, t.leaseID); errors.Is(err, rpctypes.ErrLeaseNotFound) {
		// Create a new lease since the current one has expired.
		if err = t.createLease(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return errors.Wrapf(err, "failed to extend the lease '%d'", t.leaseID)
	}

	return t.save(ctx, state)
}

// Reset resets the state of the bucket.
func (t *TokenBucketEtcd) Reset(ctx context.Context) error {
	state := TokenBucketState{
		Last:      0,
		Available: 0,
	}

	return t.SetState(ctx, state)
}

// Deprecated: These legacy keys will be removed in a future version.
// The state is now stored in a single JSON document under the "state" key.
const (
	redisKeyTBAvailable = "available"
	redisKeyTBLast      = "last"
	redisKeyTBVersion   = "version"
)

// If we do use cluster client and if the cluster is large enough, it is possible that when accessing multiple keys
// in leaky bucket or token bucket, these keys might go different slots and it will fail with error message
// `CROSSSLOT Keys in request don't hash to the same slot`. Adding hash tags in redisKey will force them into the
// same slot for keys with the same prefix.
//
// https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags
func redisKey(prefix, key string) string {
	return fmt.Sprintf("{%s}%s", prefix, key)
}

// TokenBucketRedis is a Redis implementation of a TokenBucketStateBackend.
//
// Redis is an in-memory key-value data storage which also supports persistence.
// It is a better choice for high load cases than etcd as it does not keep old values of the keys thus does not need
// the compaction/defragmentation.
//
// Although depending on a persistence and a cluster configuration some data might be lost in case of a failure
// resulting in an under-limiting the accesses to the service.
type TokenBucketRedis struct {
	cli         redis.UniversalClient
	prefix      string
	ttl         time.Duration
	raceCheck   bool
	lastVersion int64
}

// NewTokenBucketRedis creates a new TokenBucketRedis instance.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
// TTL is the TTL of the stored keys.
//
// If raceCheck is true and the keys in Redis are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
// This adds an extra overhead since a Lua script has to be executed on the Redis side which locks the entire database.
func NewTokenBucketRedis(cli redis.UniversalClient, prefix string, ttl time.Duration, raceCheck bool) *TokenBucketRedis {
	return &TokenBucketRedis{cli: cli, prefix: prefix, ttl: ttl, raceCheck: raceCheck}
}

// Deprecated: Legacy format support will be removed in a future version.
func (t *TokenBucketRedis) oldState(ctx context.Context) (TokenBucketState, error) {
	var values []interface{}
	var err error
	done := make(chan struct{}, 1)

	if t.raceCheck {
		// reset in a case of returning an empty TokenBucketState
		t.lastVersion = 0
	}

	go func() {
		defer close(done)
		keys := []string{
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
		}
		if t.raceCheck {
			keys = append(keys, redisKey(t.prefix, redisKeyTBVersion))
		}
		values, err = t.cli.MGet(ctx, keys...).Result()
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return TokenBucketState{}, ctx.Err()
	}

	if err != nil {
		return TokenBucketState{}, errors.Wrap(err, "failed to get keys from redis")
	}
	nilAny := false
	for _, v := range values {
		if v == nil {
			nilAny = true

			break
		}
	}
	if nilAny || errors.Is(err, redis.Nil) {
		// Keys don't exist, return the initial state.
		return TokenBucketState{}, nil
	}

	last, err := strconv.ParseInt(values[0].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	available, err := strconv.ParseInt(values[1].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	if t.raceCheck {
		t.lastVersion, err = strconv.ParseInt(values[2].(string), 10, 64)
		if err != nil {
			return TokenBucketState{}, err
		}
	}

	return TokenBucketState{
		Last:      last,
		Available: available,
	}, nil
}

// State gets the bucket's state from Redis.
func (t *TokenBucketRedis) State(ctx context.Context) (TokenBucketState, error) {
	var err error
	done := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	var state TokenBucketState

	if t.raceCheck {
		// reset in a case of returning an empty TokenBucketState
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
			State   TokenBucketState `json:"state"`
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
		return TokenBucketState{}, ctx.Err()
	}

	if err != nil {
		return TokenBucketState{}, errors.Wrap(err, "failed to get state from redis")
	}

	return state, nil
}

// SetState updates the state in Redis.
func (t *TokenBucketRedis) SetState(ctx context.Context, state TokenBucketState) error {
	var err error
	done := make(chan struct{}, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(done)
		key := redisKey(t.prefix, "state")
		item := struct {
			State   TokenBucketState `json:"state"`
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
func (t *TokenBucketRedis) Reset(ctx context.Context) error {
	state := TokenBucketState{
		Last:      0,
		Available: 0,
	}

	return t.SetState(ctx, state)
}

// TokenBucketMemcached is a Memcached implementation of a TokenBucketStateBackend.
//
// Memcached is a distributed memory object caching system.
type TokenBucketMemcached struct {
	cli       *memcache.Client
	key       string
	ttl       time.Duration
	raceCheck bool
	casId     uint64
}

// NewTokenBucketMemcached creates a new TokenBucketMemcached instance.
// Key is the key used to store all the keys used in this implementation in Memcached.
// TTL is the TTL of the stored keys.
//
// If raceCheck is true and the keys in Memcached are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
// This adds an extra overhead since a Lua script has to be executed on the Memcached side which locks the entire database.
func NewTokenBucketMemcached(cli *memcache.Client, key string, ttl time.Duration, raceCheck bool) *TokenBucketMemcached {
	return &TokenBucketMemcached{cli: cli, key: key, ttl: ttl, raceCheck: raceCheck}
}

// State gets the bucket's state from Memcached.
func (t *TokenBucketMemcached) State(ctx context.Context) (TokenBucketState, error) {
	var item *memcache.Item
	var state TokenBucketState
	var err error

	done := make(chan struct{}, 1)
	t.casId = 0

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
			// Keys don't exist, return the initial state.
			return state, nil
		}

		return state, errors.Wrap(err, "failed to get key from memcached")
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
func (t *TokenBucketMemcached) SetState(ctx context.Context, state TokenBucketState) error {
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

	return errors.Wrap(err, "failed to save keys to memcached")
}

// Reset resets the state in Memcached.
func (t *TokenBucketMemcached) Reset(ctx context.Context) error {
	state := TokenBucketState{
		Last:      0,
		Available: 0,
	}
	// Override casId to 0 to Set instead of CompareAndSwap in SetState
	t.casId = 0

	return t.SetState(ctx, state)
}

// TokenBucketDynamoDB is a DynamoDB implementation of a TokenBucketStateBackend.
type TokenBucketDynamoDB struct {
	client        *dynamodb.Client
	tableProps    DynamoDBTableProperties
	partitionKey  string
	ttl           time.Duration
	raceCheck     bool
	latestVersion int64
	keys          map[string]types.AttributeValue
}

// NewTokenBucketDynamoDB creates a new TokenBucketDynamoDB instance.
// PartitionKey is the key used to store all the this implementation in DynamoDB.
//
// TableProps describe the table that this backend should work with. This backend requires the following on the table:
// * TTL
//
// TTL is the TTL of the stored item.
//
// If raceCheck is true and the item in DynamoDB are modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewTokenBucketDynamoDB(client *dynamodb.Client, partitionKey string, tableProps DynamoDBTableProperties, ttl time.Duration, raceCheck bool) *TokenBucketDynamoDB {
	keys := map[string]types.AttributeValue{
		tableProps.PartitionKeyName: &types.AttributeValueMemberS{Value: partitionKey},
	}

	if tableProps.SortKeyUsed {
		keys[tableProps.SortKeyName] = &types.AttributeValueMemberS{Value: partitionKey}
	}

	return &TokenBucketDynamoDB{
		client:       client,
		partitionKey: partitionKey,
		tableProps:   tableProps,
		ttl:          ttl,
		raceCheck:    raceCheck,
		keys:         keys,
	}
}

// State gets the bucket's state from DynamoDB.
func (t *TokenBucketDynamoDB) State(ctx context.Context) (TokenBucketState, error) {
	resp, err := dynamoDBGetItem(ctx, t.client, t.getGetItemInput())
	if err != nil {
		return TokenBucketState{}, err
	}

	return t.loadStateFromDynamoDB(resp)
}

// SetState updates the state in DynamoDB.
func (t *TokenBucketDynamoDB) SetState(ctx context.Context, state TokenBucketState) error {
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
func (t *TokenBucketDynamoDB) Reset(ctx context.Context) error {
	state := TokenBucketState{
		Last:      0,
		Available: 0,
	}

	return t.SetState(ctx, state)
}

const dynamoDBBucketAvailableKey = "Available"

func (t *TokenBucketDynamoDB) getGetItemInput() *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{
		TableName: &t.tableProps.TableName,
		Key:       t.keys,
	}
}

func (t *TokenBucketDynamoDB) getPutItemInputFromState(state TokenBucketState) *dynamodb.PutItemInput {
	item := map[string]types.AttributeValue{}
	for k, v := range t.keys {
		item[k] = v
	}

	item[dynamoDBBucketLastKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(state.Last, 10)}
	item[dynamoDBBucketVersionKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(t.latestVersion+1, 10)}
	item[t.tableProps.TTLFieldName] = &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(t.ttl).Unix(), 10)}
	item[dynamoDBBucketAvailableKey] = &types.AttributeValueMemberN{Value: strconv.FormatInt(state.Available, 10)}

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

func (t *TokenBucketDynamoDB) loadStateFromDynamoDB(resp *dynamodb.GetItemOutput) (TokenBucketState, error) {
	state := TokenBucketState{}

	err := attributevalue.Unmarshal(resp.Item[dynamoDBBucketLastKey], &state.Last)
	if err != nil {
		return state, fmt.Errorf("unmarshal dynamodb Last attribute failed: %w", err)
	}

	err = attributevalue.Unmarshal(resp.Item[dynamoDBBucketAvailableKey], &state.Available)
	if err != nil {
		return state, errors.Wrap(err, "unmarshal of dynamodb item attribute failed")
	}

	if t.raceCheck {
		err = attributevalue.Unmarshal(resp.Item[dynamoDBBucketVersionKey], &t.latestVersion)
		if err != nil {
			return state, fmt.Errorf("unmarshal dynamodb Version attribute failed: %w", err)
		}
	}

	return state, nil
}

// CosmosDBTokenBucketItem represents a document in CosmosDB.
type CosmosDBTokenBucketItem struct {
	ID           string           `json:"id"`
	PartitionKey string           `json:"partitionKey"`
	State        TokenBucketState `json:"state"`
	Version      int64            `json:"version"`
	TTL          int64            `json:"ttl"`
}

// TokenBucketCosmosDB is a CosmosDB implementation of a TokenBucketStateBackend.
type TokenBucketCosmosDB struct {
	client        *azcosmos.ContainerClient
	partitionKey  string
	id            string
	ttl           time.Duration
	raceCheck     bool
	latestVersion int64
}

// NewTokenBucketCosmosDB creates a new TokenBucketCosmosDB instance.
// PartitionKey is the key used to store all the implementation in CosmosDB.
// TTL is the TTL of the stored item.
//
// If raceCheck is true and the item in CosmosDB is modified in between State() and SetState() calls then
// ErrRaceCondition is returned.
func NewTokenBucketCosmosDB(client *azcosmos.ContainerClient, partitionKey string, ttl time.Duration, raceCheck bool) *TokenBucketCosmosDB {
	return &TokenBucketCosmosDB{
		client:       client,
		partitionKey: partitionKey,
		id:           "token-bucket-" + partitionKey,
		ttl:          ttl,
		raceCheck:    raceCheck,
	}
}

func (t *TokenBucketCosmosDB) State(ctx context.Context) (TokenBucketState, error) {
	var item CosmosDBTokenBucketItem
	resp, err := t.client.ReadItem(ctx, azcosmos.NewPartitionKey().AppendString(t.partitionKey), t.id, &azcosmos.ItemOptions{})
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return TokenBucketState{}, nil
		}

		return TokenBucketState{}, err
	}

	err = json.Unmarshal(resp.Value, &item)
	if err != nil {
		return TokenBucketState{}, errors.Wrap(err, "failed to decode state from Cosmos DB")
	}

	if time.Now().Unix() > item.TTL {
		return TokenBucketState{}, nil
	}

	if t.raceCheck {
		t.latestVersion = item.Version
	}

	return item.State, nil
}

func (t *TokenBucketCosmosDB) SetState(ctx context.Context, state TokenBucketState) error {
	var err error
	done := make(chan struct{}, 1)

	item := CosmosDBTokenBucketItem{
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

func (t *TokenBucketCosmosDB) Reset(ctx context.Context) error {
	state := TokenBucketState{
		Last:      0,
		Available: 0,
	}

	return t.SetState(ctx, state)
}
