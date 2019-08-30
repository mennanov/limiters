package limiters

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
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
	SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error
}

// TokenBucket implements the https://en.wikipedia.org/wiki/Token_bucket algorithm.
type TokenBucket struct {
	Locker
	TokenBucketStateBackend
	Clock
	Logger
	// RefillRate is the tokens refill rate.
	RefillRate time.Duration
	// Capacity is the bucket's capacity.
	Capacity int64
	mu       sync.Mutex
}

// NewTokenBucket creates a new instance of TokenBucket.
func NewTokenBucket(capacity int64, refillRate time.Duration, locker Locker, tokenBucketStateBackend TokenBucketStateBackend, clock Clock, logger Logger) *TokenBucket {
	return &TokenBucket{
		Locker:                  locker,
		TokenBucketStateBackend: tokenBucketStateBackend,
		Clock:                   clock,
		Logger:                  logger,
		RefillRate:              refillRate,
		Capacity:                capacity,
	}
}

// Take takes tokens from the bucket.
//
// It returns a zero duration and a nil error if the bucket has sufficient amount of tokens.
//
// It returns ErrLimitExhausted if the amount of available tokens is less than requested. In this case the returned
// duration is the amount of time to wait to retry the request.
func (t *TokenBucket) Take(ctx context.Context, tokens int64) (time.Duration, error) {
	now := t.Clock.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	fencingToken, err := t.Lock(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := t.Unlock(); err != nil {
			t.Logger.Log(err)
		}
	}()
	state, err := t.TokenBucketStateBackend.State(ctx)
	if err != nil {
		return 0, err
	}
	if state.isZero() {
		// Initially the bucket is full.
		state.Available = t.Capacity
	}
	// Refill the bucket.
	tokensToAdd := (now - state.Last) / int64(t.RefillRate)
	if tokensToAdd > 0 {
		state.Last = now
		if tokensToAdd+state.Available <= t.Capacity {
			state.Available += tokensToAdd
		} else {
			state.Available = t.Capacity
		}
	}

	if tokens > state.Available {
		return t.RefillRate * time.Duration(tokens-state.Available), ErrLimitExhausted
	}
	// Take the tokens from the bucket.
	state.Available -= tokens
	if err := t.SetState(ctx, state, fencingToken); err != nil {
		return 0, err
	}
	return 0, nil
}

// Limit takes 1 token from the bucket.
func (t *TokenBucket) Limit(ctx context.Context) (time.Duration, error) {
	return t.Take(ctx, 1)
}

// TokenBucketInMemory is an in-memory implementation of TokenBucketStateBackend.
//
// The state is not shared nor persisted so it won't survive restarts or failures.
// Due to the local nature of the state the rate at which some endpoints are accessed can't be reliably predicted or
// limited.
//
// Although it can be used as a global rate limiter with a round-robin load-balancer.
type TokenBucketInMemory struct {
	state        TokenBucketState
	fencingToken int64
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
func (t *TokenBucketInMemory) SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error {
	if fencingToken < t.fencingToken {
		return ErrFencingTokenExpired
	}
	t.state = state
	t.fencingToken = fencingToken
	return ctx.Err()
}

const (
	etcdKeyTBLease        = "lease"
	etcdKeyTBAvailable    = "available"
	etcdKeyTBLast         = "last"
	etcdKeyTBFencingToken = "fencing_token"
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
// access to the business critical endpoints where each access must be reliably logged (e.g. change password, withdraw
// funds).
type TokenBucketEtcd struct {
	// prefix is the etcd key prefix.
	prefix  string
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	ttl     time.Duration
}

// NewTokenBucketEtcd creates a new TokenBucketEtcd instance.
// Prefix is used as an etcd key prefix for all keys stored in etcd by this algorithm.
// TTL is a TTL of the etcd lease in seconds used to store all the keys: all they keys are automatically deleted after
// the TTL expires.
func NewTokenBucketEtcd(cli *clientv3.Client, prefix string, ttl time.Duration) *TokenBucketEtcd {
	return &TokenBucketEtcd{
		prefix: prefix,
		cli:    cli,
		ttl:    ttl,
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
	for _, kv := range r.Kvs {
		switch string(kv.Key) {
		case etcdKey(t.prefix, etcdKeyTBAvailable):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Available = v
			parsed |= 1

		case etcdKey(t.prefix, etcdKeyTBLast):
			v, err := parseEtcdInt64(kv)
			if err != nil {
				return TokenBucketState{}, err
			}
			state.Last = v
			parsed |= 2

		case etcdKey(t.prefix, etcdKeyTBLease):
			v, err := parseEtcdInt64(kv)
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
func (t *TokenBucketEtcd) save(ctx context.Context, state TokenBucketState, fencingToken int64) error {
	// Put the keys only if the fencing token does not exist or its value is <= the given value.
	// etcd does not support logical OR, so inverse the logic about (using De Morgan's law).
	r, err := t.cli.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(etcdKey(t.prefix, etcdKeyTBFencingToken)), "!=", 0),
		clientv3.Compare(clientv3.Value(etcdKey(t.prefix, etcdKeyTBFencingToken)), ">", fmt.Sprintf("%d", fencingToken)),
	).Else(
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBAvailable), fmt.Sprintf("%d", state.Available), clientv3.WithLease(t.leaseID)),
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLast), fmt.Sprintf("%d", state.Last), clientv3.WithLease(t.leaseID)),
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBLease), fmt.Sprintf("%d", t.leaseID), clientv3.WithLease(t.leaseID)),
		clientv3.OpPut(etcdKey(t.prefix, etcdKeyTBFencingToken), fmt.Sprintf("%d", fencingToken), clientv3.WithLease(t.leaseID)),
	).Commit()
	if err != nil {
		return errors.Wrap(err, "failed to commit a transaction to etcd")
	}

	if !r.Succeeded {
		return nil
	}
	return ErrFencingTokenExpired
}

// SetState updates the state of the bucket.
func (t *TokenBucketEtcd) SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error {
	if t.leaseID == 0 {
		// Lease does not exist, create one.
		if err := t.createLease(ctx); err != nil {
			return err
		}
		// No need to send KeepAlive for the newly created lease: save the state immediately.
		return t.save(ctx, state, fencingToken)
	}
	// Send the KeepAlive request to extend the existing lease.
	if _, err := t.cli.KeepAliveOnce(ctx, t.leaseID); err == rpctypes.ErrLeaseNotFound {
		// Create a new lease since the current one has expired.
		if err := t.createLease(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return errors.Wrapf(err, "failed to extend the lease '%d'", t.leaseID)
	}
	return t.save(ctx, state, fencingToken)
}

const (
	redisKeyTBAvailable    = "available"
	redisKeyTBLast         = "last"
	redisKeyTBFencingToken = "fencing_token"
)

func redisKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
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
	cli          *redis.Client
	prefix       string
	ttl          time.Duration
	fencingToken int64
}

// NewTokenBucketRedis creates a new TokenBucketRedis instance.
// Prefix is the key prefix used to store all the keys used in this implementation in Redis.
// TTL is the TTL of the stored keys.
func NewTokenBucketRedis(cli *redis.Client, prefix string, ttl time.Duration) *TokenBucketRedis {
	return &TokenBucketRedis{cli: cli, prefix: prefix, ttl: ttl}
}

// State gets the bucket's state from Redis.
func (t *TokenBucketRedis) State(ctx context.Context) (TokenBucketState, error) {
	var values []interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		values, err = t.cli.MGet(
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
			redisKey(t.prefix, redisKeyTBFencingToken),
		).Result()
		done <- struct{}{}
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
	if nilAny || err == redis.Nil {
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
	t.fencingToken, err = strconv.ParseInt(values[2].(string), 10, 64)
	if err != nil {
		return TokenBucketState{}, err
	}
	return TokenBucketState{
		Last:      last,
		Available: available,
	}, nil
}

// SetState updates the state in Redis.
func (t *TokenBucketRedis) SetState(ctx context.Context, state TokenBucketState, fencingToken int64) error {
	var result interface{}
	var err error
	done := make(chan struct{}, 1)
	go func() {
		result, err = t.cli.Eval(`
	local token = tonumber(redis.call('get', KEYS[1])) or 0
	if token <= tonumber(ARGV[1]) then
		return {
			redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[4]),
			redis.call('set', KEYS[2], ARGV[2], 'PX', ARGV[4]),
			redis.call('set', KEYS[3], ARGV[3], 'PX', ARGV[4]),
		}
	else
		return 'TOKEN_EXPIRED'
	end
	`, []string{
			redisKey(t.prefix, redisKeyTBFencingToken),
			redisKey(t.prefix, redisKeyTBLast),
			redisKey(t.prefix, redisKeyTBAvailable),
		},
			fencingToken,
			state.Last,
			state.Available,
			// TTL in milliseconds: 1e9/1e6.
			int64(t.ttl/time.Microsecond)).Result()
		done <- struct{}{}
	}()

	select {
	case <-done:

	case <-ctx.Done():
		return ctx.Err()
	}

	if err != nil {
		return errors.Wrap(err, "failed to save keys to redis")
	}
	return checkResponseFromRedis(result, 3)
}
