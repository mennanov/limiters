package limiters

import (
	"context"
	"database/sql"
	"time"

	lock "github.com/alessandro-c/gomemcached-lock"
	"github.com/alessandro-c/gomemcached-lock/adapters/gomemcache"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cenkalti/backoff/v3"
	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis"
	"github.com/hashicorp/consul/api"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DistLocker is a context aware distributed locker (interface is similar to sync.Locker).
type DistLocker interface {
	// Lock locks the locker.
	Lock(ctx context.Context) error
	// Unlock unlocks the previously successfully locked lock.
	Unlock(ctx context.Context) error
}

// LockNoop is a no-op implementation of the DistLocker interface.
// It should only be used with the in-memory backends as they are already thread-safe and don't need distributed locks.
type LockNoop struct{}

// NewLockNoop creates a new LockNoop.
func NewLockNoop() *LockNoop {
	return &LockNoop{}
}

// Lock imitates locking.
func (n LockNoop) Lock(ctx context.Context) error {
	return ctx.Err()
}

// Unlock does nothing.
func (n LockNoop) Unlock(_ context.Context) error {
	return nil
}

// LockEtcd implements the DistLocker interface using etcd.
//
// See https://github.com/etcd-io/etcd/blob/master/Documentation/learning/why.md#using-etcd-for-distributed-coordination
type LockEtcd struct {
	cli     *clientv3.Client
	prefix  string
	logger  Logger
	mu      *concurrency.Mutex
	session *concurrency.Session
}

// NewLockEtcd creates a new instance of LockEtcd.
func NewLockEtcd(cli *clientv3.Client, prefix string, logger Logger) *LockEtcd {
	return &LockEtcd{cli: cli, prefix: prefix, logger: logger}
}

// Lock creates a new session-based lock in etcd and locks it.
func (l *LockEtcd) Lock(ctx context.Context) error {
	var err error
	l.session, err = concurrency.NewSession(l.cli, concurrency.WithTTL(1))
	if err != nil {
		return errors.Wrap(err, "failed to create an etcd session")
	}
	l.mu = concurrency.NewMutex(l.session, l.prefix)

	return errors.Wrap(l.mu.Lock(ctx), "failed to lock a mutex in etcd")
}

// Unlock unlocks the previously locked lock.
func (l *LockEtcd) Unlock(ctx context.Context) error {
	defer func() {
		if err := l.session.Close(); err != nil {
			l.logger.Log(err)
		}
	}()

	return errors.Wrap(l.mu.Unlock(ctx), "failed to unlock a mutex in etcd")
}

// LockConsul is a wrapper around github.com/hashicorp/consul/api.Lock that implements the DistLocker interface.
type LockConsul struct {
	lock *api.Lock
}

// NewLockConsul creates a new LockConsul instance.
func NewLockConsul(lock *api.Lock) *LockConsul {
	return &LockConsul{lock: lock}
}

// Lock locks the lock in Consul.
func (l *LockConsul) Lock(ctx context.Context) error {
	_, err := l.lock.Lock(ctx.Done())

	return errors.Wrap(err, "failed to lock a mutex in consul")
}

// Unlock unlocks the lock in Consul.
func (l *LockConsul) Unlock(_ context.Context) error {
	return l.lock.Unlock()
}

// LockZookeeper is a wrapper around github.com/samuel/go-zookeeper/zk.Lock that implements the DistLocker interface.
type LockZookeeper struct {
	lock *zk.Lock
}

// NewLockZookeeper creates a new instance of LockZookeeper.
func NewLockZookeeper(lock *zk.Lock) *LockZookeeper {
	return &LockZookeeper{lock: lock}
}

// Lock locks the lock in Zookeeper.
// TODO: add context aware support once https://github.com/samuel/go-zookeeper/pull/168 is merged.
func (l *LockZookeeper) Lock(_ context.Context) error {
	return l.lock.Lock()
}

// Unlock unlocks the lock in Zookeeper.
func (l *LockZookeeper) Unlock(_ context.Context) error {
	return l.lock.Unlock()
}

// LockRedis is a wrapper around github.com/go-redsync/redsync that implements the DistLocker interface.
type LockRedis struct {
	mutex *redsync.Mutex
}

// NewLockRedis creates a new instance of LockRedis.
func NewLockRedis(pool redsyncredis.Pool, mutexName string, options ...redsync.Option) *LockRedis {
	rs := redsync.New(pool)
	mutex := rs.NewMutex(mutexName, options...)

	return &LockRedis{mutex: mutex}
}

// Lock locks the lock in Redis.
func (l *LockRedis) Lock(ctx context.Context) error {
	err := l.mutex.LockContext(ctx)

	return errors.Wrap(err, "failed to lock a mutex in redis")
}

// Unlock unlocks the lock in Redis.
func (l *LockRedis) Unlock(ctx context.Context) error {
	if ok, err := l.mutex.UnlockContext(ctx); !ok || err != nil {
		return errors.Wrap(err, "failed to unlock a mutex in redis")
	}

	return nil
}

// LockMemcached is a wrapper around github.com/alessandro-c/gomemcached-lock that implements the DistLocker interface.
// It is caller's responsibility to make sure the uniqueness of mutexName, and not to use the same key in multiple
// Memcached-based implementations.
type LockMemcached struct {
	locker    *lock.Locker
	mutexName string
	backoff   backoff.BackOff
}

// NewLockMemcached creates a new instance of LockMemcached.
// Default backoff is to retry every 100ms for 100 times (10 seconds).
func NewLockMemcached(client *memcache.Client, mutexName string) *LockMemcached {
	adapter := gomemcache.New(client)
	locker := lock.New(adapter, mutexName, "")
	b := backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 100)

	return &LockMemcached{
		locker:    locker,
		mutexName: mutexName,
		backoff:   b,
	}
}

// WithLockAcquireBackoff sets the backoff policy for retrying an operation.
func (l *LockMemcached) WithLockAcquireBackoff(b backoff.BackOff) *LockMemcached {
	l.backoff = b

	return l
}

// Lock locks the lock in Memcached.
func (l *LockMemcached) Lock(ctx context.Context) error {
	o := func() error { return l.locker.Lock(time.Minute) }

	return backoff.Retry(o, l.backoff)
}

// Unlock unlocks the lock in Memcached.
func (l *LockMemcached) Unlock(ctx context.Context) error {
	return l.locker.Release()
}

// LockPostgreSQL is an implementation of the DistLocker interface using PostgreSQL's advisory lock.
type LockPostgreSQL struct {
	db *sql.DB
	id int64
	tx *sql.Tx
}

// NewLockPostgreSQL creates a new LockPostgreSQL.
func NewLockPostgreSQL(db *sql.DB, id int64) *LockPostgreSQL {
	return &LockPostgreSQL{db, id, nil}
}

// Make sure LockPostgreSQL implements DistLocker interface.
var _ DistLocker = (*LockPostgreSQL)(nil)

// Lock acquire an advisory lock in PostgreSQL.
func (l *LockPostgreSQL) Lock(ctx context.Context) error {
	var err error
	l.tx, err = l.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	_, err = l.tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", l.id)

	return err
}

// Unlock releases an advisory lock in PostgreSQL.
func (l *LockPostgreSQL) Unlock(ctx context.Context) error {
	return l.tx.Rollback()
}
