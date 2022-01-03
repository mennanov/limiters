package limiters

import (
	"context"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DistLocker is a context aware distributed locker (interface is similar to sync.Locker).
type DistLocker interface {
	// Lock locks the locker.
	Lock(ctx context.Context) error
	// Unlock unlocks the previously successfully locked lock.
	Unlock() error
}

// LockNoop is a no-op implementation of the DistLocker interface.
// It should only be used with the in-memory backends as they are already thread-safe and don't need distributed locks.
type LockNoop struct {
}

// NewLockNoop creates a new LockNoop.
func NewLockNoop() *LockNoop {
	return &LockNoop{}
}

// Lock imitates locking.
func (n LockNoop) Lock(ctx context.Context) error {
	return ctx.Err()
}

// Unlock does nothing.
func (n LockNoop) Unlock() error {
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
func (l *LockEtcd) Unlock() error {
	defer func() {
		if err := l.session.Close(); err != nil {
			l.logger.Log(err)
		}
	}()
	return errors.Wrap(l.mu.Unlock(l.cli.Ctx()), "failed to unlock a mutex in etcd")
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
func (l *LockConsul) Unlock() error {
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
func (l *LockZookeeper) Unlock() error {
	return l.lock.Unlock()
}
