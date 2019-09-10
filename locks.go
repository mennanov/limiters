package limiters

import (
	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

// Locker is a context aware distributed locker (see sync.Locker).
type Locker interface {
	// Lock locks the locker.
	Lock(ctx context.Context) error
	// Unlock unlocks the previously successfully locked lock.
	Unlock() error
}

// LockerNoop is a no-op implementation of the Locker interface.
// It should only be used with the in-memory backends as they don't need distributed locks.
type LockerNoop struct {
}

// NewLockerNoop creates a new LockerNoop.
func NewLockerNoop() *LockerNoop {
	return &LockerNoop{}
}

// Lock imitates locking.
func (n LockerNoop) Lock(ctx context.Context) error {
	return ctx.Err()
}

// Unlock does nothing.
func (n LockerNoop) Unlock() error {
	return nil
}

// LockerEtcd implements a distributed lock with etcd.
//
// See https://github.com/etcd-io/etcd/blob/master/Documentation/learning/why.md#using-etcd-for-distributed-coordination
type LockerEtcd struct {
	cli     *clientv3.Client
	prefix  string
	logger  Logger
	mu      *concurrency.Mutex
	session *concurrency.Session
}

// NewLockerEtcd creates a new instance of LockerEtcd.
func NewLockerEtcd(cli *clientv3.Client, prefix string, logger Logger) *LockerEtcd {
	return &LockerEtcd{cli: cli, prefix: prefix, logger: logger}
}

// Lock creates a new session-based lock in etcd and locks it.
func (l *LockerEtcd) Lock(ctx context.Context) error {
	var err error
	l.session, err = concurrency.NewSession(l.cli, concurrency.WithTTL(1))
	if err != nil {
		return errors.Wrap(err, "failed to create an etcd session")
	}
	l.mu = concurrency.NewMutex(l.session, l.prefix)
	return errors.Wrap(l.mu.Lock(ctx), "failed to lock a mutex in etcd")
}

// Unlock unlocks the previously locked lock.
func (l *LockerEtcd) Unlock() error {
	defer func() {
		if err := l.session.Close(); err != nil {
			l.logger.Log(err)
		}
	}()
	return errors.Wrap(l.mu.Unlock(l.cli.Ctx()), "failed to unlock a mutex in etcd")
}
