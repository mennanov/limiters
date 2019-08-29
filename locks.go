package limiters

import (
	"context"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

// Locker is a context aware distributed locker (see sync.Locker).
type Locker interface {
	// Lock locks the locker and returns a monotonically increasing fencing token assigned for the lock.
	// More on fencing tokens: http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
	Lock(ctx context.Context) (fencingToken int64, err error)
	// Unlock unlocks the previously successfully locked lock.
	Unlock() error
}

// LockerNoop is a no-op implementation of the Locker interface.
// It should only be used with the in-memory backends as they don't need distributed locks.
type LockerNoop struct {
	// mu guards the c field below.
	mu sync.Mutex
	c  int64
}

// NewLockerNoop creates a new LockerNoop.
func NewLockerNoop() *LockerNoop {
	return &LockerNoop{}
}

// Lock returns an incrementing fencing token.
// It does not actually lock anything.
func (n LockerNoop) Lock(ctx context.Context) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.c++
	return n.c, ctx.Err()
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
// The returned fencing token is the corresponding lease's ID.
func (l *LockerEtcd) Lock(ctx context.Context) (int64, error) {
	var err error
	l.session, err = concurrency.NewSession(l.cli, concurrency.WithTTL(1))
	if err != nil {
		return 0, errors.Wrap(err, "failed to create an etcd session")
	}
	l.mu = concurrency.NewMutex(l.session, l.prefix)
	return int64(l.session.Lease()), errors.Wrap(l.mu.Lock(ctx), "failed to lock a mutex in etcd")
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
