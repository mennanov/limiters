package limiters_test

import (
	"context"
	"sync"
	"time"

	"github.com/mennanov/limiters"
)

func (s *LimitersTestSuite) useLock(lock limiters.DistLocker, shared *int, sleep time.Duration) {
	s.NoError(lock.Lock(context.TODO()))
	sh := *shared
	// Imitate heavy work...
	time.Sleep(sleep)
	// Check for the race condition.
	s.Equal(sh, *shared)
	*shared++
	s.NoError(lock.Unlock())
}

func (s *LimitersTestSuite) TestLockEtcd() {
	// Create 2 etcd locks with the same prefix.
	prefix := "prefix"
	lock1 := limiters.NewLockEtcd(s.etcdClient, prefix, s.logger)
	lock2 := limiters.NewLockEtcd(s.etcdClient, prefix, s.logger)
	var shared int
	rounds := 6
	sleep := time.Millisecond * 50
	for i := 0; i < rounds; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.useLock(lock1, &shared, sleep)
		}(i)
		go func(i int) {
			defer wg.Done()
			s.useLock(lock2, &shared, sleep)
		}(i)
		wg.Wait()
	}
	s.Equal(rounds*2, shared)
}
