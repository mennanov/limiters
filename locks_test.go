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
	s.NoError(lock.Unlock(context.Background()))
}

func (s *LimitersTestSuite) TestDistLockers() {
	locks1 := s.distLockers(false)
	locks2 := s.distLockers(false)
	for k := 0; k < len(locks1); k++ {
		s.NotEmpty(locks1[k].Name())
		s.Run(locks1[k].Name(), func() {
			var shared int
			rounds := 6
			sleep := time.Millisecond * 50
			for i := 0; i < rounds; i++ {
				wg := sync.WaitGroup{}
				wg.Add(2)
				go func(k int) {
					defer wg.Done()
					s.useLock(locks1[k], &shared, sleep)
				}(k)
				go func(k int) {
					defer wg.Done()
					s.useLock(locks2[k], &shared, sleep)
				}(k)
				wg.Wait()
			}
			s.Equal(rounds*2, shared)
		})
	}
}
