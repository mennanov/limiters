package limiters_test

import (
	"context"
	"sync"
	"testing"
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
	for name := range locks1 {
		s.Run(name, func() {
			var shared int

			rounds := 6
			sleep := time.Millisecond * 50

			for i := 0; i < rounds; i++ {
				wg := sync.WaitGroup{}
				wg.Add(2)

				go func(k string) {
					defer wg.Done()

					s.useLock(locks1[k], &shared, sleep)
				}(name)
				go func(k string) {
					defer wg.Done()

					s.useLock(locks2[k], &shared, sleep)
				}(name)

				wg.Wait()
			}

			s.Equal(rounds*2, shared)
		})
	}
}

func BenchmarkDistLockers(b *testing.B) {
	s := new(LimitersTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()

	lockers := s.distLockers(false)
	for name, locker := range lockers {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s.Require().NoError(locker.Lock(context.Background()))
				s.Require().NoError(locker.Unlock(context.Background()))
			}
		})
	}

	s.TearDownSuite()
}
