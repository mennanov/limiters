package limiters_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/mennanov/limiters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testingLimiter struct{}

func newTestingLimiter() *testingLimiter {
	return &testingLimiter{}
}

func (l *testingLimiter) Limit(context.Context) (time.Duration, error) {
	return 0, nil
}

func TestRegistry_GetOrCreate(t *testing.T) {
	registry := limiters.NewRegistry()
	called := false
	clock := newFakeClock()
	limiter := newTestingLimiter()
	l := registry.GetOrCreate("key", func() interface{} {
		called = true

		return limiter
	}, time.Second, clock.Now())
	assert.Equal(t, limiter, l)
	// Verify that the closure was called to create a value.
	assert.True(t, called)
	called = false
	l = registry.GetOrCreate("key", func() interface{} {
		called = true

		return newTestingLimiter()
	}, time.Second, clock.Now())
	assert.Equal(t, limiter, l)
	// Verify that the closure was NOT called to create a value as it already exists.
	assert.False(t, called)
}

func TestRegistry_DeleteExpired(t *testing.T) {
	registry := limiters.NewRegistry()
	clock := newFakeClock()
	// Add limiters to the registry.
	for i := 1; i <= 10; i++ {
		registry.GetOrCreate(fmt.Sprintf("key%d", i), func() interface{} {
			return newTestingLimiter()
		}, time.Second*time.Duration(i), clock.Now())
	}
	clock.Sleep(time.Second * 3)
	// "touch" the "key3" value that is about to be expired so that its expiration time is extended for 1s.
	registry.GetOrCreate("key3", func() interface{} {
		return newTestingLimiter()
	}, time.Second, clock.Now())

	assert.Equal(t, 2, registry.DeleteExpired(clock.Now()))
	for i := 1; i <= 10; i++ {
		if i <= 2 {
			assert.False(t, registry.Exists(fmt.Sprintf("key%d", i)))
		} else {
			assert.True(t, registry.Exists(fmt.Sprintf("key%d", i)))
		}
	}
}

func TestRegistry_Delete(t *testing.T) {
	registry := limiters.NewRegistry()
	clock := newFakeClock()
	item := &struct{}{}
	require.Equal(t, item, registry.GetOrCreate("key", func() interface{} {
		return item
	}, time.Second, clock.Now()))
	require.Equal(t, item, registry.GetOrCreate("key", func() interface{} {
		return &struct{}{}
	}, time.Second, clock.Now()))
	registry.Delete("key")
	assert.False(t, registry.Exists("key"))
}

// This test is expected to fail when run with the --race flag.
func TestRegistry_ConcurrentUsage(t *testing.T) {
	registry := limiters.NewRegistry()
	clock := newFakeClock()
	for i := 0; i < 10; i++ {
		go func(i int) {
			registry.GetOrCreate(strconv.Itoa(i), func() interface{} { return &struct{}{} }, 0, clock.Now())
		}(i)
	}
	for i := 0; i < 10; i++ {
		go func(i int) {
			registry.DeleteExpired(clock.Now())
		}(i)
	}
}
