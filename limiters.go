// Package limiters provides general purpose rate limiter implementations.
package limiters

import (
	"context"
	"errors"
	"log"
	"time"
)

var (
	// ErrLimitExhausted is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	ErrLimitExhausted = errors.New("requests limit exhausted")

	// ErrFencingTokenExpired is returned when the token obtained from a Locker.Lock() has expired.
	// It happens when a state on a backend was modified using a lock that was created later than the current one.
	ErrFencingTokenExpired = errors.New("fencing token expired")
)

// Limiter is the interface that wraps the Limit method.
type Limiter interface {
	// Limit returns the time duration to wait before processing the request.
	// It returns ErrLimitExhausted if the request overflows the Limiter's capacity.
	Limit(context.Context) (time.Duration, error)
}

// Logger wraps the Log method for logging.
type Logger interface {
	// Log logs the given arguments.
	Log(v ...interface{})
}

// StdLogger implements the Logger interface.
type StdLogger struct{}

// NewStdLogger creates a new instance of StdLogger.
func NewStdLogger() *StdLogger {
	return &StdLogger{}
}

// Log delegates the logging to the std logger.
func (l *StdLogger) Log(v ...interface{}) {
	log.Println(v...)
}

// Clock encapsulates a system Clock.
// Used
type Clock interface {
	// Now returns the current system time.
	Now() time.Time
}

// SystemClock implements the Clock interface by using the real system clock.
type SystemClock struct {
}

// NewSystemClock creates a new instance of SystemClock.
func NewSystemClock() *SystemClock {
	return &SystemClock{}
}

// Now returns the current system time.
func (c *SystemClock) Now() time.Time {
	return time.Now()
}

// Sleep blocks (sleeps) for the given duration.
func (c *SystemClock) Sleep(d time.Duration) {
	time.Sleep(d)
}
