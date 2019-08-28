// Package limiters provides general purpose rate limiter implementations.
package limiters

import (
	"context"
	"errors"
	"log"
	"time"
)

// ErrLimitExhausted means the number of requests overflows the capacity of a Limiter.
var ErrLimitExhausted = errors.New("requests limit exhausted")

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
type Clock interface {
	// Now returns the current system time.
	Now() time.Time
	// Sleep moves the clock forward for the given duration.
	Sleep(time.Duration)
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
