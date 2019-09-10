// Package limiters provides general purpose rate limiter implementations.
package limiters

import (
	"errors"
	"log"
	"time"
)

var (
	// ErrLimitExhausted is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	ErrLimitExhausted = errors.New("requests limit exhausted")

	// ErrRaceCondition is returned when there is a race condition while saving a state of a rate limiter.
	ErrRaceCondition = errors.New("race condition detected")
)

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
