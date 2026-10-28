package shutdown

import (
	"os"
	"slices"
	"time"
)

// Option configures a Shutdown created by New.
type Option func(*Shutdown)

// StepOption configures a single step passed to Register.
type StepOption func(*step)

// WithTimeout sets the global deadline for a whole Run. The default is 10 seconds.
// Non-positive values are ignored and the default stays in place.
func WithTimeout(d time.Duration) Option {
	return func(s *Shutdown) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// WithSignals sets the signals that InstallSignalHandler listens for.
// The default is SIGTERM and SIGINT.
func WithSignals(sigs ...os.Signal) Option {
	return func(s *Shutdown) {
		s.signals = slices.Clone(sigs)
	}
}

// WithLogger sets a printf-style logger for progress messages. It is silent by default.
// A nil function is ignored.
func WithLogger(logf func(format string, args ...any)) Option {
	return func(s *Shutdown) {
		if logf != nil {
			s.logf = logf
		}
	}
}

// WithExit replaces os.Exit, which is called with the exit code after a signal-triggered
// run. Tests use it to observe the code without ending the process. A nil function is ignored.
func WithExit(exit func(code int)) Option {
	return func(s *Shutdown) {
		if exit != nil {
			s.exit = exit
		}
	}
}

// WithStepTimeout sets the deadline for one step. Zero, the default, means the step uses
// the global timeout. A negative value makes Register return ErrInvalidTimeout.
func WithStepTimeout(d time.Duration) StepOption {
	return func(st *step) {
		st.timeout = d
	}
}
