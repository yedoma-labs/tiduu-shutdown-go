// Package shutdown tears a Go service down in a fixed order with deadlines.
//
// Steps are registered in startup order and torn down in reverse (LIFO), one at a time.
// Each step has its own deadline, and the whole Run has a global deadline. A failing step
// does not stop the run. A step that ignores its context is reported as timed out and keeps
// running in the background; Run does not wait for it.
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"
)

const defaultTimeout = 10 * time.Second

// StepStatus is the outcome of one teardown step.
type StepStatus string

const (
	// StatusOK means the step returned nil within its deadline.
	StatusOK StepStatus = "ok"
	// StatusError means the step returned an error or panicked.
	StatusError StepStatus = "error"
	// StatusTimeout means the step exceeded its deadline, or the global deadline cut it short.
	StatusTimeout StepStatus = "timeout"
	// StatusSkipped means the global deadline or a cancelled context expired before the step started.
	StatusSkipped StepStatus = "skipped"
)

// StepResult describes the outcome of one step.
type StepResult struct {
	Name     string
	Status   StepStatus
	Duration time.Duration
	// Err is set for error and timeout results. Timeouts wrap context.DeadlineExceeded.
	Err error
}

// Report is the result of a Run.
type Report struct {
	// Steps lists the steps in teardown order (reverse registration order).
	Steps []StepResult
	// Forced is true when the global deadline or a cancelled parent context cut the run short.
	Forced bool
}

type step struct {
	name    string
	fn      func(ctx context.Context) error
	timeout time.Duration
}

// Shutdown holds the registered steps and runs the teardown. Create it with New.
type Shutdown struct {
	mu        sync.Mutex
	timeout   time.Duration
	signals   []os.Signal
	logf      func(format string, args ...any)
	exit      func(code int)
	steps     []step
	names     map[string]struct{}
	started   bool
	cancelRun context.CancelFunc

	runOnce  sync.Once
	report   Report
	exitOnce sync.Once
}

// New creates a Shutdown with the given options applied over the defaults.
func New(opts ...Option) *Shutdown {
	s := &Shutdown{
		timeout: defaultTimeout,
		signals: []os.Signal{syscall.SIGTERM, os.Interrupt},
		logf:    func(string, ...any) {},
		exit:    os.Exit,
		names:   make(map[string]struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Register adds a teardown step. Steps are registered in startup order and torn down in
// reverse. The context passed to fn carries the step deadline. Register returns an error for
// an empty or duplicate name, a nil fn, a negative step timeout, or a call after Run started.
func (s *Shutdown) Register(name string, fn func(ctx context.Context) error, opts ...StepOption) error {
	st := step{name: name, fn: fn}
	for _, opt := range opts {
		if opt != nil {
			opt(&st)
		}
	}
	if name == "" {
		return ErrInvalidName
	}
	if fn == nil {
		return ErrNilStep
	}
	if st.timeout < 0 {
		return fmt.Errorf("%w: step %q has %s", ErrInvalidTimeout, name, st.timeout)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return fmt.Errorf("register %q: %w", name, ErrAlreadyStarted)
	}
	if _, dup := s.names[name]; dup {
		return fmt.Errorf("%w: %q", ErrDuplicateName, name)
	}
	s.names[name] = struct{}{}
	s.steps = append(s.steps, st)
	return nil
}

// Run tears down all registered steps in reverse order and returns the report. The reason
// is written to the logger. Run is idempotent: the first call does the work, and later calls
// return the same report without running any step again. A concurrent call waits for the
// first one to finish. Run never exits the process.
func (s *Shutdown) Run(ctx context.Context, reason string) Report {
	s.runOnce.Do(func() {
		s.report = s.execute(ctx, reason)
	})
	return cloneReport(s.report)
}

func (s *Shutdown) execute(parent context.Context, reason string) Report {
	s.mu.Lock()
	s.started = true
	steps := slices.Clone(s.steps)
	runCtx, cancel := context.WithTimeout(parent, s.timeout)
	s.cancelRun = cancel
	globalTimeout := s.timeout
	s.mu.Unlock()
	defer cancel()

	s.logf("shutdown started (%s)", reason)

	var report Report
	for i := len(steps) - 1; i >= 0; i-- {
		st := steps[i]
		if runCtx.Err() != nil {
			report.Forced = true
			report.Steps = append(report.Steps, StepResult{Name: st.name, Status: StatusSkipped})
			s.logf("shutdown: %q skipped, deadline reached", st.name)
			continue
		}

		s.logf("shutdown: closing %q", st.name)
		res := runStep(runCtx, st, globalTimeout)
		if res.Status == StatusTimeout && runCtx.Err() != nil {
			report.Forced = true
		}
		report.Steps = append(report.Steps, res)
		s.logf("shutdown: %q %s (%s)", res.Name, res.Status, res.Duration)
	}

	s.logf("shutdown finished (forced=%t)", report.Forced)
	return report
}

// runStep executes one step under its own deadline and returns without waiting for it once
// that deadline or the run deadline has passed.
func runStep(runCtx context.Context, st step, globalTimeout time.Duration) StepResult {
	timeout := st.timeout
	if timeout == 0 {
		timeout = globalTimeout
	}
	stepCtx, cancel := context.WithTimeout(runCtx, timeout)
	defer cancel()

	started := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- callStep(stepCtx, st.fn)
	}()

	var err error
	select {
	case err = <-done:
	case <-stepCtx.Done():
		select {
		case err = <-done:
		default:
			return timedOut(st.name, runCtx, timeout, time.Since(started))
		}
	}

	res := StepResult{Name: st.name, Duration: time.Since(started)}
	switch {
	case err == nil:
		res.Status = StatusOK
	case stepCtx.Err() != nil && errors.Is(err, stepCtx.Err()):
		// The step returned because its context expired. That counts as a timeout.
		return timedOut(st.name, runCtx, timeout, res.Duration)
	default:
		res.Status = StatusError
		res.Err = fmt.Errorf("step %q: %w", st.name, err)
	}
	return res
}

func timedOut(name string, runCtx context.Context, timeout, elapsed time.Duration) StepResult {
	if cause := runCtx.Err(); cause != nil {
		return StepResult{
			Name:     name,
			Status:   StatusTimeout,
			Duration: elapsed,
			Err:      fmt.Errorf("step %q interrupted by shutdown deadline: %w", name, cause),
		}
	}
	return StepResult{
		Name:     name,
		Status:   StatusTimeout,
		Duration: elapsed,
		Err:      fmt.Errorf("step %q exceeded its timeout of %s: %w", name, timeout, context.DeadlineExceeded),
	}
}

// callStep runs fn and turns a panic into an error, so one bad step cannot crash the teardown.
func callStep(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx)
}

func (s *Shutdown) abortRun() {
	s.mu.Lock()
	cancel := s.cancelRun
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func cloneReport(r Report) Report {
	return Report{Steps: slices.Clone(r.Steps), Forced: r.Forced}
}
