package shutdown

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

const runGuard = 5 * time.Second

// recorder logs step names in the order they run. It is safe to use from abandoned
// goroutines, so tests can read it while such goroutines are still alive.
type recorder struct {
	mu    sync.Mutex
	names []string
}

func (r *recorder) step(name string) func(context.Context) error {
	return func(context.Context) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.names = append(r.names, name)
		return nil
	}
}

func (r *recorder) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.names...)
}

// summarize turns a report into "name:status" strings in teardown order.
func summarize(r Report) []string {
	out := make([]string, 0, len(r.Steps))
	for _, st := range r.Steps {
		out = append(out, st.Name+":"+string(st.Status))
	}
	return out
}

func mustRegister(t *testing.T, s *Shutdown, name string, fn func(context.Context) error, opts ...StepOption) {
	t.Helper()
	if err := s.Register(name, fn, opts...); err != nil {
		t.Fatalf("Register(%q) returned %v", name, err)
	}
}

// runWithin calls Run and fails the test if it does not return within runGuard.
func runWithin(t *testing.T, s *Shutdown, ctx context.Context) Report {
	t.Helper()
	ch := make(chan Report, 1)
	go func() { ch <- s.Run(ctx, "test") }()
	select {
	case r := <-ch:
		return r
	case <-time.After(runGuard):
		t.Fatalf("Run did not return within %s", runGuard)
		return Report{}
	}
}

func TestRunTeardownIsLIFO(t *testing.T) {
	s := New()
	rec := &recorder{}
	for _, name := range []string{"http", "pool", "queue"} {
		mustRegister(t, s, name, rec.step(name))
	}

	report := runWithin(t, s, context.Background())

	if got, want := rec.order(), []string{"queue", "pool", "http"}; !reflect.DeepEqual(got, want) {
		t.Errorf("run order = %v, want %v", got, want)
	}
	if got, want := summarize(report), []string{"queue:ok", "pool:ok", "http:ok"}; !reflect.DeepEqual(got, want) {
		t.Errorf("report = %v, want %v", got, want)
	}
	if report.Forced {
		t.Errorf("Forced = true, want false")
	}
}

func TestRunStepFailureDoesNotAbort(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name    string
		failing func(context.Context) error
		wantErr bool
	}{
		{name: "returned error", failing: func(context.Context) error { return boom }, wantErr: true},
		{name: "panic", failing: func(context.Context) error { panic("kaputt") }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			rec := &recorder{}
			mustRegister(t, s, "first", rec.step("first"))
			mustRegister(t, s, "failing", tt.failing)
			mustRegister(t, s, "last", rec.step("last"))

			report := runWithin(t, s, context.Background())

			if got, want := rec.order(), []string{"last", "first"}; !reflect.DeepEqual(got, want) {
				t.Errorf("run order = %v, want %v", got, want)
			}
			if got, want := summarize(report), []string{"last:ok", "failing:error", "first:ok"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("report = %v, want %v", got, want)
			}
			failed := report.Steps[1]
			if failed.Err == nil {
				t.Errorf("failing step has nil Err")
			}
			if tt.name == "returned error" && !errors.Is(failed.Err, boom) {
				t.Errorf("Err = %v, want it to wrap boom", failed.Err)
			}
			if report.Forced {
				t.Errorf("Forced = true, want false")
			}
		})
	}
}

func TestStepTimeout(t *testing.T) {
	tests := []struct {
		name string
		// fn builds the step. release is closed at cleanup and must be used by steps that block.
		fn func(release <-chan struct{}) func(context.Context) error
	}{
		{
			name: "step honours its context",
			fn: func(<-chan struct{}) func(context.Context) error {
				return func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				}
			},
		},
		{
			name: "step ignores its context",
			fn: func(release <-chan struct{}) func(context.Context) error {
				return func(context.Context) error {
					<-release
					return nil
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			s := New()
			rec := &recorder{}
			mustRegister(t, s, "after", rec.step("after"))
			mustRegister(t, s, "slow", tt.fn(release), WithStepTimeout(20*time.Millisecond))

			report := runWithin(t, s, context.Background())

			if got, want := summarize(report), []string{"slow:timeout", "after:ok"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("report = %v, want %v", got, want)
			}
			if !errors.Is(report.Steps[0].Err, context.DeadlineExceeded) {
				t.Errorf("timeout Err = %v, want it to wrap context.DeadlineExceeded", report.Steps[0].Err)
			}
			if report.Forced {
				t.Errorf("Forced = true, want false: a step timeout is not a forced run")
			}
			if got, want := rec.order(), []string{"after"}; !reflect.DeepEqual(got, want) {
				t.Errorf("run order = %v, want %v", got, want)
			}
		})
	}
}

func TestGlobalDeadlineSkipsRemainingSteps(t *testing.T) {
	s := New(WithTimeout(50 * time.Millisecond))
	rec := &recorder{}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	mustRegister(t, s, "first", rec.step("first"))
	// The blocking step ignores its context. Only the global deadline can end it.
	mustRegister(t, s, "blocking", func(context.Context) error {
		<-release
		return nil
	}, WithStepTimeout(time.Minute))
	mustRegister(t, s, "last", rec.step("last"))

	report := runWithin(t, s, context.Background())

	want := []string{"last:ok", "blocking:timeout", "first:skipped"}
	if got := summarize(report); !reflect.DeepEqual(got, want) {
		t.Fatalf("report = %v, want %v", got, want)
	}
	if !report.Forced {
		t.Errorf("Forced = false, want true")
	}
	if !errors.Is(report.Steps[1].Err, context.DeadlineExceeded) {
		t.Errorf("interrupted step Err = %v, want it to wrap context.DeadlineExceeded", report.Steps[1].Err)
	}
	if got, want := rec.order(), []string{"last"}; !reflect.DeepEqual(got, want) {
		t.Errorf("run order = %v, want %v: skipped steps must not run", got, want)
	}
}

func TestCancelledParentSkipsEverything(t *testing.T) {
	s := New()
	rec := &recorder{}
	mustRegister(t, s, "a", rec.step("a"))
	mustRegister(t, s, "b", rec.step("b"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := s.Run(ctx, "test")

	if got, want := summarize(report), []string{"b:skipped", "a:skipped"}; !reflect.DeepEqual(got, want) {
		t.Errorf("report = %v, want %v", got, want)
	}
	if !report.Forced {
		t.Errorf("Forced = false, want true")
	}
	if got := rec.order(); len(got) != 0 {
		t.Errorf("run order = %v, want no steps executed", got)
	}
}

func TestRunIgnoresContextWithoutBlocking(t *testing.T) {
	// A step that never returns must not keep Run from finishing.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	s := New()
	mustRegister(t, s, "stuck", func(context.Context) error {
		<-release
		return nil
	}, WithStepTimeout(10*time.Millisecond))

	report := runWithin(t, s, context.Background())

	if got, want := summarize(report), []string{"stuck:timeout"}; !reflect.DeepEqual(got, want) {
		t.Errorf("report = %v, want %v", got, want)
	}
}

func TestRunIsIdempotent(t *testing.T) {
	s := New()
	var mu sync.Mutex
	calls := 0
	mustRegister(t, s, "once", func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil
	})

	first := runWithin(t, s, context.Background())
	second := s.Run(context.Background(), "again")

	var wg sync.WaitGroup
	results := make([]Report, 5)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = s.Run(context.Background(), "concurrent")
		}()
	}
	wg.Wait()

	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 1 {
		t.Errorf("step ran %d times, want 1", gotCalls)
	}
	want := summarize(first)
	if got := summarize(second); !reflect.DeepEqual(got, want) {
		t.Errorf("second Run = %v, want %v", got, want)
	}
	for i, r := range results {
		if got := summarize(r); !reflect.DeepEqual(got, want) {
			t.Errorf("concurrent Run %d = %v, want %v", i, got, want)
		}
	}
}

func TestRegisterErrors(t *testing.T) {
	noop := func(context.Context) error { return nil }
	tests := []struct {
		name     string
		stepName string
		fn       func(context.Context) error
		opts     []StepOption
		afterRun bool
		wantErr  error
	}{
		{name: "valid step", stepName: "a", fn: noop},
		{name: "duplicate name", stepName: "dup", fn: noop, wantErr: ErrDuplicateName},
		{name: "empty name", stepName: "", fn: noop, wantErr: ErrInvalidName},
		{name: "nil function", stepName: "nil", fn: nil, wantErr: ErrNilStep},
		{name: "negative step timeout", stepName: "neg", fn: noop, opts: []StepOption{WithStepTimeout(-time.Second)}, wantErr: ErrInvalidTimeout},
		{name: "after run", stepName: "late", fn: noop, afterRun: true, wantErr: ErrAlreadyStarted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.name == "duplicate name" {
				mustRegister(t, s, "dup", noop)
			}
			if tt.afterRun {
				s.Run(context.Background(), "test")
			}

			err := s.Register(tt.stepName, tt.fn, tt.opts...)

			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Register returned %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Register returned %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRunNeverExitsWithoutSignal(t *testing.T) {
	var mu sync.Mutex
	var codes []int
	s := New(WithExit(func(code int) {
		mu.Lock()
		defer mu.Unlock()
		codes = append(codes, code)
	}))
	mustRegister(t, s, "a", func(context.Context) error { return errors.New("fail") })

	runWithin(t, s, context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(codes) != 0 {
		t.Errorf("exit called with %v, want no exit without a signal", codes)
	}
}

func TestNewOptionDefaults(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantDur time.Duration
	}{
		{name: "default", opts: nil, wantDur: 10 * time.Second},
		{name: "explicit", opts: []Option{WithTimeout(time.Second)}, wantDur: time.Second},
		{name: "zero is ignored", opts: []Option{WithTimeout(0)}, wantDur: 10 * time.Second},
		{name: "negative is ignored", opts: []Option{WithTimeout(-time.Second)}, wantDur: 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.opts...).timeout; got != tt.wantDur {
				t.Errorf("timeout = %s, want %s", got, tt.wantDur)
			}
		})
	}
}
