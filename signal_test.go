package shutdown

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

// exitRecorder returns an exit function that reports codes on a buffered channel.
func exitRecorder() (func(int), chan int) {
	codes := make(chan int, 10)
	return func(code int) { codes <- code }, codes
}

func awaitExit(t *testing.T, codes <-chan int) int {
	t.Helper()
	select {
	case code := <-codes:
		return code
	case <-time.After(runGuard):
		t.Fatalf("exit was not called within %s", runGuard)
		return -1
	}
}

func TestSignalExitCode(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name     string
		fn       func(release <-chan struct{}) func(context.Context) error
		timeout  time.Duration
		wantCode int
	}{
		{
			name: "clean run exits 0",
			fn: func(<-chan struct{}) func(context.Context) error {
				return func(context.Context) error { return nil }
			},
			wantCode: 0,
		},
		{
			name: "failed step exits 1",
			fn: func(<-chan struct{}) func(context.Context) error {
				return func(context.Context) error { return boom }
			},
			wantCode: 1,
		},
		{
			name: "global deadline exits 1",
			fn: func(release <-chan struct{}) func(context.Context) error {
				return func(context.Context) error {
					<-release
					return nil
				}
			},
			timeout:  20 * time.Millisecond,
			wantCode: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			exit, codes := exitRecorder()
			opts := []Option{WithExit(exit)}
			if tt.timeout > 0 {
				opts = append(opts, WithTimeout(tt.timeout))
			}
			s := New(opts...)
			mustRegister(t, s, "step", tt.fn(release))

			sigs := make(chan os.Signal, 2)
			stop := s.watchSignals(sigs, func() {})
			t.Cleanup(stop)

			sigs <- syscall.SIGTERM

			if got := awaitExit(t, codes); got != tt.wantCode {
				t.Errorf("exit code = %d, want %d", got, tt.wantCode)
			}
		})
	}
}

func TestSecondSignalForcesExitWithCode1(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	started := make(chan struct{}, 1)

	exit, codes := exitRecorder()
	s := New(WithExit(exit))
	mustRegister(t, s, "stuck", func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	})

	sigs := make(chan os.Signal, 2)
	stop := s.watchSignals(sigs, func() {})
	t.Cleanup(stop)

	sigs <- syscall.SIGTERM
	select {
	case <-started:
	case <-time.After(runGuard):
		t.Fatalf("first run did not start within %s", runGuard)
	}

	sigs <- syscall.SIGINT

	if got := awaitExit(t, codes); got != 1 {
		t.Errorf("exit code after second signal = %d, want 1", got)
	}
}

func TestStopRemovesSignalRegistration(t *testing.T) {
	tests := []struct {
		name    string
		signals []os.Signal
	}{
		{name: "one signal", signals: []os.Signal{syscall.SIGUSR1}},
		{name: "no signals", signals: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exit, codes := exitRecorder()
			s := New(WithSignals(tt.signals...), WithExit(exit))

			stop := s.InstallSignalHandler()
			stop()
			stop() // a second call must be a no-op

			select {
			case code := <-codes:
				t.Errorf("exit called with %d after stop, want no exit", code)
			default:
			}
		})
	}
}
