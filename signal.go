package shutdown

import (
	"context"
	"os"
	"os/signal"
	"sync"
)

// InstallSignalHandler listens for the configured signals (default SIGTERM and SIGINT).
// The first signal starts Run with the reason "signal: <name>" in a goroutine. When the run
// has finished, the process exits with 0 if every step returned StatusOK and the run was not
// forced, and with 1 otherwise. A second signal while the run is active aborts it and exits
// with 1 right away. Without a signal, nothing exits.
//
// The returned function removes the signal registration. A run that is already in progress
// keeps going and still exits the process when it ends. A second signal is no longer handled
// after stop has been called.
func (s *Shutdown) InstallSignalHandler() (stop func()) {
	if len(s.signals) == 0 {
		return func() {}
	}
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, s.signals...)
	return s.watchSignals(ch, func() { signal.Stop(ch) })
}

// watchSignals consumes signals from ch. It is split out so tests can inject their own
// signal source instead of sending real OS signals.
func (s *Shutdown) watchSignals(ch <-chan os.Signal, release func()) (stop func()) {
	quit := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		select {
		case sig := <-ch:
			go s.runFromSignal(sig)
		case <-quit:
			return
		}

		select {
		case sig := <-ch:
			s.forceExit(sig)
		case <-quit:
		}
	}()

	return func() {
		stopOnce.Do(func() {
			release()
			close(quit)
		})
	}
}

func (s *Shutdown) runFromSignal(sig os.Signal) {
	report := s.Run(context.Background(), "signal: "+sig.String())
	code := 0
	if report.Forced || !allOK(report.Steps) {
		code = 1
	}
	s.exitOnce.Do(func() { s.exit(code) })
}

func (s *Shutdown) forceExit(sig os.Signal) {
	s.logf("shutdown: second %s received, forcing exit", sig.String())
	s.abortRun()
	s.exitOnce.Do(func() { s.exit(1) })
}

func allOK(steps []StepResult) bool {
	for _, st := range steps {
		if st.Status != StatusOK {
			return false
		}
	}
	return true
}
