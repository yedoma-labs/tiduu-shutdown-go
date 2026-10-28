package shutdown

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestShutdownHTTPWithIdleKeepAlive(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(ts.Close)

	client := ts.Client()
	t.Cleanup(client.CloseIdleConnections)

	// Read the body fully so the keep-alive connection goes back to the idle pool.
	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), runGuard)
	defer cancel()
	if err := ShutdownHTTP(ts.Config)(ctx); err != nil {
		t.Fatalf("ShutdownHTTP with an idle keep-alive connection returned %v, want nil", err)
	}
}

func TestShutdownHTTPHardCloseAfterDeadline(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{
			name: "cancelled context",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "expired deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Unix(0, 0))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				started <- struct{}{}
				<-release
			}))
			// Cleanups run last-in first-out: release the handler before the server closes.
			t.Cleanup(ts.Close)
			t.Cleanup(func() { close(release) })

			client := ts.Client()
			t.Cleanup(client.CloseIdleConnections)
			reqErr := make(chan error, 1)
			go func() {
				resp, err := client.Get(ts.URL)
				if err == nil {
					_ = resp.Body.Close()
				}
				reqErr <- err
			}()

			select {
			case <-started:
			case <-time.After(runGuard):
				t.Fatalf("request did not reach the handler within %s", runGuard)
			}

			ctx, cancel := tt.ctx()
			defer cancel()
			err := ShutdownHTTP(ts.Config)(ctx)

			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("ShutdownHTTP returned %v, want an error wrapping the context error", err)
			}
			select {
			case e := <-reqErr:
				if e == nil {
					t.Errorf("in-flight request succeeded after hard close, want a connection error")
				}
			case <-time.After(runGuard):
				t.Fatalf("in-flight request was not terminated within %s", runGuard)
			}
		})
	}
}

func TestShutdownHTTPAsRunnerStep(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(ts.Close)

	s := New()
	mustRegister(t, s, "http", ShutdownHTTP(ts.Config))

	report := runWithin(t, s, context.Background())

	if got, want := summarize(report), []string{"http:ok"}; !reflect.DeepEqual(got, want) {
		t.Errorf("report = %v, want %v", got, want)
	}
}
