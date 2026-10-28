package shutdown

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// HTTP returns a step function that shuts down srv.
//
// It calls srv.Shutdown with the step context. Shutdown stops accepting connections, closes
// idle keep-alive connections, and waits for active requests. If the context expires first,
// the step calls srv.Close, which closes the remaining connections hard, and returns an error
// that wraps the context error.
//
// The step's deadline is enforced by the runner. When it passes, the runner reports the step as
// StatusTimeout and moves on, while the hard Close may still be running in the background.
//
// Keep-alive connections are closed by Shutdown once they are idle. The step never waits
// beyond its deadline, whatever the standard library does with a connection that stays busy.
func HTTP(srv *http.Server) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		err := srv.Shutdown(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() == nil {
			return err
		}
		if cerr := srv.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return fmt.Errorf("http shutdown: connections closed hard: %w", err)
	}
}
