# tiduu-shutdown-go

Ordered graceful shutdown for Go services. It tears resources down in reverse startup order, with a deadline for each step and one for the whole run, and it turns OS signals into an orderly teardown with exit codes.

Standard library only. Go 1.23 or newer.

```sh
go get github.com/yedoma-labs/tiduu-shutdown-go
```

## The problem

A Go service that receives `SIGTERM` usually goes wrong in predictable ways:

- **No handler.** The process dies at once and in-flight requests are cut off. The load balancer answers them with a 502.
- **Wrong order.** If the database pool closes before the HTTP server has drained, requests still in flight fail with query errors.
- **One resource only.** `http.Server.Shutdown` drains the server, but it knows nothing about the pool, the workers, or the rest of the teardown.
- **Steps that hang.** One stuck step blocks everything after it, and the orchestrator sends `SIGKILL` before anything is logged.

This library covers the teardown itself: ordering, deadlines, signal handling, and exit codes.

## Quickstart

This example has two resources: an HTTP server and a database pool. The pool is a fake here. Replace it with your own type that has a context-aware `Close`.

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	shutdown "github.com/yedoma-labs/tiduu-shutdown-go"
)

// pool stands in for a database pool. Close must wait for in-flight queries.
type pool struct{}

func (p *pool) Close(ctx context.Context) error { return nil }

func main() {
	db := &pool{}
	srv := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}),
	}

	sd := shutdown.New(shutdown.WithTimeout(15 * time.Second))

	// Register in startup order: the pool first, then the server that uses it.
	// Teardown runs in reverse, so the server drains before the pool closes.
	if err := sd.Register("db", db.Close); err != nil {
		log.Fatal(err)
	}
	if err := sd.Register("http", shutdown.ShutdownHTTP(srv)); err != nil {
		log.Fatal(err)
	}
	sd.InstallSignalHandler() // SIGTERM and SIGINT start the teardown

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Block here. The signal handler ends the process once the teardown has finished.
	select {}
}
```

Set your orchestrator's grace period above the global timeout. For example, use `terminationGracePeriodSeconds` in Kubernetes, so `SIGKILL` does not arrive first.

## API

```go
sd := shutdown.New(
	shutdown.WithTimeout(10*time.Second),       // global deadline for one Run (default 10s)
	shutdown.WithSignals(syscall.SIGTERM),      // signals for InstallSignalHandler (default SIGTERM, SIGINT)
	shutdown.WithLogger(log.Printf),            // optional progress logger (silent by default)
	shutdown.WithExit(os.Exit),                 // exit function (default os.Exit)
)

err := sd.Register("name", func(ctx context.Context) error { /* ... */ return nil },
	shutdown.WithStepTimeout(5*time.Second))    // per-step deadline (default: the global timeout)

report := sd.Run(ctx, "manual")                  // idempotent; never exits the process
stop := sd.InstallSignalHandler()                // returns the function that removes the registration
```

`shutdown.ShutdownHTTP(srv)` returns a step function for an `*http.Server`.

## Semantics

| Situation | Result |
|---|---|
| Order | Steps run in reverse registration order, one at a time (LIFO). |
| A step returns an error or panics | Recorded as `error`. The remaining steps still run. |
| A step exceeds its timeout | Recorded as `timeout`. The run continues. |
| A step ignores its context | Recorded as `timeout` after its deadline. `Run` does not wait for it, so the goroutine keeps running in the background. |
| The global deadline passes during a step | That step is recorded as `timeout`, every later step as `skipped`, and `Forced` is `true`. |
| The parent context is already cancelled | All steps are `skipped`, and `Forced` is `true`. |
| `Run` is called again, or from several goroutines | The first call does the work. Every call returns the same report, and no step runs twice. |
| `Register` with a duplicate or empty name, a nil function, or a negative step timeout | Returns `ErrDuplicateName`, `ErrInvalidName`, `ErrNilStep`, or `ErrInvalidTimeout`. |
| `Register` after `Run` has started | Returns `ErrAlreadyStarted`. |
| First signal | Starts `Run("signal: ...")` in a goroutine. The process exits with `0` when every step is `ok` and the run was not forced, and with `1` otherwise. |
| Second signal during the run | Aborts the run and exits with `1` immediately. |
| `Run` called without a signal | Never exits the process. |

`ShutdownHTTP` calls `srv.Shutdown` with the step context. Shutdown stops accepting connections, closes idle keep-alive connections, and waits for active requests. If the deadline passes first, it calls `srv.Close`, which closes the remaining connections hard. The runner enforces the deadline either way, so the step never blocks the run for longer than its timeout.

## Limitations

- **No health checks or readiness.** Nothing here reports `/healthz` or `/ready`, and nothing marks the instance as unready when shutdown begins. You need your own logic for that.
- **Load balancer delay is not handled.** Some load balancers keep routing traffic for a few seconds after an instance stops accepting connections. Flip your readiness endpoint or add a short `preStop` delay yourself.
- **Steps cannot be cancelled.** A step that ignores its context keeps running in the background after it has been reported as `timeout`. Make steps respect `ctx` where you can.
- **Only the resources you register are torn down.** WebSockets, gRPC servers, and raw TCP listeners need their own `Register` step.
- **Hard close may outlast its step.** When `ShutdownHTTP` hits its deadline, the runner moves on at once, and the `srv.Close` call may still be running for a moment.

## Comparison

The standard library's `http.Server.Shutdown` handles one HTTP server. It does not order several resources, and it has no deadline for the other steps.

Health checks and readiness are out of scope for this library. If you need them, implement the readiness flip yourself before calling `Run`, and keep the design small: a failing readiness endpoint plus a short delay usually covers the load balancer case.

`tiduu-shutdown-go` is the Go counterpart of `@yedoma-labs/tiduu-shutdown` for Node.js. It keeps the same semantics: LIFO order, per-step and global deadlines, and forced exits.

## Development

```sh
gofmt -l .                       # must print nothing
go vet ./...
go test -race -count=1 ./...
go build ./...
```

CI runs these checks and golangci-lint on every pull request.

## License

MIT. See [LICENSE](LICENSE).
