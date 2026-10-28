package shutdown

import "errors"

var (
	// ErrAlreadyStarted is returned by Register after Run has been called.
	ErrAlreadyStarted = errors.New("shutdown: already started")
	// ErrDuplicateName is returned by Register when a step name is already in use.
	ErrDuplicateName = errors.New("shutdown: duplicate step name")
	// ErrInvalidName is returned by Register for an empty step name.
	ErrInvalidName = errors.New("shutdown: step name must not be empty")
	// ErrNilStep is returned by Register for a nil step function.
	ErrNilStep = errors.New("shutdown: step function must not be nil")
	// ErrInvalidTimeout is returned by Register for a negative step timeout.
	ErrInvalidTimeout = errors.New("shutdown: step timeout must not be negative")
)
