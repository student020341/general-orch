package sdk

import "errors"

// ErrTerminal is a sentinel matched by errors.Is for non-retryable errors.
var ErrTerminal = errors.New("terminal error")

type terminalError struct {
	err error
}

func (e *terminalError) Error() string {
	return e.err.Error()
}

func (e *terminalError) Unwrap() error {
	return e.err
}

func (e *terminalError) Is(target error) bool {
	return target == ErrTerminal
}

// Terminal marks err as non-retryable while preserving its wrapped error chain.
// If err is nil, Terminal returns nil.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	if IsTerminal(err) {
		return err
	}
	return &terminalError{err: err}
}

// IsTerminal reports whether err (or any error in its chain) was marked non-retryable.
func IsTerminal(err error) bool {
	return errors.Is(err, ErrTerminal)
}
