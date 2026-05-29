package store

import "time"

// RetryError signals a transient failure. Return it from ITaskRunner.Run
// to reset the workitem to PENDING with a scheduled next_run_at.
//
//	return store.Retry(5 * time.Minute)   // retry in 5 minutes
//	return store.Retry(0)                 // retry immediately (next tick)
type RetryError struct {
	After time.Duration // delay from now; 0 = immediately claimable
	Cause error
}

func (e *RetryError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "transient error — will retry"
}

func (e *RetryError) Unwrap() error { return e.Cause }

// Retry returns a RetryError that reschedules the item after the given delay.
func Retry(after time.Duration) *RetryError {
	return &RetryError{After: after}
}

// RetryWithCause wraps an underlying error in a RetryError.
func RetryWithCause(after time.Duration, cause error) *RetryError {
	return &RetryError{After: after, Cause: cause}
}
