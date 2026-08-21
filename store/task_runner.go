package store

import (
	"context"
	"errors"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// ITaskRunner is the single runner contract shared by every job family
// (simplejob and distributedjob). A runner is therefore interchangeable between
// them: switching a job from one family to the other is a registration + config
// change, not a code change.
//
// The runner receives the full WorkItem (payload included). It does NOT have to
// touch the lifecycle: the framework applies ApplyResult on the return value. A
// runner that wants to finalize the item itself (e.g. MarkDone together with child
// inserts in a transaction) injects an IWorkItemStore via fx into its struct and
// returns ErrHandled so the framework leaves it untouched.
type ITaskRunner interface {
	Run(ctx context.Context, item *WorkItem) error
}

// Outcome is the classification of a runner result, returned by ApplyResult.
type Outcome int

const (
	OutcomeDone    Outcome = iota // runner returned nil → MarkDone
	OutcomeHandled                // runner returned ErrHandled → left untouched
	OutcomeRetry                  // runner returned *RetryError → MarkPending
	OutcomeFailed                 // runner returned any other error → MarkFailed
)

// ApplyResult finalizes a workitem from the runner's return value, applying the
// lifecycle convention shared by all job families:
//
//	nil          → MarkDone
//	ErrHandled   → no-op (the runner already finalized the item)
//	*RetryError  → MarkPending(after)  (transient: reset to PENDING)
//	other error  → MarkFailed
//
// It returns the classified Outcome and any error raised while persisting the status.
// token è il fencing token del claim corrente (item.LockToken): i Mark* lo richiedono, così
// un worker stale non può finalizzare un item ri-claimato da un'altra replica.
func ApplyResult(ctx context.Context, items IWorkItemStore, id, token string, runErr error) (Outcome, *core.ApplicationError) {
	if runErr == nil {
		return OutcomeDone, items.MarkDone(ctx, []string{id}, token)
	}
	if errors.Is(runErr, ErrHandled) {
		return OutcomeHandled, nil
	}
	if re, ok := errors.AsType[*RetryError](runErr); ok {
		return OutcomeRetry, items.MarkPending(ctx, id, token, re.After)
	}
	return OutcomeFailed, items.MarkFailed(ctx, id, token, runErr.Error())
}
