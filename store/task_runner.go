package store

import (
	"context"
	"errors"
	"fmt"

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
	OutcomeRetry                  // runner returned *RetryError → MarkPending (tetto non raggiunto)
	OutcomeFailed                 // runner returned any other error → MarkFailed
	// OutcomeExhausted: il runner ha chiesto un retry ma il tetto dei ritentativi del task è
	// stato raggiunto → MarkFailed. È distinto da OutcomeFailed perché la causa è diversa:
	// l'errore era transiente, si è solo smesso di riprovare.
	OutcomeExhausted
)

// ApplyResult finalizes a workitem from the runner's return value, applying the
// lifecycle convention shared by all job families:
//
//	nil          → MarkDone
//	ErrHandled   → no-op (the runner already finalized the item)
//	*RetryError  → MarkPending(after)  (transient: reset to PENDING)
//	             → MarkFailed          se il tetto dei ritentativi è stato raggiunto
//	other error  → MarkFailed
//
// It returns the classified Outcome and any error raised while persisting the status.
//
// maxRetry è il tetto ai RITENTATIVI del task (task.Config.ResolveMaxRetry): negativo =
// illimitato, 0 = nessun ritentativo. Si misura su item.Retry, che è il numero di tentativi
// già consumati, quindi `max-retry: N` concede N ritentativi e N+1 esecuzioni in tutto.
// Senza questo controllo un errore transiente permanente — un database irraggiungibile —
// farebbe riprovare l'item per sempre.
//
// L'item serve intero e non come coppia (id, token): il fencing token è item.LockToken — i
// Mark* lo richiedono, così un worker stale non può finalizzare un item ri-claimato da un'altra
// replica — e il contatore è item.Retry.
func ApplyResult(ctx context.Context, items IWorkItemStore, item *WorkItem, maxRetry int, runErr error) (Outcome, *core.ApplicationError) {
	if runErr == nil {
		return OutcomeDone, items.MarkDone(ctx, []string{item.Id}, item.LockToken)
	}
	if errors.Is(runErr, ErrHandled) {
		return OutcomeHandled, nil
	}
	if re, ok := errors.AsType[*RetryError](runErr); ok {
		if maxRetry >= 0 && item.Retry >= maxRetry {
			return OutcomeExhausted, items.MarkFailed(ctx, item.Id, item.LockToken, exhaustedReason(maxRetry, re))
		}
		return OutcomeRetry, items.MarkPending(ctx, item.Id, item.LockToken, re.After)
	}
	return OutcomeFailed, items.MarkFailed(ctx, item.Id, item.LockToken, runErr.Error())
}

// exhaustedReason è il motivo scritto sull'item: nomina il tetto, così rileggendo la collection
// si distingue "ha smesso di riprovare" da "è fallito subito", e riporta la causa dell'ultimo
// tentativo, che è l'informazione con cui si capisce perché.
func exhaustedReason(maxRetry int, re *RetryError) string {
	if re.Cause != nil {
		return fmt.Sprintf("superati i %d ritentativi previsti: %s", maxRetry, re.Cause.Error())
	}
	return fmt.Sprintf("superati i %d ritentativi previsti", maxRetry)
}
