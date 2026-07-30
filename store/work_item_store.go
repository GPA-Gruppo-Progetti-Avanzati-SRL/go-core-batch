package store

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
)

// IWorkItemStore is the persistence interface for the outbox/work-item pattern.
// Implementations live in store/mongostore and store/sqlstore.
type IWorkItemStore interface {
	// ClaimPending atomically selects up to limit PENDING items matching the given filters,
	// marks them IN_PROGRESS (with locked_at = now), and returns them.
	// destination and objectType are optional — pass "" to skip.
	// Safe for concurrent use across multiple replicas (SKIP LOCKED on SQL, optimistic on Mongo).
	ClaimPending(ctx context.Context, workType, destination, objectType string, limit int) ([]*WorkItem, *core.ApplicationError)
	// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
	// refreshing locked_at to now and incrementing retry. Returns the items for
	// immediate processing in the current run — no reset to PENDING, no waiting for
	// the next tick. destination and objectType are optional — pass "" to skip.
	RecoverOrphans(ctx context.Context, workType, destination, objectType string, maxAge time.Duration, limit int) ([]*WorkItem, *core.ApplicationError)
	// MarkDone transitions IN_PROGRESS items to DONE in batch. È fenced dal token: solo gli
	// item il cui lock_token coincide con quello passato transitano (gli id passati devono
	// condividere lo stesso token del claim — vero per il gruppo orfani o il gruppo fresh di un
	// tick). È idempotente: gli id non matchati (già finalizzati o token stale) sono ignorati
	// (log Debug), senza errore. Chi finalizza un singolo item passa una slice a 1 elemento.
	MarkDone(ctx context.Context, ids []string, token string) *core.ApplicationError
	MarkFailed(ctx context.Context, id, token, reason string) *core.ApplicationError
	// MarkPending resets an item back to PENDING and increments retry.
	// Use this when a task returns store.RetryError.
	// retryDelay controls when the item becomes claimable again:
	//   0              → next_run_at = now (immediately claimable)
	//   > 0            → next_run_at = now + retryDelay
	// È fenced dal token come gli altri Mark*.
	MarkPending(ctx context.Context, id, token string, retryDelay time.Duration) *core.ApplicationError
	Insert(ctx context.Context, items []*WorkItem) *core.ApplicationError
	// InsertIfNotActive inserts workitems only when no PENDING or IN_PROGRESS entry
	// already exists for the same (type, object_id). Returns the number inserted.
	// Safe to call repeatedly — already-active items are silently skipped.
	InsertIfNotActive(ctx context.Context, items []*WorkItem) (int, *core.ApplicationError)
	// HasActive returns true when at least one PENDING or IN_PROGRESS item
	// exists for the given workType and objectId.
	HasActive(ctx context.Context, workType, objectId string) (bool, *core.ApplicationError)
	// GetById returns the WorkItem with the given id, or a NotFound error.
	GetById(ctx context.Context, id string) (*WorkItem, *core.ApplicationError)
	// DeleteIfPending deletes the item with the given id only if its status is PENDING.
	// Returns (true, nil) if deleted, (false, nil) if the item is not found or is no longer PENDING.
	DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError)
	// List returns a paginated list of workitems filtered by type and optionally by status.
	// Pass status="" to include all statuses.
	// sort controls the order; pass nil to use the default (createTime DESC).
	// paging must be pre-initialised by the caller (page.InitPaging); List updates TotalCount in place.
	List(ctx context.Context, workType, status string, paging *page.Paging, sort page.SortRequest) ([]*WorkItem, *core.ApplicationError)
}
