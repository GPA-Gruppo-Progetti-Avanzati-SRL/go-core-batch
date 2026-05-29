package store

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// IWorkItemStore is the persistence interface for the outbox/work-item pattern.
// Implementations live in store/mongostore and store/sqlstore.
type IWorkItemStore interface {
	FindPending(ctx context.Context, workType, destination, objectType string) ([]*WorkItem, *core.ApplicationError)
	// ClaimPending atomically selects up to limit PENDING items of the given type,
	// marks them IN_PROGRESS (with locked_at = now), and returns them.
	// Safe for concurrent use across multiple replicas (SKIP LOCKED on SQL, findAndModify on Mongo).
	ClaimPending(ctx context.Context, workType string, limit int) ([]*WorkItem, *core.ApplicationError)
	// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
	// refreshing locked_at to now and incrementing retry. Returns the items for
	// immediate processing in the current run — no reset to PENDING, no waiting for
	// the next tick. Safe across replicas (SKIP LOCKED on SQL, optimistic on Mongo).
	RecoverOrphans(ctx context.Context, workType string, maxAge time.Duration, limit int) ([]*WorkItem, *core.ApplicationError)
	MarkDone(ctx context.Context, ids []string) *core.ApplicationError
	MarkFailed(ctx context.Context, id, reason string) *core.ApplicationError
	Insert(ctx context.Context, items []*WorkItem) *core.ApplicationError
	// InsertIfNotActive inserts workitems only when no PENDING or IN_PROGRESS entry
	// already exists for the same (type, object_id). Returns the number inserted.
	// Safe to call repeatedly — already-active items are silently skipped.
	InsertIfNotActive(ctx context.Context, items []*WorkItem) (int, *core.ApplicationError)
}
