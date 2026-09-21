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
	ClaimPending(ctx context.Context, taskName, destination, objectType string, limit int) ([]*WorkItem, *core.ApplicationError)
	// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
	// refreshing locked_at to now and incrementing retry. Returns the items for
	// immediate processing in the current run — no reset to PENDING, no waiting for
	// the next tick. destination and objectType are optional — pass "" to skip.
	RecoverOrphans(ctx context.Context, taskName, destination, objectType string, maxAge time.Duration, limit int) ([]*WorkItem, *core.ApplicationError)
	// MarkDone transitions IN_PROGRESS items to DONE in batch. È fenced dal token: solo gli
	// item il cui lock_token coincide con quello passato transitano (gli id passati devono
	// condividere lo stesso token del claim — vero per il gruppo orfani o il gruppo fresh di un
	// tick). È idempotente: gli id non matchati (già finalizzati o token stale) sono ignorati
	// (log Debug), senza errore. Chi finalizza un singolo item passa una slice a 1 elemento.
	MarkDone(ctx context.Context, ids []string, token string) *core.ApplicationError
	MarkFailed(ctx context.Context, id, token, reason string) *core.ApplicationError
	// Release riporta a PENDING un item claimato che NON è stato eseguito — il dispatch non è
	// riuscito (pool saturo, worker irraggiungibile) e nessun runner l'ha mai visto.
	//
	// È distinta da MarkPending proprio per il contatore: Release NON incrementa retry, perché
	// un tentativo che non è avvenuto non è un tentativo. Senza di lei l'item resterebbe
	// IN_PROGRESS fino a RecoverOrphans, che oltre a farlo aspettare l'orphan timeout gli
	// consuma un ritentativo — con max-retry configurato, una saturazione temporanea del pool
	// esaurisce il budget di item mai eseguiti.
	//
	// È fenced dal token come i Mark*, e idempotente: un id non matchato è ignorato.
	Release(ctx context.Context, id, token string) *core.ApplicationError
	// MarkPending resets an item back to PENDING and increments retry.
	// Use this when a task returns store.RetryError.
	// retryDelay controls when the item becomes claimable again:
	//   0              → next_run_at = now (immediately claimable)
	//   > 0            → next_run_at = now + retryDelay
	// È fenced dal token come gli altri Mark*.
	MarkPending(ctx context.Context, id, token string, retryDelay time.Duration) *core.ApplicationError
	Insert(ctx context.Context, items []*WorkItem) *core.ApplicationError
	// InsertIfNotActive inserts workitems only when no PENDING or IN_PROGRESS entry
	// already exists for the same (task_name, object_id). Returns the number inserted.
	// Safe to call repeatedly — already-active items are silently skipped.
	InsertIfNotActive(ctx context.Context, items []*WorkItem) (int, *core.ApplicationError)
	// HasActive returns true when at least one PENDING or IN_PROGRESS item
	// exists for the given taskName and objectId.
	HasActive(ctx context.Context, taskName, objectId string) (bool, *core.ApplicationError)
	// GetById returns the WorkItem with the given id, or a NotFound error.
	GetById(ctx context.Context, id string) (*WorkItem, *core.ApplicationError)
	// DeleteIfPending deletes the item with the given id only if its status is PENDING.
	// Returns (true, nil) if deleted, (false, nil) if the item is not found or is no longer PENDING.
	DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError)
	// Purge cancella definitivamente gli item nello stato indicato più vecchi di olderThan
	// (confrontando update_time), al più limit per chiamata. Ritorna quanti ne ha cancellati.
	//
	// Esiste perché senza di lei work_items cresce per sempre: gli item DONE non venivano mai
	// rimossi, e con la collection crescono gli indici su cui gira il claim di ogni tick. Il
	// limit serve a tenere corta la singola transazione: la cancellazione è ripetuta a ogni
	// tick del job di retention, non fatta tutta in una volta.
	//
	// Non ha un default e non viene chiamata da sola: la retention si abilita in config
	// (job type PurgeWorkItems). Cancellare dati non può essere un comportamento che si
	// ottiene aggiornando la libreria.
	Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, *core.ApplicationError)
	// Backlog ritorna quanti item PENDING sono in attesa per i filtri dati e la data di
	// creazione del più vecchio (zero se non ce ne sono). È il numero su cui si costruisce un
	// alert — "la coda cresce", "c'è un item fermo" — che i counter di claimed/processed non
	// danno: dalla loro differenza non si distingue una coda stabile da una che si allunga.
	//
	// È una query in più per tick, quindi la paga solo chi la abilita (property backlog-metrics).
	Backlog(ctx context.Context, taskName, destination, objectType string) (pending int, oldest time.Time, appErr *core.ApplicationError)
	// List returns a paginated list of workitems filtered by type and optionally by status.
	// Pass status="" to include all statuses.
	// sort controls the order; pass nil to use the default (createTime DESC).
	// paging must be pre-initialised by the caller (page.InitPaging); List updates TotalCount in place.
	List(ctx context.Context, taskName, status string, paging *page.Paging, sort page.SortRequest) ([]*WorkItem, *core.ApplicationError)
}
