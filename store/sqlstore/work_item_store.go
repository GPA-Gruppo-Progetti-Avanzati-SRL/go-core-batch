package sqlstore

import (
	"context"
	"strings"
	"sync"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/rs/zerolog/log"

	"github.com/uptrace/bun"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type workItemFilter struct {
	Id          string   `col:"id"          op:"="  omitempty:"true"`
	IdIn        []string `col:"id"          op:"IN" omitempty:"true"`
	Type        string   `col:"type"        op:"="  omitempty:"true"`
	Status      string   `col:"status"      op:"="  omitempty:"true"`
	Destination string   `col:"destination" op:"="  omitempty:"true"`
	ObjectType  string   `col:"object_type" op:"="  omitempty:"true"`
}

func (f workItemFilter) GetFilterTableName(ctx context.Context) string {
	return store.TableWorkItems
}

// workItemDataSQL implements store.IWorkItemStore using a SQL database via bun.
type workItemDataSQL struct {
	DB          *bun.DB
	idxWarnOnce sync.Once
}

func newWorkItemDataSQL(db *bun.DB) *workItemDataSQL {
	return &workItemDataSQL{DB: db}
}

var _ store.IWorkItemStore = (*workItemDataSQL)(nil)

// warnIfActiveIndexMissing logga (una sola volta) un warning se l'indice partiale unico
// uk_workitem_active non esiste. Senza quell'indice InsertIfNotActive (ON CONFLICT DO NOTHING)
// non deduplica → rischio work item duplicati e doppia esecuzione. L'indice NON viene creato
// in automatico (gestione manuale via EnsureIndexes o migration/ops).
func (d *workItemDataSQL) warnIfActiveIndexMissing(ctx context.Context) {
	d.idxWarnOnce.Do(func() {
		var n int
		if err := d.DB.NewRaw(
			"SELECT COUNT(*) FROM pg_indexes WHERE indexname = ?", "uk_workitem_active",
		).Scan(ctx, &n); err != nil {
			log.Warn().Err(err).Msg("go-core-batch: impossibile verificare l'indice uk_workitem_active")
			return
		}
		if n == 0 {
			log.Warn().Msg("go-core-batch: indice partiale unico 'uk_workitem_active' ASSENTE su work_items — InsertIfNotActive NON deduplica (rischio work item duplicati / doppia esecuzione). Crearlo via sqlstore.EnsureIndexes o migration, oppure confermare che l'assenza è voluta.")
		}
	})
}

func (d *workItemDataSQL) FindPending(ctx context.Context, workType, destination, objectType string) ([]*store.WorkItem, *core.ApplicationError) {
	filter := workItemFilter{
		Type:        workType,
		Status:      store.StatusPending,
		Destination: destination,
		ObjectType:  objectType,
	}
	sort := page.SortRequest{{Field: "create_time", Dir: page.Asc}}
	return coresql.GetAllByFilterSorted[store.WorkItem](ctx, d.DB, filter, sort)
}

// ClaimPending atomically selects up to limit PENDING items of workType,
// marks them IN_PROGRESS with locked_at = now, and returns the full records.
// Uses SELECT FOR UPDATE SKIP LOCKED — safe across multiple replicas.
func (d *workItemDataSQL) ClaimPending(ctx context.Context, workType, destination, objectType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	now := time.Now()
	token := store.NewLockToken()
	host := store.Hostname()
	var items []*store.WorkItem
	err := d.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		q := `SELECT * FROM work_items WHERE type = ? AND status = ?
			  AND (next_run_at IS NULL OR next_run_at <= NOW())`
		args := []any{workType, store.StatusPending}
		if destination != "" {
			q += ` AND destination = ?`
			args = append(args, destination)
		}
		if objectType != "" {
			q += ` AND object_type = ?`
			args = append(args, objectType)
		}
		q += ` ORDER BY next_run_at ASC NULLS FIRST, create_time ASC LIMIT ? FOR UPDATE SKIP LOCKED`
		args = append(args, limit)
		if err := tx.NewRaw(q, args...).Scan(ctx, &items); err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		ids := make([]string, len(items))
		for i, it := range items {
			ids[i] = it.Id
			it.Status = store.StatusInProgress
			it.LockedAt = &now
			it.LockToken = token
			it.LockedBy = host
		}
		_, err := tx.NewUpdate().TableExpr(store.TableWorkItems).
			Set("status = ?", store.StatusInProgress).
			Set("locked_at = ?", now).
			Set("update_time = ?", now).
			Set("lock_token = ?", token).
			Set("locked_by = ?", host).
			Where("id IN (?)", bun.List(ids)).
			Exec(ctx)
		return err
	})
	if err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	return items, nil
}

// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
// refreshing locked_at to now and incrementing retry. Returns the items for
// immediate processing — no reset to PENDING, no waiting for the next tick.
// Uses a CTE with FOR UPDATE SKIP LOCKED so it is safe across replicas.
func (d *workItemDataSQL) RecoverOrphans(ctx context.Context, workType, destination, objectType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()

	token := store.NewLockToken()
	host := store.Hostname()

	where := `type = ? AND status = ? AND locked_at < ?`
	args := []any{workType, store.StatusInProgress, cutoff}
	if destination != "" {
		where += ` AND destination = ?`
		args = append(args, destination)
	}
	if objectType != "" {
		where += ` AND object_type = ?`
		args = append(args, objectType)
	}
	args = append(args, limit, now, token, host, now)

	var items []*store.WorkItem
	err := d.DB.NewRaw(`
		WITH recovered AS (
			SELECT id FROM work_items
			WHERE `+where+`
			ORDER BY locked_at ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		UPDATE work_items
		SET locked_at = ?, lock_token = ?, locked_by = ?, retry = retry + 1, update_time = ?
		WHERE id IN (SELECT id FROM recovered)
		RETURNING *
	`, args...).Scan(ctx, &items)
	if err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	return items, nil
}

// MarkDone transitions a single IN_PROGRESS item to DONE, fenced dal token (WHERE lock_token = ?).
// Idempotente: 0 righe (item già finalizzato o token stale) NON è un errore — è l'esito atteso
// quando un worker stale prova a finalizzare un item ri-claimato altrove.
func (d *workItemDataSQL) MarkDone(ctx context.Context, id, token string) *core.ApplicationError {
	now := time.Now()
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusDone).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkDone: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// MarkFailed transitions a single IN_PROGRESS item to FAILED, fenced dal token (idempotente).
func (d *workItemDataSQL) MarkFailed(ctx context.Context, id, token, reason string) *core.ApplicationError {
	now := time.Now()
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusFailed).
		Set("error = ?", reason).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkFailed: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// MarkPending resets a single IN_PROGRESS item back to PENDING for retry, fenced dal token (idempotente).
func (d *workItemDataSQL) MarkPending(ctx context.Context, id, token string, after time.Duration) *core.ApplicationError {
	now := time.Now()
	nextRunAt := now.Add(after)
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusPending).
		Set("locked_at = NULL").
		Set("update_time = ?", now).
		Set("retry = retry + 1").
		Set("next_run_at = ?", nextRunAt).
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkPending: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

func (d *workItemDataSQL) Insert(ctx context.Context, items []*store.WorkItem) *core.ApplicationError {
	return coresql.InsertMany[store.WorkItem](ctx, d.DB, items)
}

func (d *workItemDataSQL) DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError) {
	res, err := d.DB.NewDelete().TableExpr(store.TableWorkItems).
		Where("id = ? AND status = ?", id, store.StatusPending).
		Exec(ctx)
	if err != nil {
		return false, core.TechnicalErrorWithError(err)
	}
	affected, _ := res.RowsAffected()
	return affected == 1, nil
}

func (d *workItemDataSQL) GetById(ctx context.Context, id string) (*store.WorkItem, *core.ApplicationError) {
	return coresql.GetById[store.WorkItem](ctx, d.DB, id)
}

func (d *workItemDataSQL) HasActive(ctx context.Context, workType, objectId string) (bool, *core.ApplicationError) {
	var count int
	if err := d.DB.NewSelect().TableExpr(store.TableWorkItems).
		ColumnExpr("COUNT(*)").
		Where("type = ? AND object_id = ? AND status IN (?, ?)",
			workType, objectId, store.StatusPending, store.StatusInProgress).
		Scan(ctx, &count); err != nil {
		return false, core.TechnicalErrorWithError(err)
	}
	return count > 0, nil
}

// InsertIfNotActive inserts each item only if no active (PENDING or IN_PROGRESS) entry
// exists for the same (type, object_id). Relies on the partial unique index
// uk_workitem_active — call EnsureIndexes at startup to create it.
func (d *workItemDataSQL) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	d.warnIfActiveIndexMissing(ctx)
	res, err := d.DB.NewInsert().
		Model(&items).
		On("CONFLICT DO NOTHING").
		Exec(ctx)
	if err != nil {
		return 0, core.TechnicalErrorWithError(err)
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (d *workItemDataSQL) List(ctx context.Context, workType, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	q := d.DB.NewSelect().TableExpr(store.TableWorkItems).Where("type = ?", workType)
	if status != "" {
		q = q.Where("status = ?", status)
	}

	var total int64
	if err := q.ColumnExpr("COUNT(*)").Scan(ctx, &total); err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	paging.SetTotalItems(total)

	offset, appErr := paging.Paging()
	if appErr != nil {
		return nil, appErr
	}

	orderExpr := "create_time DESC"
	if expr := strings.TrimPrefix(coresql.SortToSQL(sort), "ORDER BY "); expr != "" {
		orderExpr = expr
	}

	q = d.DB.NewSelect().TableExpr(store.TableWorkItems).Where("type = ?", workType).
		OrderExpr(orderExpr)
	if status != "" {
		q = q.Where("status = ?", status)
	}
	if offset >= 0 {
		q = q.Offset(offset).Limit(paging.PageSize)
	}

	var items []*store.WorkItem
	if err := q.Scan(ctx, &items); err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	return items, nil
}

// EnsureIndexes creates the indexes and columns required by workItemDataSQL. Call once at
// application startup. Include:
//   - le colonne di fencing lock_token/locked_by (ADD COLUMN IF NOT EXISTS) usate da ClaimPending/
//     RecoverOrphans/Mark* per impedire che un worker stale finalizzi un item ri-claimato;
//   - l'indice partiale unico uk_workitem_active, che impedisce l'inserimento concorrente di
//     item attivi (PENDING o IN_PROGRESS) duplicati per lo stesso (type, object_id).
//
// È Postgres-specifico (come il resto delle utility DDL del modulo). Su MySQL/SQLite le colonne
// e l'indice vanno creati manualmente via migration.
func EnsureIndexes(ctx context.Context, db *bun.DB) error {
	if _, err := db.ExecContext(ctx, `
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS lock_token TEXT;
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS locked_by  TEXT;
	`); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS uk_workitem_active
		ON work_items (type, object_id)
		WHERE status IN ('PENDING', 'IN_PROGRESS')
	`)
	return err
}
