package sqlstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"

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

// WorkItemDataSQL implements store.IWorkItemStore using a SQL database via bun.
type WorkItemDataSQL struct {
	DB *bun.DB
}

func NewWorkItemDataSQL(db *bun.DB) *WorkItemDataSQL {
	return &WorkItemDataSQL{DB: db}
}

var _ store.IWorkItemStore = (*WorkItemDataSQL)(nil)

func (d *WorkItemDataSQL) FindPending(ctx context.Context, workType, destination, objectType string) ([]*store.WorkItem, *core.ApplicationError) {
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
func (d *WorkItemDataSQL) ClaimPending(ctx context.Context, workType, destination, objectType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	now := time.Now()
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
		}
		_, err := tx.NewUpdate().TableExpr(store.TableWorkItems).
			Set("status = ?", store.StatusInProgress).
			Set("locked_at = ?", now).
			Set("update_time = ?", now).
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
func (d *WorkItemDataSQL) RecoverOrphans(ctx context.Context, workType, destination, objectType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()

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
	args = append(args, limit, now, now)

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
		SET locked_at = ?, retry = retry + 1, update_time = ?
		WHERE id IN (SELECT id FROM recovered)
		RETURNING *
	`, args...).Scan(ctx, &items)
	if err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	return items, nil
}

// MarkDone transitions the given IN_PROGRESS items to DONE.
// Returns an error if any id is not found in IN_PROGRESS state.
func (d *WorkItemDataSQL) MarkDone(ctx context.Context, ids []string) *core.ApplicationError {
	now := time.Now()
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusDone).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id IN (?) AND status = ?", bun.List(ids), store.StatusInProgress).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if affected, _ := res.RowsAffected(); int(affected) != len(ids) {
		return core.TechnicalErrorWithCodeAndMessage("WIS-MD-001",
			fmt.Sprintf("MarkDone: expected %d IN_PROGRESS items, modified %d", len(ids), affected))
	}
	return nil
}

// MarkFailed transitions the given IN_PROGRESS item to FAILED.
// Returns an error if the item is not found in IN_PROGRESS state.
func (d *WorkItemDataSQL) MarkFailed(ctx context.Context, id, reason string) *core.ApplicationError {
	now := time.Now()
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusFailed).
		Set("error = ?", reason).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id = ? AND status = ?", id, store.StatusInProgress).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return core.TechnicalErrorWithCodeAndMessage("WIS-MF-001",
			fmt.Sprintf("MarkFailed: item %q not found in IN_PROGRESS state", id))
	}
	return nil
}

// MarkPending resets the given IN_PROGRESS item back to PENDING for retry.
// Returns an error if the item is not found in IN_PROGRESS state.
func (d *WorkItemDataSQL) MarkPending(ctx context.Context, id string, after time.Duration) *core.ApplicationError {
	now := time.Now()
	nextRunAt := now.Add(after)
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusPending).
		Set("locked_at = NULL").
		Set("update_time = ?", now).
		Set("retry = retry + 1").
		Set("next_run_at = ?", nextRunAt).
		Where("id = ? AND status = ?", id, store.StatusInProgress).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return core.TechnicalErrorWithCodeAndMessage("WIS-MP-001",
			fmt.Sprintf("MarkPending: item %q not found in IN_PROGRESS state", id))
	}
	return nil
}

func (d *WorkItemDataSQL) Insert(ctx context.Context, items []*store.WorkItem) *core.ApplicationError {
	return coresql.InsertMany[store.WorkItem](ctx, d.DB, items)
}

func (d *WorkItemDataSQL) DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError) {
	res, err := d.DB.NewDelete().TableExpr(store.TableWorkItems).
		Where("id = ? AND status = ?", id, store.StatusPending).
		Exec(ctx)
	if err != nil {
		return false, core.TechnicalErrorWithError(err)
	}
	affected, _ := res.RowsAffected()
	return affected == 1, nil
}

func (d *WorkItemDataSQL) GetById(ctx context.Context, id string) (*store.WorkItem, *core.ApplicationError) {
	return nil, core.TechnicalErrorWithCodeAndMessage("NOT-IMPL", "GetById not implemented for SQL store")
}

func (d *WorkItemDataSQL) HasActive(ctx context.Context, workType, objectId string) (bool, *core.ApplicationError) {
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
func (d *WorkItemDataSQL) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
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

func (d *WorkItemDataSQL) List(ctx context.Context, workType, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
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

// EnsureIndexes creates the indexes required by WorkItemDataSQL. Call once at application startup.
// The partial unique index uk_workitem_active prevents concurrent insertion of duplicate
// active (PENDING or IN_PROGRESS) items for the same (type, object_id).
func EnsureIndexes(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS uk_workitem_active
		ON work_items (type, object_id)
		WHERE status IN ('PENDING', 'IN_PROGRESS')
	`)
	return err
}
