package sqlstore

import (
	"context"
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
func (d *WorkItemDataSQL) ClaimPending(ctx context.Context, workType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	now := time.Now()
	var items []*store.WorkItem
	err := d.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := tx.NewRaw(`
			SELECT * FROM workitems
			WHERE type = ? AND status = ?
			ORDER BY create_time ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		`, workType, store.StatusPending, limit).Scan(ctx, &items); err != nil {
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
		_, err := tx.NewUpdate().TableExpr("workitems").
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
func (d *WorkItemDataSQL) RecoverOrphans(ctx context.Context, workType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()
	var items []*store.WorkItem
	err := d.DB.NewRaw(`
		WITH recovered AS (
			SELECT id FROM workitems
			WHERE type = ? AND status = ? AND locked_at < ?
			ORDER BY locked_at ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		UPDATE workitems
		SET locked_at = ?, retry = retry + 1, update_time = ?
		WHERE id IN (SELECT id FROM recovered)
		RETURNING *
	`, workType, store.StatusInProgress, cutoff, limit, now, now).Scan(ctx, &items)
	if err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	return items, nil
}

// MarkDone marks the given items as DONE regardless of current status.
// When claiming is used, items are IN_PROGRESS at this point.
func (d *WorkItemDataSQL) MarkDone(ctx context.Context, ids []string) *core.ApplicationError {
	now := time.Now()
	_, err := d.DB.NewUpdate().TableExpr("workitems").
		Set("status = ?", store.StatusDone).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id IN (?)", bun.List(ids)).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	return nil
}

// MarkFailed marks the given item as FAILED regardless of current status.
func (d *WorkItemDataSQL) MarkFailed(ctx context.Context, id, reason string) *core.ApplicationError {
	now := time.Now()
	_, err := d.DB.NewUpdate().TableExpr("workitems").
		Set("status = ?", store.StatusFailed).
		Set("error = ?", reason).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	return nil
}

func (d *WorkItemDataSQL) Insert(ctx context.Context, items []*store.WorkItem) *core.ApplicationError {
	return coresql.InsertMany[store.WorkItem](ctx, d.DB, items)
}

func (d *WorkItemDataSQL) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	objectIds := make([]string, len(items))
	workType := items[0].Type
	for i, item := range items {
		objectIds[i] = item.ObjectId
	}
	var activeIds []string
	if err := d.DB.NewSelect().TableExpr("workitems").
		ColumnExpr("object_id").
		Where("type = ? AND object_id IN (?) AND status IN (?, ?)",
			workType, bun.List(objectIds), store.StatusPending, store.StatusInProgress).
		Scan(ctx, &activeIds); err != nil {
		return 0, core.TechnicalErrorWithError(err)
	}
	active := make(map[string]struct{}, len(activeIds))
	for _, id := range activeIds {
		active[id] = struct{}{}
	}
	var toInsert []*store.WorkItem
	for _, item := range items {
		if _, exists := active[item.ObjectId]; !exists {
			toInsert = append(toInsert, item)
		}
	}
	if len(toInsert) == 0 {
		return 0, nil
	}
	if appErr := coresql.InsertMany[store.WorkItem](ctx, d.DB, toInsert); appErr != nil {
		return 0, appErr
	}
	return len(toInsert), nil
}
