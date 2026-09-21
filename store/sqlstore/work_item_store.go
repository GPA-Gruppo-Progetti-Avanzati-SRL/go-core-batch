package sqlstore

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"slices"
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
	TaskName    string   `col:"task_name"   op:"="  omitempty:"true"`
	Status      string   `col:"status"      op:"="  omitempty:"true"`
	Destination string   `col:"destination" op:"="  omitempty:"true"`
	ObjectType  string   `col:"object_type" op:"="  omitempty:"true"`
}

func (f workItemFilter) GetFilterTableName(ctx context.Context) string {
	return store.TableWorkItems
}

// workItemDataSQL implements store.IWorkItemStore using a SQL database via bun.
type workItemDataSQL struct {
	Sql         *coresql.Service
	DB          *bun.DB // handle bun del Service, per le query native e il DDL
	idxWarnOnce sync.Once
}

func newWorkItemDataSQL(sqlService *coresql.Service) *workItemDataSQL {
	return &workItemDataSQL{Sql: sqlService, DB: sqlService.DB()}
}

var _ store.IWorkItemStore = (*workItemDataSQL)(nil)

// indiciAttesi sono gli indici su cui gira il sottosistema. Non vengono creati in automatico
// (gestione manuale via EnsureIndexes o migration/ops): il warning rende l'eventuale assenza una
// scelta consapevole, non una svista.
//
//   - uk_workitem_active — unico parziale: senza, InsertIfNotActive (ON CONFLICT DO NOTHING) NON
//     deduplica e nascono work item doppi, con rischio di doppia esecuzione;
//   - ix_workitem_claim / ix_workitem_orphan / ix_workitem_claim_dest — servono le query di
//     ClaimPending e RecoverOrphans, eseguite da ogni job a OGNI tick. Senza, il claim fa una
//     sequential scan: un costo che cresce con lo storico invece che col lavoro da fare.
var indiciAttesi = []string{
	"uk_workitem_active",
	"ix_workitem_claim",
	"ix_workitem_orphan",
	"ix_workitem_claim_dest",
}

// warnIfIndexesMissing logga (una sola volta) gli indici attesi che non esistono.
func (d *workItemDataSQL) warnIfIndexesMissing(ctx context.Context) {
	d.idxWarnOnce.Do(func() {
		var presenti []string
		if err := d.DB.NewRaw(
			"SELECT indexname FROM pg_indexes WHERE tablename = ?", store.TableWorkItems,
		).Scan(ctx, &presenti); err != nil {
			log.Warn().Err(err).Msg("go-core-batch: impossibile verificare gli indici di work_items")
			return
		}
		var mancanti []string
		for _, nome := range indiciAttesi {
			if !slices.Contains(presenti, nome) {
				mancanti = append(mancanti, nome)
			}
		}
		if len(mancanti) == 0 {
			return
		}
		if slices.Contains(mancanti, "uk_workitem_active") {
			log.Warn().Msg("go-core-batch: indice partiale unico 'uk_workitem_active' ASSENTE su work_items — InsertIfNotActive NON deduplica (rischio work item duplicati / doppia esecuzione)")
		}
		log.Warn().Strs("indici", mancanti).
			Msg("go-core-batch: indici ASSENTI su work_items — il claim di ogni tick fa una sequential scan. Crearli via sqlstore.EnsureIndexes o migration, oppure confermare che l'assenza è voluta.")
	})
}

// ClaimPending atomically selects up to limit PENDING items of taskName,
// marks them IN_PROGRESS with locked_at = now, and returns the full records.
// Uses SELECT FOR UPDATE SKIP LOCKED — safe across multiple replicas.
func (d *workItemDataSQL) ClaimPending(ctx context.Context, taskName, destination, objectType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	// La verifica sta anche qui, e non solo su InsertIfNotActive: gli indici del claim servono a
	// OGNI job, compresi quelli claim-only (DistribuiteTask, NotificationKafka) che un feed non
	// ce l'hanno e quindi non passerebbero mai di là. È sync.Once: una sola lettura per processo.
	d.warnIfIndexesMissing(ctx)
	now := time.Now()
	token := store.NewLockToken()
	host := store.Hostname()
	var items []*store.WorkItem
	err := d.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		q := `SELECT * FROM work_items WHERE task_name = ? AND status = ?
			  AND (next_run_at IS NULL OR next_run_at <= NOW())`
		args := []any{taskName, store.StatusPending}
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
		return nil, errs.Tech(errs.CodeClaim).WithCause(err)
	}
	return items, nil
}

// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
// refreshing locked_at to now and incrementing retry. Returns the items for
// immediate processing — no reset to PENDING, no waiting for the next tick.
// Uses a CTE with FOR UPDATE SKIP LOCKED so it is safe across replicas.
func (d *workItemDataSQL) RecoverOrphans(ctx context.Context, taskName, destination, objectType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()

	token := store.NewLockToken()
	host := store.Hostname()

	where := `task_name = ? AND status = ? AND locked_at < ?`
	args := []any{taskName, store.StatusInProgress, cutoff}
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
		return nil, errs.Tech(errs.CodeRecover).WithCause(err)
	}
	return items, nil
}

// MarkDone transitions IN_PROGRESS items to DONE in batch, fenced dal token (gli id devono
// condividere lo stesso lock_token). Idempotente: gli id non matchati (già finalizzati o token
// stale) sono ignorati — è l'esito atteso quando un worker stale prova a finalizzarli.
func (d *workItemDataSQL) MarkDone(ctx context.Context, ids []string, token string) *core.ApplicationError {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now()
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusDone).
		Set("update_time = ?", now).
		Set("locked_at = NULL").
		Where("id IN (?) AND status = ? AND lock_token = ?", bun.List(ids), store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeMarkDone).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); int(affected) != len(ids) {
		log.Debug().Msgf("MarkDone: %d/%d item marcati DONE (gli altri già finalizzati o token stale)",
			affected, len(ids))
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
		return errs.Tech(errs.CodeMarkFailed).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkFailed: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// Release riporta a PENDING un item claimato ma mai eseguito (dispatch fallito), fenced dal
// token e idempotente. È MarkPending meno l'incremento di retry: il tentativo non è avvenuto,
// quindi non va contato. next_run_at = now, così il tick successivo lo riprende subito.
func (d *workItemDataSQL) Release(ctx context.Context, id, token string) *core.ApplicationError {
	now := time.Now()
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusPending).
		Set("locked_at = NULL").
		Set("update_time = ?", now).
		Set("next_run_at = ?", now).
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeRelease).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("Release: item %q non aggiornato (già finalizzato o token stale)", id)
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
		return errs.Tech(errs.CodeMarkPending).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkPending: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

func (d *workItemDataSQL) Insert(ctx context.Context, items []*store.WorkItem) *core.ApplicationError {
	return d.Sql.InsertMany[store.WorkItem](ctx, items)
}

func (d *workItemDataSQL) DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError) {
	res, err := d.DB.NewDelete().TableExpr(store.TableWorkItems).
		Where("id = ? AND status = ?", id, store.StatusPending).
		Exec(ctx)
	if err != nil {
		return false, errs.Tech(errs.CodeDelete).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return affected == 1, nil
}

func (d *workItemDataSQL) GetById(ctx context.Context, id string) (*store.WorkItem, *core.ApplicationError) {
	return d.Sql.GetById[store.WorkItem](ctx, id)
}

func (d *workItemDataSQL) HasActive(ctx context.Context, taskName, objectId string) (bool, *core.ApplicationError) {
	var count int
	if err := d.DB.NewSelect().TableExpr(store.TableWorkItems).
		ColumnExpr("COUNT(*)").
		Where("task_name = ? AND object_id = ? AND status IN (?, ?)",
			taskName, objectId, store.StatusPending, store.StatusInProgress).
		Scan(ctx, &count); err != nil {
		return false, errs.Tech(errs.CodeHasActive).WithCause(err)
	}
	return count > 0, nil
}

// InsertIfNotActive inserts each item only if no active (PENDING or IN_PROGRESS) entry
// exists for the same (task_name, object_id). Relies on the partial unique index
// uk_workitem_active — call EnsureIndexes at startup to create it.
func (d *workItemDataSQL) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	d.warnIfIndexesMissing(ctx)
	res, err := d.DB.NewInsert().
		Model(&items).
		On("CONFLICT DO NOTHING").
		Exec(ctx)
	if err != nil {
		return 0, errs.Tech(errs.CodeInsert).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// Purge cancella gli item nello stato indicato più vecchi di olderThan, al più limit per
// chiamata. Il limit tiene corta la singola transazione: la retention è ripetuta a ogni tick
// del job, non fatta tutta in una volta.
func (d *workItemDataSQL) Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, *core.ApplicationError) {
	res, err := d.DB.NewRaw(`
		DELETE FROM work_items
		WHERE id IN (
			SELECT id FROM work_items
			WHERE status = ? AND update_time < ?
			ORDER BY update_time ASC
			LIMIT ?
		)
	`, status, olderThan, limit).Exec(ctx)
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// Backlog conta i PENDING in attesa e ritorna la data di creazione del più vecchio.
func (d *workItemDataSQL) Backlog(ctx context.Context, taskName, destination, objectType string) (int, time.Time, *core.ApplicationError) {
	where := "task_name = ? AND status = ?"
	args := []any{taskName, store.StatusPending}
	if destination != "" {
		where += " AND destination = ?"
		args = append(args, destination)
	}
	if objectType != "" {
		where += " AND object_type = ?"
		args = append(args, objectType)
	}
	var row struct {
		N      int        `bun:"n"`
		Oldest *time.Time `bun:"oldest"`
	}
	if err := d.DB.NewRaw(
		"SELECT COUNT(*) AS n, MIN(create_time) AS oldest FROM work_items WHERE "+where, args...,
	).Scan(ctx, &row); err != nil {
		return 0, time.Time{}, errs.Tech(errs.CodeBacklog).WithCause(err)
	}
	if row.Oldest == nil {
		return row.N, time.Time{}, nil
	}
	return row.N, *row.Oldest, nil
}

func (d *workItemDataSQL) List(ctx context.Context, taskName, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	q := d.DB.NewSelect().TableExpr(store.TableWorkItems).Where("task_name = ?", taskName)
	if status != "" {
		q = q.Where("status = ?", status)
	}

	var total int64
	if err := q.ColumnExpr("COUNT(*)").Scan(ctx, &total); err != nil {
		return nil, errs.Tech(errs.CodeList).WithCause(err)
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

	q = d.DB.NewSelect().TableExpr(store.TableWorkItems).Where("task_name = ?", taskName).
		OrderExpr(orderExpr)
	if status != "" {
		q = q.Where("status = ?", status)
	}
	if offset >= 0 {
		q = q.Offset(offset).Limit(paging.PageSize)
	}

	var items []*store.WorkItem
	if err := q.Scan(ctx, &items); err != nil {
		return nil, errs.Tech(errs.CodeList).WithCause(err)
	}
	return items, nil
}

// EnsureIndexes crea colonne e indici richiesti da workItemDataSQL. Chiamarla una volta
// all'avvio. Include:
//   - le colonne di fencing lock_token/locked_by (ADD COLUMN IF NOT EXISTS), usate da
//     ClaimPending/RecoverOrphans/Mark* per impedire che un worker stale finalizzi un item
//     ri-claimato;
//   - uk_workitem_active, unico parziale, che impedisce l'inserimento concorrente di item attivi
//     duplicati per lo stesso (task_name, object_id);
//   - ix_workitem_claim / ix_workitem_orphan / ix_workitem_claim_dest, che servono le query di
//     claim e recupero orfani eseguite da ogni job a ogni tick. Sono parziali sugli stati attivi:
//     gli item DONE/FAILED non vengono mai claimati, quindi tenerli fuori mantiene l'indice della
//     dimensione del LAVORO e non dello storico.
//
// È Postgres-specifico (come il resto delle utility DDL del modulo). Su MySQL/SQLite le colonne
// e gli indici vanno creati manualmente via migration.
func EnsureIndexes(ctx context.Context, db *bun.DB) error {
	if _, err := db.ExecContext(ctx, `
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS lock_token TEXT;
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS locked_by  TEXT;
	`); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS uk_workitem_active
		ON work_items (task_name, object_id)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_claim
		ON work_items (task_name, status, next_run_at, create_time)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_orphan
		ON work_items (task_name, status, locked_at)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_claim_dest
		ON work_items (task_name, status, destination, object_type, next_run_at)
		WHERE status IN ('PENDING', 'IN_PROGRESS');
	`)
	return err
}
