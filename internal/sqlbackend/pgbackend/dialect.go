// Package pgbackend è il Dialect PostgreSQL dello store SQL: lo importa store/sqlstore, e solo lui.
package pgbackend

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// pgDialect è lo store su PostgreSQL: ogni operazione è un'istruzione sola, quindi atomica senza
// transazione, e ritorna ciò che ha scritto (RETURNING) invece di ricostruirlo.
type pgDialect struct {
	db *bun.DB
}

// New è il sqlbackend.NewDialect di PostgreSQL.
func New(db *bun.DB) (sqlbackend.Dialect, *core.Error) {
	if err := sqlbackend.CheckDialect(db, dialect.PG, "store/sqlstore"); err != nil {
		return nil, err
	}
	return pgDialect{db: db}, nil
}

func (pgDialect) Now() string             { return "NOW()" }
func (pgDialect) AfterMs() string         { return "NOW() + (? * INTERVAL '1 millisecond')" }
func (pgDialect) FitText(s string) string { return s }

// claim usa SELECT FOR UPDATE SKIP LOCKED — safe across multiple replicas.
func (p pgDialect) Claim(ctx context.Context, taskName string, limit int, token, host string) ([]*store.WorkItem, error) {
	var items []*store.WorkItem
	err := p.db.NewRaw(`
		WITH claimed AS (
			SELECT id FROM work_items
			WHERE task_name = ? AND status = ? AND (next_run_at IS NULL OR next_run_at <= NOW())
			ORDER BY next_run_at ASC NULLS FIRST, create_time ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE work_items w
			SET status = ?, locked_at = NOW(), update_time = NOW(), lock_token = ?, locked_by = ?
			FROM claimed WHERE w.id = claimed.id
			RETURNING w.*
		)
		SELECT * FROM updated ORDER BY next_run_at ASC NULLS FIRST, create_time ASC
	`, taskName, store.StatusPending, limit,
		store.StatusInProgress, token, host,
	).Scan(ctx, &items)
	return items, err
}

// recoverOrphans: CTE con FOR UPDATE SKIP LOCKED, safe across replicas.
func (p pgDialect) RecoverOrphans(ctx context.Context, taskName string, maxAge time.Duration, limit int, token, host string) ([]*store.WorkItem, error) {
	var items []*store.WorkItem
	err := p.db.NewRaw(`
		WITH recovered AS (
			SELECT id FROM work_items
			WHERE task_name = ? AND status = ?
			  AND (locked_at IS NULL OR locked_at < NOW() - (? * INTERVAL '1 millisecond'))
			ORDER BY locked_at ASC NULLS FIRST
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		UPDATE work_items
		SET locked_at = NOW(), lock_token = ?, locked_by = ?, retry = retry + 1, update_time = NOW()
		WHERE id IN (SELECT id FROM recovered)
		RETURNING *
	`, taskName, store.StatusInProgress, maxAge.Milliseconds(), limit,
		token, host,
	).Scan(ctx, &items)
	return items, err
}

func (p pgDialect) Insert(ctx context.Context, items []*store.WorkItem) *core.Error {
	if _, err := p.db.NewInsert().Model(&items).Exec(ctx); err != nil {
		return errs.Tech(errs.CodeInsert).WithCause(err)
	}
	return nil
}

// insertIfNotActive si appoggia all'unico parziale uk_workitem_active: il conflitto È la deduplica.
func (p pgDialect) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, error) {
	res, err := p.db.NewInsert().
		Model(&items).
		On("CONFLICT DO NOTHING").
		Exec(ctx)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (p pgDialect) Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, error) {
	res, err := p.db.NewRaw(`
		DELETE FROM work_items
		WHERE id IN (
			SELECT id FROM work_items
			WHERE status = ? AND update_time < ?
			ORDER BY update_time ASC
			LIMIT ?
		)
	`, status, olderThan, limit).Exec(ctx)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// insertTaskLogs scrive le righe in una sola INSERT multi-valore.
func (p pgDialect) InsertTaskLogs(ctx context.Context, logs []*store.TaskLog) *core.Error {
	if _, err := p.db.NewInsert().Model(&logs).Exec(ctx); err != nil {
		return errs.Tech(errs.CodeInsert).WithCause(err)
	}
	return nil
}

// purgeTaskLogs: task_logs non ha chiave primaria, quindi il limit passa da ctid.
func (p pgDialect) PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, error) {
	res, err := p.db.NewRaw(`
		DELETE FROM task_logs
		WHERE ctid IN (
			SELECT ctid FROM task_logs WHERE logdate < ? ORDER BY logdate ASC LIMIT ?
		)
	`, olderThan, limit).Exec(ctx)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (p pgDialect) IndexNames(ctx context.Context) ([]string, error) {
	var presenti []string
	err := p.db.NewRaw(
		"SELECT indexname FROM pg_indexes WHERE tablename = ?", store.TableWorkItems,
	).Scan(ctx, &presenti)
	return presenti, err
}

func (p pgDialect) EnsureIndexes(ctx context.Context) error {
	return sqlbackend.ExecDDL(ctx, p.db, EnsureColumnsDDL+EnsureIndexesDDL, nil)
}

// EnsureColumnsDDL sono le colonne che la libreria aggiunge a una tabella già esistente. Come per
// gli indici è una costante e non un literal in linea, perché ha un secondo lettore: il test che
// verifica che ci sia una ADD COLUMN per ogni colonna scritta dal claim e dai Mark*.
const EnsureColumnsDDL = `
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS lock_token  TEXT;
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS locked_by   TEXT;
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS executed_by TEXT;
	`

// EnsureIndexesDDL è il DDL degli indici, estratto in una costante perché ha due lettori:
// EnsureIndexes che lo esegue e un test che verifica che crei tutti gli store.ExpectedIndexes —
// cioè esattamente quelli che la verifica di avvio pretende di trovare.
const EnsureIndexesDDL = `
		CREATE UNIQUE INDEX IF NOT EXISTS uk_workitem_active
		ON work_items (task_name, object_id)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_claim
		ON work_items (task_name, status, next_run_at, create_time)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_orphan
		ON work_items (task_name, status, locked_at)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_purge
		ON work_items (status, update_time)
		WHERE status IN ('DONE', 'FAILED');

		CREATE INDEX IF NOT EXISTS ix_tasklog_purge
		ON task_logs (logdate);
	`
