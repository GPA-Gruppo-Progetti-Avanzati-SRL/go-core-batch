// Package sqlstore provides bun/SQL-backed implementations of store.IData and store.IWorkItemStore.
// Import this package only in applications that use a SQL database.
package sqlstore

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/rs/zerolog/log"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// batchDataSQL implements store.IData using a SQL database via bun. Le cinque Set* per riga
// singola arrivano da store.TaskLogWriter — sono identiche per ogni backend, cambia solo la
// scrittura — mentre la scrittura in blocco e la retention sono di qui.
type batchDataSQL struct {
	store.TaskLogWriter
	Sql *coresql.Service
}

// batchDataSQLParams: il livello è OPZIONALE perché il wiring manuale (senza batch.Module) non
// lo fornisce, e lì deve valere il default storico invece di far fallire l'avvio.
type batchDataSQLParams struct {
	core.In
	Sql   *coresql.Service
	Level store.TaskLogLevel `optional:"true"`
}

func newBatchDataSQL(p batchDataSQLParams) *batchDataSQL {
	d := &batchDataSQL{Sql: p.Sql}
	d.TaskLogWriter = store.TaskLogWriter{Level: p.Level, Insert: d.insertTask}
	return d
}

var _ store.IData = (*batchDataSQL)(nil)

func (d *batchDataSQL) insertTask(ctx context.Context, tl *store.TaskLog) {
	if err := d.Sql.InsertOne(ctx, tl); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire task log: %s", err.Message)
	}
}

// InsertTaskLogs scrive le righe in una sola INSERT multi-valore.
func (d *batchDataSQL) InsertTaskLogs(ctx context.Context, logs []*store.TaskLog) {
	logs = d.Level.Filter(logs)
	if len(logs) == 0 {
		return
	}
	if err := d.Sql.InsertMany[store.TaskLog](ctx, logs); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire %d task log: %s", len(logs), err.Message)
	}
}

// PurgeTaskLogs cancella le righe più vecchie di olderThan, al più limit per chiamata.
func (d *batchDataSQL) PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, *core.Error) {
	res, err := d.Sql.DB().NewRaw(`
		DELETE FROM task_logs
		WHERE ctid IN (
			SELECT ctid FROM task_logs WHERE logdate < ? ORDER BY logdate ASC LIMIT ?
		)
	`, olderThan, limit).Exec(ctx)
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}
