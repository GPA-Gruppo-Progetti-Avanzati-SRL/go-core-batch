package sqlbackend

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/rs/zerolog/log"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Data implements store.IData using a SQL database via bun. Le cinque Set* per riga singola
// arrivano da store.TaskLogWriter — sono identiche per ogni backend, cambia solo la scrittura —
// mentre la scrittura in blocco e la retention sono del Dialect.
type Data struct {
	store.TaskLogWriter
	Sql     *coresql.Service
	dialect Dialect
}

// DataParams: il livello è OPZIONALE perché il wiring manuale (senza batch.Module) non lo
// fornisce, e lì deve valere il default storico invece di far fallire l'avvio.
type DataParams struct {
	core.In
	Sql   *coresql.Service
	Level store.TaskLogLevel `optional:"true"`
}

// NewData fallisce se il database non è quello del dialetto, come NewWorkItemStore.
func NewData(p DataParams, newDialect NewDialect) (*Data, error) {
	dialect, err := newDialect(p.Sql.DB())
	if err != nil {
		return nil, err
	}
	d := &Data{Sql: p.Sql, dialect: dialect}
	d.TaskLogWriter = store.TaskLogWriter{Level: p.Level, Insert: d.insertTask}
	return d, nil
}

var _ store.IData = (*Data)(nil)

func (d *Data) insertTask(ctx context.Context, tl *store.TaskLog) {
	if err := d.dialect.InsertTaskLogs(ctx, []*store.TaskLog{tl}); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire task log: %s", err.Message)
	}
}

// InsertTaskLogs scrive le righe in blocco, nel modo del Dialect.
func (d *Data) InsertTaskLogs(ctx context.Context, logs []*store.TaskLog) {
	logs = d.Level.Filter(logs)
	if len(logs) == 0 {
		return
	}
	if err := d.dialect.InsertTaskLogs(ctx, logs); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire %d task log: %s", len(logs), err.Message)
	}
}

// PurgeTaskLogs cancella le righe più vecchie di olderThan, al più limit per chiamata.
func (d *Data) PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, *core.Error) {
	n, err := d.dialect.PurgeTaskLogs(ctx, olderThan, limit)
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	return n, nil
}
