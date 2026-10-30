// Package sqlbackend è il motore dello store SQL di go-core-batch: le operazioni scritte una volta
// sola per ogni database relazionale, e l'interfaccia Dialect con cui ciascun database porta le
// proprie.
//
// I dialetti sono package separati (pgbackend, oraclebackend) e i package pubblici che l'app
// importa (store/sqlstore, store/oraclestore) ne nominano uno: un'app Postgres non compila il
// codice Oracle e viceversa. Questo package non ne conosce nessuno.
package sqlbackend

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Dialect è ciò che dello store cambia da un database all'altro. Il resto — i Mark*, Release,
// HasActive, Backlog, List — è scritto una volta sola con identificatori quotati, che PostgreSQL e
// Oracle leggono allo stesso modo.
//
// Le operazioni qui dentro non sono la stessa query con una sintassi diversa: su Oracle 19c non
// esistono `UPDATE … RETURNING` multi-riga, `ON CONFLICT`, gli indici parziali né l'INSERT
// multi-riga, quindi il claim è una transazione in tre passi e la deduplica è un INSERT per riga.
// Per questo l'astrazione è per operazione e non per frammento di SQL.
type Dialect interface {
	// Now è l'espressione dell'istante corrente del database: ogni istante del lease la usa.
	Now() string
	// AfterMs è "now + ? millisecondi", con un solo segnaposto.
	AfterMs() string
	// FitText adatta un testo libero (il motivo di MarkFailed) a ciò che il dialetto accetta
	// come letterale.
	FitText(s string) string

	Claim(ctx context.Context, taskName string, limit int, token, host string) ([]*store.WorkItem, error)
	RecoverOrphans(ctx context.Context, taskName string, maxAge time.Duration, limit int, token, host string) ([]*store.WorkItem, error)
	Insert(ctx context.Context, items []*store.WorkItem) *core.Error
	InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, error)
	Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, error)

	InsertTaskLogs(ctx context.Context, logs []*store.TaskLog) *core.Error
	PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, error)

	// IndexNames ritorna i nomi (minuscoli) degli indici che esistono su work_items.
	IndexNames(ctx context.Context) ([]string, error)
	EnsureIndexes(ctx context.Context) error
}

// NewDialect costruisce il Dialect sul *bun.DB dell'app, o fallisce se il database non è il suo.
type NewDialect func(db *bun.DB) (Dialect, *core.Error)

// CheckDialect è il controllo che ogni NewDialect fa per primo: un dialetto diverso da quello
// atteso è un errore di avvio, che dice quale package andava importato. Prima lo store partiva
// comunque e falliva al primo claim, su una sintassi che quel database non ha.
func CheckDialect(db *bun.DB, want dialect.Name, pkg string) *core.Error {
	if got := db.Dialect().Name(); got != want {
		return errs.Tech(errs.CodeDialect).WithMessage(fmt.Sprintf(
			"go-core-batch %s: il database è %s, questo store è per %s "+
				"(store/sqlstore = PostgreSQL, store/oraclestore = Oracle 19c+)", pkg, got, want))
	}
	return nil
}

// ExecDDL esegue un blocco DDL un'istruzione per volta: un blocco multi-statement lo accettano solo
// alcuni driver (il protocollo semplice di PostgreSQL sì, Oracle mai), e se una delle istruzioni
// fallisce l'errore non dice quale. ignore, se non nil, dice quali errori valgono come "già
// fatto" — è l'idempotenza dove IF NOT EXISTS non esiste.
func ExecDDL(ctx context.Context, db *bun.DB, ddl string, ignore func(error) bool) error {
	for _, stmt := range DDLStatements(ddl) {
		// db.DB: il DDL non ha argomenti, e il formatter di bun leggerebbe come segnaposto un `?`
		// che comparisse in un letterale.
		_, err := db.DB.ExecContext(ctx, stmt)
		if err != nil && ignore != nil && ignore(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("EnsureIndexes: %s: %w", strings.Join(strings.Fields(stmt), " "), err)
		}
	}
	return nil
}

// DDLStatements divide un blocco DDL nelle sue istruzioni (separate da `;`), scartando quelle vuote.
func DDLStatements(ddl string) []string {
	var out []string
	for _, s := range strings.Split(ddl, ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// EnsureIndexes è il corpo delle EnsureIndexes pubbliche: costruisce il dialetto, quindi
// verifica che il database sia quello giusto, e gli delega il DDL.
func EnsureIndexes(ctx context.Context, db *bun.DB, newDialect NewDialect) error {
	d, appErr := newDialect(db)
	if appErr != nil {
		return appErr
	}
	return d.EnsureIndexes(ctx)
}
