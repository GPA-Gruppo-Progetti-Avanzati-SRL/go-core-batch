// Package sqlstore provides a SQL-backed implementation of distributedjob.IQueryStore.
// Import this package when the external feed source is a SQL database.
package sqlstore

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"regexp"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/uptrace/bun"
)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validateIdent verifica che s sia un identificatore SQL semplice (colonna o tabella),
// eventualmente schema-qualified (una sola parte dotted, se allowDot). table/sort provengono
// da config trusted (Properties del job), ma la validazione previene injection nel caso venissero
// templati da input esterno e intercetta refusi (fail-fast con errore chiaro). Il filter NON è
// validabile così (è una WHERE arbitraria) e resta trusted-config-only.
func validateIdent(kind, s string, allowDot bool) *core.ApplicationError {
	parts := []string{s}
	if allowDot {
		parts = strings.Split(s, ".")
		if len(parts) > 2 {
			return errs.Tech(errs.CodeQueryIdent).WithMessage(fmt.Sprintf("%s %q non valido", kind, s))
		}
	}
	for _, p := range parts {
		if !identRe.MatchString(p) {
			return errs.Tech(errs.CodeQueryIdent).WithMessage(fmt.Sprintf("%s %q non valido: atteso un identificatore SQL", kind, s))
		}
	}
	return nil
}

// queryDataSQL implements distributedjob.IQueryStore against a SQL database.
type queryDataSQL struct {
	DB *bun.DB
}

func newQueryDataSQL(db *bun.DB) *queryDataSQL {
	return &queryDataSQL{DB: db}
}

var _ distributedjob.IQueryStore = (*queryDataSQL)(nil)

func (q *queryDataSQL) GetIds(ctx context.Context, table, filter string, limit int) ([]string, *core.ApplicationError) {
	return q.GetIdsSorted(ctx, table, filter, "", limit)
}

func (q *queryDataSQL) GetIdsSorted(ctx context.Context, table, filter, sort string, limit int) ([]string, *core.ApplicationError) {
	if err := validateIdent("table", table, true); err != nil {
		return nil, err
	}
	// bun.Ident quota correttamente l'identificatore (anche schema-qualified / parole riservate).
	query := q.DB.NewSelect().TableExpr("?", bun.Ident(table)).ColumnExpr("id")
	if filter != "" {
		// filter è una clausola WHERE grezza: NON parametrizzabile qui, TRUSTED CONFIG ONLY
		// (deve provenire dalle Properties del job, mai da input esterno). Vedi IQueryStore.
		query = query.Where(filter)
	}
	if sort != "" {
		for part := range strings.SplitSeq(sort, ",") {
			fields := strings.SplitN(strings.TrimSpace(part), ":", 2)
			col := strings.TrimSpace(fields[0])
			if err := validateIdent("sort column", col, false); err != nil {
				return nil, err
			}
			dir := "ASC"
			if len(fields) == 2 && strings.ToLower(strings.TrimSpace(fields[1])) == "desc" {
				dir = "DESC"
			}
			query = query.OrderExpr("? "+dir, bun.Ident(col))
		}
	}
	if limit > 0 {
		query = query.Limit(limit)
	}
	var ids []string
	if err := query.Scan(ctx, &ids); err != nil {
		return nil, errs.Tech(errs.CodeQuery).WithCause(err)
	}
	return ids, nil
}
