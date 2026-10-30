// Package sqlstore provides a SQL-backed implementation of distributedjob.IQueryStore.
// Import this package when the external feed source is a SQL database.
package sqlstore

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/uptrace/bun"
)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validateIdent verifica che s sia un identificatore SQL semplice (colonna o tabella),
// eventualmente schema-qualified (una sola parte dotted, se allowDot). table/sort provengono
// da config trusted (Properties del job), ma la validazione previene injection nel caso venissero
// templati da input esterno e intercetta refusi (fail-fast con errore chiaro). Il filter NON è
// validabile così (è una WHERE arbitraria) e resta trusted-config-only.
func validateIdent(kind, s string, allowDot bool) *core.Error {
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

// Ident scrive un identificatore (tabella o colonna di sort) nella query. È la sola cosa che
// cambia fra i database, e la sceglie il guscio pubblico: Quoted per PostgreSQL & co., Unquoted
// per Oracle — così il guscio Postgres non porta il caso Oracle, e viceversa.
type Ident func(name string) any

// Quoted quota l'identificatore col dialetto (anche schema-qualified / parole riservate).
func Quoted(name string) any { return bun.Ident(name) }

// Unquoted lo scrive così com'è. Serve dove un nome quotato è case-sensitive e le tabelle hanno
// nomi non quotati (Oracle: `my_table` quotato cercherebbe "my_table" e non troverebbe MY_TABLE).
// Non apre a un'injection: validateIdent ha già ridotto il nome a un identificatore semplice.
func Unquoted(name string) any { return bun.Safe(name) }

// queryDataSQL implements distributedjob.IQueryStore against a SQL database.
type queryDataSQL struct {
	DB    *bun.DB
	ident Ident
}

var _ distributedjob.IQueryStore = (*queryDataSQL)(nil)

func (q *queryDataSQL) GetIds(ctx context.Context, table, filter, sort string, limit int) ([]string, *core.Error) {
	if err := validateIdent("table", table, true); err != nil {
		return nil, err
	}
	query := q.DB.NewSelect().TableExpr("?", q.ident(table)).ColumnExpr("id")
	if filter != "" {
		// filter è una clausola WHERE grezza: NON parametrizzabile qui, TRUSTED CONFIG ONLY
		// (deve provenire dalle Properties del job, mai da input esterno). Vedi IQueryStore.
		query = query.Where(filter)
	}
	for _, c := range distributedjob.ParseSort(sort) {
		if err := validateIdent("sort column", c.Column, false); err != nil {
			return nil, err
		}
		dir := "ASC"
		if c.Desc {
			dir = "DESC"
		}
		query = query.OrderExpr("? "+dir, q.ident(c.Column))
	}
	if limit > 0 {
		query = query.Limit(int64(limit))
	}
	var ids []string
	if err := query.Scan(ctx, &ids); err != nil {
		return nil, errs.Tech(errs.CodeQuery).WithCause(err)
	}
	return ids, nil
}
