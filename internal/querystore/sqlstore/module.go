package sqlstore

import (
	"fmt"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
)

// Module fornisce l'implementazione SQL (bun) di distributedjob.IQueryStore (feed by query).
// Il costruttore concreto non è esportato: gli entry-point sono i gusci pubblici, che scelgono
// come scrivere gli identificatori (ident) e su quali database possono girare (accept).
// Richiede via fx un *bun.DB (fornito da coresql.Module insieme al *coresql.Service):
// questo query store costruisce solo query bun native, quindi non passa dai CRUD generici.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(ident Ident, accept func(dialect.Name) bool, pkg string, modes ...string) {
	core.ProvideAs[distributedjob.IQueryStore](func(db *bun.DB) (*queryDataSQL, error) {
		if name := db.Dialect().Name(); !accept(name) {
			return nil, errs.Tech(errs.CodeDialect).WithMessage(fmt.Sprintf(
				"go-core-batch %s: il database è %s, che questo query store non supporta "+
					"(scheduler/distributedjob/sqlstore = PostgreSQL e altri, .../oraclestore = Oracle)", pkg, name))
		}
		return &queryDataSQL{DB: db, ident: ident}, nil
	}, modes...)
}
