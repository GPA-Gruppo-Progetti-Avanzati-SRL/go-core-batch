// Package oraclestore è il query store Oracle del job DistribuiteTaskByQuery: il gemello di
// scheduler/distributedjob/sqlstore per Oracle. L'unica differenza è come si scrivono tabella e
// colonne di sort prese dalla config: NON quotate, perché su Oracle un nome quotato è
// case-sensitive e le tabelle di un feed — di uno schema che l'app non ha creato — hanno nomi
// non quotati, cioè maiuscoli. Restano validate da validateIdent, quindi non aprono a injection.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(oraclestore.Module))
package oraclestore

import (
	"github.com/uptrace/bun/dialect"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/querystore/sqlstore"
)

// Module fornisce il query store dei job DistribuiteTaskByQuery su Oracle. Si passa a
// batch.WithModule; richiede via fx un *bun.DB su Oracle, altrimenti ferma l'avvio con
// BATCH-SQL-DIALECT.
func Module(modes ...string) {
	sqlstore.Module(sqlstore.Unquoted, func(n dialect.Name) bool { return n == dialect.Oracle },
		"scheduler/distributedjob/oraclestore", modes...)
}
