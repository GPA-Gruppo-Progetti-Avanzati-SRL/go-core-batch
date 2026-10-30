// Package sqlstore è il query store SQL (bun) del job DistribuiteTaskByQuery: a ogni tick esegue la
// query configurata nel job e ne accoda i risultati come work item. È un package a parte perché
// porta il driver del database, e un'app che non lo importa non se lo trascina nel go.mod;
// l'implementazione sta in internal/querystore/sqlstore.
//
// Vale per PostgreSQL (e per gli altri dialetti di bun in cui un identificatore quotato non cambia
// significato); per Oracle c'è scheduler/distributedjob/oraclestore.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(sqlstore.Module))
package sqlstore

import (
	"github.com/uptrace/bun/dialect"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/querystore/sqlstore"
)

// Module fornisce il query store dei job DistribuiteTaskByQuery, con tabella e colonne di sort
// quotate. Si passa a batch.WithModule; richiede via fx un *bun.DB (coresql.Module). Su Oracle
// ferma l'avvio con BATCH-SQL-DIALECT: lì un nome quotato non troverebbe le tabelle legacy.
func Module(modes ...string) {
	sqlstore.Module(sqlstore.Quoted, func(n dialect.Name) bool { return n != dialect.Oracle },
		"scheduler/distributedjob/sqlstore", modes...)
}
