// Package sqlstore è il query store SQL (bun) del job DistribuiteTaskByQuery: a ogni tick esegue la
// query configurata nel job e ne accoda i risultati come work item. È un package a parte perché
// porta il driver del database, e un'app che non lo importa non se lo trascina nel go.mod;
// l'implementazione sta in internal/querystore/sqlstore.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(sqlstore.Module))
package sqlstore

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/querystore/sqlstore"

// Module fornisce il query store dei job DistribuiteTaskByQuery. Si passa a batch.WithModule;
// richiede via fx un *bun.DB (coresql.Module).
func Module(modes ...string) { sqlstore.Module(modes...) }
