// Package mongostore è il query store MongoDB del job DistribuiteTaskByQuery: a ogni tick esegue la
// query configurata nel job e ne accoda i risultati come work item. È un package a parte perché
// porta il driver del database, e un'app che non lo importa non se lo trascina nel go.mod;
// l'implementazione sta in internal/querystore/mongostore.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(mongostore.Module))
package mongostore

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/querystore/mongostore"

// Module fornisce il query store dei job DistribuiteTaskByQuery. Si passa a batch.WithModule;
// richiede via fx un *coremongo.Service (coremongo.Module).
func Module(modes ...string) { mongostore.Module(modes...) }
