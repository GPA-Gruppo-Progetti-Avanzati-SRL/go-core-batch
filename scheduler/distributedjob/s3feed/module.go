// Package s3feed è il backend del job DistribuiteTaskByS3File: a ogni tick elenca i file di un
// bucket S3 che corrispondono a un pattern, ne accoda uno per work item e li consegna ai runner
// registrati con runner.RegisterFile, che ricevono chiave e contenuto del file.
//
// È un package a parte, e non un componente che batch.Module wira da sé, perché porta l'SDK AWS:
// un'app che non lo importa non se lo trascina nel go.mod. L'implementazione sta in
// internal/s3feed; qui c'è soltanto la registrazione.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(s3feed.Module))
//	func Register() { runner.RegisterFile[myS3Runner]("S3_IMPORT") }
package s3feed

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3feed"

// Module registra il job DistribuiteTaskByS3File. Si passa a batch.WithModule, che lo gate-a sui
// scheduler modes e gli fornisce la s3.Config (sezione `s3:`).
func Module(modes ...string) { s3feed.Module(modes...) }
