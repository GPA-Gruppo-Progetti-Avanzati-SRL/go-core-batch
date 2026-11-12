// Package s3feed porta i flussi su file S3 del batch. La sorgente sta sul TASK (blocco `s3:` della
// voce di `tasks:`), e chi alimenta e chi esegue sono separati:
//
//   - ALIMENTAZIONE: il feed del task elenca il prefisso e accoda un work item per ogni file che
//     corrisponde al pattern. Lo esegue il job FeedS3Task a cron, oppure su richiesta il s3.Feeder
//     (go-core-trigger, o un'API dell'app): è lo stesso giro. I file già in coda o in lavorazione non
//     si riaccodano, e l'esito dice se c'era qualcosa da fare (s3.NothingToDo, s3.Queued,
//     s3.InProgress);
//   - ESECUZIONE: il task è un runner.RegisterFile, eseguito da un SingleTask (un file per tick, in
//     sequenza) o da un DistribuiteTask che gira ogni tot. Il runner riceve chiave e contenuto del
//     file; a scaricarlo prima e a spostarlo in `dest-path` dopo un esito riuscito è la libreria.
//
// Resta disponibile DistribuiteTaskByS3File, che fa feed ed esecuzione nello stesso job a ogni tick
// con la sorgente sulle property del job.
//
// È un package a parte, e non un componente che batch.Module wira da sé, perché porta l'SDK AWS:
// un'app che non lo importa non se lo trascina nel go.mod. L'implementazione sta in
// internal/s3feed; qui c'è soltanto la registrazione.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithS3(s3feed.Module))
//	func Register() { runner.RegisterFile[importRunner]("IMPORT") }
//
//	tasks:
//	  - name: import
//	    type: IMPORT
//	    s3: { service: s3, path: "inbox/", pattern: "*", dest-path: "archive", limit: 100 }
//	jobs:
//	  - name: feed-import             # alimentazione a cron (facoltativa: su richiesta basta il trigger)
//	    type: FeedS3Task
//	    cron: "0 0 22 * * *"
//	    properties: { task: import }
//	  - name: import-exec             # esecuzione: un file per tick
//	    type: SingleTask
//	    cron: "*/30 * * * * *"
//	    singleton: true
//	    lock-timeout: 3h
//	    properties: { task: import }
package s3feed

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3feed"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
)

// Feeder è il seam del feed su richiesta, dichiarato nel package s3 (senza dipendenze) perché chi lo
// inietta non debba importare l'SDK AWS. Richiede batch.WithS3(s3feed.Module).
//
//	res, err := feeder.Feed(ctx, "import") // il nome del task
//	if err != nil { return err }
//	return res.Outcome()                  // s3.NothingToDo | s3.Queued | s3.InProgress
type Feeder = s3.Feeder

// Module registra i flussi su file S3: il job FeedS3Task, il s3.Feeder, il feed del job
// DistribuiteTaskByS3File e l'avvolgimento dei runner.RegisterFile. Si passa a batch.WithS3, che lo
// registra senza mode gate (è tutto lazy) e gli fornisce le sezioni `s3:` e `tasks:`.
//
// Passato a batch.WithModule, la forma precedente, funziona ancora sugli scheduler modes, ma il
// Feeder resta privato al sottosistema e l'app non lo può iniettare.
func Module(modes ...string) { s3feed.Module(modes...) }
