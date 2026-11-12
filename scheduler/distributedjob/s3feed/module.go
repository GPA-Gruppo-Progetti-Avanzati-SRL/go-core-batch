// Package s3feed porta i flussi su file S3 del batch. Chi alimenta e chi esegue sono separati:
//
//   - ALIMENTAZIONE: il job FeedS3Task controlla a cron un prefisso del bucket e accoda un work item
//     per ogni file che corrisponde al pattern; lo stesso giro l'app lo esegue da API col Feeder.
//     I file già in coda o in lavorazione non si riaccodano, e l'esito dice se c'era qualcosa da
//     fare (s3.NothingToDo, s3.Queued, s3.InProgress);
//   - ESECUZIONE: il task che riceve i work item è un runner.RegisterFile, eseguito da un SingleTask
//     (un file per tick, in sequenza) o da un DistribuiteTask che gira ogni tot. Il runner riceve
//     chiave e contenuto del file; a scaricarlo prima e a spostarlo in `dest-path` dopo un esito
//     riuscito è la libreria.
//
// Resta disponibile DistribuiteTaskByS3File, che fa feed ed esecuzione nello stesso job a ogni tick.
//
// È un package a parte, e non un componente che batch.Module wira da sé, perché porta l'SDK AWS:
// un'app che non lo importa non se lo trascina nel go.mod. L'implementazione sta in
// internal/s3feed; qui c'è soltanto la registrazione.
//
//	batch.Module(&svc.Batch, Register, …, batch.WithS3(s3feed.Module))
//	func Register() { runner.RegisterFile[cflowRunner]("CFLOW") }
//
//	tasks:
//	  - { name: cflow, type: CFLOW }
//	jobs:
//	  - name: feed-cflow             # alimentazione: a cron, o disabled: true se solo da API
//	    type: FeedS3Task
//	    cron: "0 0 22 * * *"
//	    properties: { task: cflow, service: s3, path: "cflow/todo/", pattern: "*", dest-path: "cflow/done", limit: 100 }
//	  - name: cflow-exec             # esecuzione: un file per tick
//	    type: SingleTask
//	    cron: "*/30 * * * * *"
//	    singleton: true
//	    lock-timeout: 3h
//	    properties: { task: cflow }
package s3feed

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3feed"

// Feeder esegue su richiesta il feed di un job FeedS3Task, con le property di quella voce di
// `jobs:`: è ciò che un'API chiama per far partire un flusso su file. Si inietta come ogni altra
// dipendenza; richiede batch.WithS3(s3feed.Module).
//
//	res, err := feeder.Feed(ctx, "feed-cflow")
//	if err != nil { return err }
//	return res.Outcome() // s3.NothingToDo | s3.Queued | s3.InProgress
type Feeder = s3feed.Feeder

// Module registra i flussi su file S3: il job FeedS3Task, il Feeder, il feed del job
// DistribuiteTaskByS3File e l'avvolgimento dei runner.RegisterFile. Si passa a batch.WithS3, che lo
// registra senza mode gate (è tutto lazy) e gli fornisce la sezione `s3:`.
//
// Passato a batch.WithModule, la forma precedente, funziona ancora sugli scheduler modes, ma il
// Feeder resta privato al sottosistema e l'app non lo può iniettare.
func Module(modes ...string) { s3feed.Module(modes...) }
