// Package errs raccoglie ambit e codici degli errori di go-core-batch. È interno perché i
// codici sono un contratto verso chi *legge* l'errore (log, risposta HTTP), non verso chi
// scrive codice: le app non li costruiscono, li riconoscono — e i valori stanno in ERRORI.md.
package errs

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Ambit è la libreria di origine. I costruttori di core riempiono Ambit con l'AppName, cioè
// con l'app che l'errore lo riceve: senza sovrascriverlo un guasto dello store batch si
// presenta come un errore dell'applicazione.
const Ambit = "go-core-batch"

// Codici degli errori emessi dal modulo. I BATCH-MARK-* sono separati perché è l'unica cosa
// che distingue "non sono riuscito a prendere il lavoro" da "l'ho fatto e non riesco a
// scriverne l'esito": il secondo caso lascia il WorkItem IN_PROGRESS fino al RecoverOrphans.
const (
	CodeClaim       = "BATCH-CLAIM"        // ClaimPending fallita
	CodeRecover     = "BATCH-RECOVER"      // RecoverOrphans fallita
	CodeMarkDone    = "BATCH-MARK-DONE"    // MarkDone fallita
	CodeMarkFailed  = "BATCH-MARK-FAILED"  // MarkFailed fallita
	CodeMarkPending = "BATCH-MARK-PENDING" // MarkPending (retry) fallita
	CodeDelete      = "BATCH-DELETE"       // DeleteIfPending fallita
	CodeGet         = "BATCH-GET"          // GetById fallita
	CodeHasActive   = "BATCH-HASACTIVE"    // HasActive fallita
	CodeInsert      = "BATCH-INSERT"       // InsertIfNotActive fallita
	CodeList        = "BATCH-LIST"         // List fallita

	CodeQuery      = "BATCH-QUERY"       // query di feed fallita
	CodeQueryCur   = "BATCH-QUERY-CUR"   // lettura del cursore del feed fallita
	CodeQueryIdent = "BATCH-QUERY-IDENT" // identificatore SQL non valido nella query di feed

	CodeKafkaProducer  = "BATCH-KAFKA-PRODUCER"  // creazione del producer fallita
	CodeKafkaTx        = "BATCH-KAFKA-TX"        // begin/commit della transazione fallita
	CodeKafkaSerialize = "BATCH-KAFKA-SERIALIZE" // serializzazione di chiave o messaggio fallita
	CodeKafkaProduce   = "BATCH-KAFKA-PRODUCE"   // produce fallita

	CodeJobProperties = "BATCH-JOB-PROPS" // property infrastrutturale mancante in jobs[].properties
)

// Tech è il costruttore usato da tutto il modulo: errore tecnico con codice e libreria.
func Tech(code string) *core.ApplicationError {
	return core.TechnicalError().WithAmbit(Ambit).WithCode(code)
}

// NotFound è il 404 del modulo (codice NOT-FOUND di core), con la libreria di origine.
func NotFound() *core.ApplicationError {
	return core.NotFoundError().WithAmbit(Ambit)
}
