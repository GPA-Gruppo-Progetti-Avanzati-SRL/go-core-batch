package s3

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Feeder esegue su richiesta il feed S3 di un task: lo stesso giro del tick del suo job FeedS3Task
// — elenco del Source del task, un work item per file, ciò che è già in coda o in lavorazione non si
// riaccoda. È il seam con cui go-core-trigger (o un'API dell'app) fa partire un flusso su file.
//
// Sta in questo package, che non porta dipendenze, perché chi lo inietta non debba importare l'SDK
// AWS: l'implementazione la fornisce scheduler/distributedjob/s3feed, passato a batch.WithS3.
type Feeder interface {
	// Feed esegue un giro del feed del task. Un task senza blocco `s3:` è un errore.
	Feed(ctx context.Context, task string) (FeedResult, *core.Error)
	// Sources ritorna i task con un blocco `s3:`, per nome.
	Sources() map[string]Source
}

// FeedResult è l'esito di un giro del feed S3 — il tick di un job FeedS3Task o una chiamata di
// Feeder.Feed, che fanno la stessa cosa.
type FeedResult struct {
	// Found sono i file del bucket che corrispondono a path/pattern, al più `limit`.
	Found int
	// Queued sono i work item creati: i file già in coda o in lavorazione non si riaccodano.
	Queued int
}

// Outcome è l'esito in una parola, pensato per chi deve decidere cosa fare dopo — lo script che
// chiama l'API e ripete finché non riceve NothingToDo.
type Outcome string

const (
	// NothingToDo: nel bucket non c'è nulla da elaborare. Poiché un file elaborato viene spostato in
	// `dest-path`, dopo un QUEUED significa che il flusso è finito.
	NothingToDo Outcome = "NOTHINGTODO"
	// Queued: almeno un file nuovo è stato accodato.
	Queued Outcome = "QUEUED"
	// InProgress: i file ci sono, ma sono tutti già in coda o in lavorazione.
	InProgress Outcome = "INPROGRESS"
)

// Outcome classifica il risultato.
func (r FeedResult) Outcome() Outcome {
	switch {
	case r.Found == 0:
		return NothingToDo
	case r.Queued > 0:
		return Queued
	default:
		return InProgress
	}
}
