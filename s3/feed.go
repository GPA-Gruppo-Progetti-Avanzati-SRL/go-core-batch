package s3

// FeedResult è l'esito di un giro del feed S3 — il tick di un job FeedS3Task o una chiamata di
// Feeder.Feed dall'API, che fanno la stessa cosa.
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
