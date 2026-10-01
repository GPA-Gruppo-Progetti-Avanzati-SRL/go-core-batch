package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tickTracer = otel.Tracer("BatchClaimingJob")

// ClaimingTick descrive un tick claim-based. I campi sono quelli che le tre famiglie di job
// (distributedjob, kafkajob, simplejob) avevano ciascuna nel proprio preambolo: id
// dell'esecuzione, context con timeout, span, feed opzionale, ClaimBatch, metriche.
//
// Esiste perché quel preambolo era copiato tre volte, e le tre copie erano già divergite senza
// che nessuno l'avesse deciso: due aprivano uno span e una no, due emettevano JobProcessed e una
// no. Ciò che DAVVERO distingue le tre famiglie è la sola fase di elaborazione — dispatch a un
// worker, esecuzione in linea, publish su Kafka — ed è l'unica cosa che resta al chiamante.
type ClaimingTick struct {
	// JobName è il nome del job di config: è la label delle metriche (bassa cardinalità).
	JobName string
	// JobType è il `type` della voce di `jobs:`, usato come attributo dello span.
	JobType string
	// TaskName è la coda su cui il job lavora, ed è il solo filtro del claim.
	TaskName string
	// Limit è il tetto agli item claimati per tick (backpressure).
	Limit int
	// RunTimeout e OrphanTimeout vengono da Config.ResolveTimeouts.
	RunTimeout    time.Duration
	OrphanTimeout time.Duration
	// Backlog abilita le gauge di coda (una query in più per tick).
	Backlog bool
	// Feed, se valorizzata, gira PRIMA del claim e popola nuovi work item. Un suo errore non ferma
	// il tick — gli item già in coda si lavorano lo stesso — ma ne è l'esito: prima era un Warn, e un
	// feed rotto da giorni si presentava come una serie di tick riusciti senza lavoro.
	Feed func(ctx context.Context, jobID string) error
	// Process lavora il batch claimato. È l'unica parte specifica della famiglia di job.
	Process func(ctx context.Context, jobID string, batch []*store.WorkItem) error
}

// Run esegue un tick completo: feed (se c'è) → recupero orfani + claim → elaborazione.
//
// Un errore nel recupero orfani non ferma il tick (è best-effort, lo dice store.ClaimBatch); un
// errore di ClaimPending sì, a meno che degli orfani siano già stati recuperati — in quel caso si
// lavorano quelli, perché sono già IN_PROGRESS e lasciarli lì significherebbe aspettare un altro
// giro di orphan timeout.
func (t ClaimingTick) Run(items store.IWorkItemStore) error {
	jobID := NewJobID(t.JobName)
	ctx, cancel := context.WithTimeout(context.Background(), t.RunTimeout)
	defer cancel()

	spanCtx, span := tickTracer.Start(ctx, t.JobName)
	span.SetAttributes(
		attribute.String("jobName", t.JobName),
		attribute.String("jobType", t.JobType),
		attribute.String("jobId", jobID),
		attribute.String("taskName", t.TaskName),
	)
	defer span.End()

	var feedErr error
	if t.Feed != nil {
		if feedErr = t.Feed(spanCtx, jobID); feedErr != nil {
			span.RecordError(feedErr)
			span.SetStatus(codes.Error, "feed failed")
			log.Error().Err(feedErr).Msgf("[%s] feed fallito, si lavora la coda esistente", jobID)
		}
	}

	batch, orphans, fresh, appErr := store.ClaimBatch(
		spanCtx, items, jobID, t.TaskName, t.OrphanTimeout, t.Limit)
	if appErr != nil && len(batch) == 0 {
		span.RecordError(appErr)
		span.SetStatus(codes.Error, "claim failed")
		log.Error().Err(appErr).Msgf("[%s] ClaimPending failed", jobID)
		return errors.Join(feedErr, appErr)
	}
	if appErr != nil {
		log.Error().Err(appErr).Msgf("[%s] ClaimPending failed, si lavorano i %d orfani già recuperati", jobID, len(batch))
	}

	// La coda si misura DOPO il claim: quel che resta è ciò che questo tick non ha preso, che è
	// esattamente l'arretrato di cui si vuole l'allarme.
	if t.Backlog {
		if pending, oldest, errB := items.Backlog(spanCtx, t.TaskName); errB != nil {
			log.Warn().Err(errB).Msgf("[%s] lettura del backlog fallita", jobID)
		} else {
			batchmetrics.ObserveBacklog(t.JobName, t.TaskName, pending, oldest)
		}
	}

	if len(batch) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobID)
		return feedErr
	}

	batchmetrics.JobClaimed(t.JobName, t.TaskName, len(batch))
	log.Info().Msgf("[%s] processing %d item(s) (%d orphaned, %d fresh)", jobID, len(batch), orphans, fresh)

	if err := t.Process(spanCtx, jobID, batch); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "process failed")
		return errors.Join(feedErr, err)
	}
	return feedErr
}
