package batchmetrics

import (
	"errors"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// TestOutcomeMapping copre la tabella Outcome → label che tutto il resto della libreria assume.
// È il test da aggiornare se si aggiunge un valore a store.Outcome.
func TestOutcomeMapping(t *testing.T) {
	cases := []struct {
		outcome store.Outcome
		name    string
		status  string
	}{
		{store.OutcomeDone, OutcomeDone, StatusSuccess},
		{store.OutcomeHandled, OutcomeHandled, StatusSuccess},
		{store.OutcomeRetry, OutcomeRetry, StatusError},
		{store.OutcomeFailed, OutcomeFailed, StatusError},
	}
	for _, c := range cases {
		if got := OutcomeName(c.outcome); got != c.name {
			t.Errorf("OutcomeName(%v) = %q, atteso %q", c.outcome, got, c.name)
		}
		if got := Status(c.outcome); got != c.status {
			t.Errorf("Status(%v) = %q, atteso %q", c.outcome, got, c.status)
		}
	}
}

// TestJobClaimedNoopOnZero è l'invariante che ha originato il fix: un tick a vuoto non deve
// muovere la metrica di throughput. Il no-op sta in JobClaimed proprio per non dover ripetere
// l'if in ogni famiglia di job.
func TestJobClaimedNoopOnZero(t *testing.T) {
	const job, taskType = "noop-job", "NoopType"

	// Prima/dopo sul conteggio delle serie: il no-op non deve nemmeno creare la serie a zero, e
	// il confronto rende il test indipendente dall'ordine rispetto agli altri (i collector sono
	// globali di package).
	before := seriesCount(JobItemsClaimed)
	JobClaimed(job, taskType, 0)
	JobClaimed(job, taskType, -1)
	if n := seriesCount(JobItemsClaimed); n != before {
		t.Fatalf("batch_job_items_claimed_total: %d serie prima, %d dopo due JobClaimed a vuoto", before, n)
	}

	JobClaimed(job, taskType, 3)
	if got := counterValue(t, JobItemsClaimed.WithLabelValues(job, taskType)); got != 3 {
		t.Fatalf("batch_job_items_claimed_total = %v, atteso 3", got)
	}
}

// TestObserveTaskAndJobProcessedShareWindow verifica che le due istogrammi per item registrino
// lo stesso numero di osservazioni: è ciò che garantisce che ObserveTask e JobProcessed siano
// chiamate in coppia con lo stesso started.
func TestObserveTaskAndJobProcessedShareWindow(t *testing.T) {
	const job, taskType = "paired-job", "PairedType"
	start := time.Now()

	for _, o := range []store.Outcome{store.OutcomeDone, store.OutcomeRetry} {
		ObserveTask(taskType, o, start)
		JobProcessed(job, taskType, o, start)
	}

	if got := counterValue(t, TaskOutcome.WithLabelValues(taskType, OutcomeDone)); got != 1 {
		t.Errorf("outcome done = %v, atteso 1", got)
	}
	if got := counterValue(t, TaskOutcome.WithLabelValues(taskType, OutcomeRetry)); got != 1 {
		t.Errorf("outcome retry = %v, atteso 1", got)
	}
	// Un retry NON deve finire fra i failed: è la distinzione che la vecchia task_error non
	// poteva esprimere.
	if got := counterValue(t, TaskOutcome.WithLabelValues(taskType, OutcomeFailed)); got != 0 {
		t.Errorf("outcome failed = %v, atteso 0 (un retry non è un fallimento)", got)
	}
	if got := counterValue(t, JobItemsProcessed.WithLabelValues(job, taskType, StatusSuccess)); got != 1 {
		t.Errorf("processed success = %v, atteso 1", got)
	}
	if got := counterValue(t, JobItemsProcessed.WithLabelValues(job, taskType, StatusError)); got != 1 {
		t.Errorf("processed error = %v, atteso 1", got)
	}
}

func TestTaskAssigned(t *testing.T) {
	const taskType = "AssignedType"

	TaskAssigned(taskType, nil)
	TaskAssigned(taskType, errors.New("dispatch rifiutato"))

	if got := counterValue(t, TaskAssignedTotal.WithLabelValues(taskType, StatusSuccess)); got != 1 {
		t.Errorf("assigned success = %v, atteso 1", got)
	}
	if got := counterValue(t, TaskAssignedTotal.WithLabelValues(taskType, StatusError)); got != 1 {
		t.Errorf("assigned error = %v, atteso 1", got)
	}
}
