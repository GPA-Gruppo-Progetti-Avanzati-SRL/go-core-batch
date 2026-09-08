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
	const job, taskName = "noop-job", "noop-task"

	// Sul ramo a vuoto l'asserzione è sul CONTEGGIO DELLE SERIE, non su un valore: la serie non
	// deve nemmeno essere creata, e leggerne il valore la creerebbe a zero rendendo
	// indistinguibile "non incrementata" da "mai toccata".
	series := seriesCount(JobItemsClaimed)
	JobClaimed(job, taskName, 0)
	JobClaimed(job, taskName, -1)
	if n := seriesCount(JobItemsClaimed); n != series {
		t.Fatalf("batch_job_items_claimed_total: %d serie prima, %d dopo due JobClaimed a vuoto", series, n)
	}

	claimed := counterDelta(t, JobItemsClaimed.WithLabelValues(job, taskName))
	JobClaimed(job, taskName, 3)
	if got := claimed(); got != 3 {
		t.Fatalf("batch_job_items_claimed_total += %v, atteso 3", got)
	}
}

// TestObserveTaskAndJobProcessed verifica che le due facce dell'esito — quella di task e quella
// di job — siano emesse in coppia e con la stessa classificazione.
func TestObserveTaskAndJobProcessed(t *testing.T) {
	const job, taskName = "paired-job", "paired-task"

	done := counterDelta(t, TaskOutcome.WithLabelValues(taskName, OutcomeDone))
	retry := counterDelta(t, TaskOutcome.WithLabelValues(taskName, OutcomeRetry))
	failed := counterDelta(t, TaskOutcome.WithLabelValues(taskName, OutcomeFailed))
	success := counterDelta(t, JobItemsProcessed.WithLabelValues(job, taskName, StatusSuccess))
	errored := counterDelta(t, JobItemsProcessed.WithLabelValues(job, taskName, StatusError))

	start := time.Now()
	for _, o := range []store.Outcome{store.OutcomeDone, store.OutcomeRetry} {
		ObserveTask(taskName, o, start)
		JobProcessed(job, taskName, o, start)
	}

	if got := done(); got != 1 {
		t.Errorf("outcome done += %v, atteso 1", got)
	}
	if got := retry(); got != 1 {
		t.Errorf("outcome retry += %v, atteso 1", got)
	}
	// Un retry NON deve finire fra i failed: è la distinzione che la vecchia task_error non
	// poteva esprimere.
	if got := failed(); got != 0 {
		t.Errorf("outcome failed += %v, atteso 0 (un retry non è un fallimento)", got)
	}
	if got := success(); got != 1 {
		t.Errorf("processed success += %v, atteso 1", got)
	}
	if got := errored(); got != 1 {
		t.Errorf("processed error += %v, atteso 1", got)
	}
}

func TestTaskAssigned(t *testing.T) {
	const taskName = "assigned-task"

	ok := counterDelta(t, TaskAssignedTotal.WithLabelValues(taskName, StatusSuccess))
	ko := counterDelta(t, TaskAssignedTotal.WithLabelValues(taskName, StatusError))

	TaskAssigned(taskName, nil)
	TaskAssigned(taskName, errors.New("dispatch rifiutato"))

	if got := ok(); got != 1 {
		t.Errorf("assigned success += %v, atteso 1", got)
	}
	if got := ko(); got != 1 {
		t.Errorf("assigned error += %v, atteso 1", got)
	}
}
