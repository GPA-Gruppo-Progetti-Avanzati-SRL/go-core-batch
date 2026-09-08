package simplejob

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// metricsStore è uno store fake che consegna a ClaimPending gli item preparati e accetta ogni
// transizione di lifecycle: qui interessa cosa finisce nelle metriche, non cosa finisce nel DB.
type metricsStore struct {
	pending []*store.WorkItem
}

func (s *metricsStore) ClaimPending(_ context.Context, _, _, _ string, _ int) ([]*store.WorkItem, *core.ApplicationError) {
	out := s.pending
	s.pending = nil
	return out, nil
}
func (s *metricsStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (s *metricsStore) MarkDone(context.Context, []string, string) *core.ApplicationError { return nil }
func (s *metricsStore) MarkFailed(context.Context, string, string, string) *core.ApplicationError {
	return nil
}
func (s *metricsStore) MarkPending(context.Context, string, string, time.Duration) *core.ApplicationError {
	return nil
}
func (s *metricsStore) GetById(context.Context, string) (*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (s *metricsStore) FindPending(context.Context, string, string, string) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (s *metricsStore) Insert(context.Context, []*store.WorkItem) *core.ApplicationError { return nil }
func (s *metricsStore) InsertIfNotActive(context.Context, []*store.WorkItem) (int, *core.ApplicationError) {
	return 0, nil
}
func (s *metricsStore) HasActive(context.Context, string, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (s *metricsStore) DeleteIfPending(context.Context, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (s *metricsStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}

// scriptedRunner ritorna in sequenza gli errori con cui è stato costruito, uno per item.
type scriptedRunner struct {
	results []error
	calls   int
}

func (r *scriptedRunner) Run(context.Context, *store.WorkItem) error {
	res := r.results[r.calls]
	r.calls++
	return res
}

func items(n int) []*store.WorkItem {
	out := make([]*store.WorkItem, n)
	for i := range n {
		out[i] = &store.WorkItem{Id: string(rune('a' + i)), LockToken: "tok"}
	}
	return out
}

// TestRunEmitsPerItemMetrics è il caso che ha originato il fix: simplejob eseguiva i runner
// senza emettere nulla, quindi "quanti item ha processato questo job" non aveva risposta.
// I quattro esiti sono coperti tutti perché è la distinzione che il vecchio task_done/task_error
// non poteva esprimere (un retry transitorio finiva fra i fallimenti).
func TestRunEmitsPerItemMetrics(t *testing.T) {
	const job, taskName = "metrics-job", "metrics-task"

	// Tutte le asserzioni sono su DELTA: i collector sono globali di processo, quindi con
	// `go test -count=2` il secondo giro li troverebbe già valorizzati dal primo.
	started := counterDelta(t, batchmetrics.TaskStarted.WithLabelValues(taskName))
	outcomes := map[string]func() float64{}
	for _, o := range []string{
		batchmetrics.OutcomeDone, batchmetrics.OutcomeHandled,
		batchmetrics.OutcomeRetry, batchmetrics.OutcomeFailed,
	} {
		outcomes[o] = counterDelta(t, batchmetrics.TaskOutcome.WithLabelValues(taskName, o))
	}
	claimed := counterDelta(t, batchmetrics.JobItemsClaimed.WithLabelValues(job, taskName))
	success := counterDelta(t, batchmetrics.JobItemsProcessed.WithLabelValues(job, taskName, batchmetrics.StatusSuccess))
	errored := counterDelta(t, batchmetrics.JobItemsProcessed.WithLabelValues(job, taskName, batchmetrics.StatusError))

	runner := &scriptedRunner{results: []error{
		nil,                                 // done
		store.ErrHandled,                    // handled
		store.Retry(time.Second),            // retry
		errors.New("fallimento definitivo"), // failed
	}}
	st := &metricsStore{pending: items(4)}

	if err := run(job, taskName, false, time.Minute, time.Minute, 100, st, runner); err != nil {
		t.Fatalf("run ha ritornato errore: %v", err)
	}

	if got := started(); got != 4 {
		t.Errorf("batch_task_started_total += %v, atteso 4", got)
	}
	for outcome, delta := range outcomes {
		if got := delta(); got != 1 {
			t.Errorf("batch_task_outcome_total{outcome=%q} += %v, atteso 1", outcome, got)
		}
	}
	if got := claimed(); got != 4 {
		t.Errorf("batch_job_items_claimed_total += %v, atteso 4", got)
	}
	// done + handled sono successi, retry + failed no.
	if got := success(); got != 2 {
		t.Errorf("processed success += %v, atteso 2", got)
	}
	if got := errored(); got != 2 {
		t.Errorf("processed error += %v, atteso 2", got)
	}
}

// TestRunIdleTickEmitsNothing è l'altra metà del fix: le metriche di job crescevano a ogni tick
// cron anche quando non c'era nulla da fare. batch_job_ticks_total continua a contarli (è la
// liveness, e la emette gocron), ma le metriche di lavoro devono restare ferme.
func TestRunIdleTickEmitsNothing(t *testing.T) {
	const job, taskName = "idle-job", "idle-task"
	st := &metricsStore{} // nessun item pending

	// Il conteggio delle SERIE prima e dopo, e non il valore di una singola serie: un tick a
	// vuoto non deve nemmeno creare la serie a zero, e confrontare prima/dopo rende il test
	// indipendente dagli altri test del package (i collector sono globali).
	claimed := seriesCount(batchmetrics.JobItemsClaimed)
	processed := seriesCount(batchmetrics.JobItemsProcessed)
	started := seriesCount(batchmetrics.TaskStarted)

	if err := run(job, taskName, false, time.Minute, time.Minute, 100, st, &scriptedRunner{}); err != nil {
		t.Fatalf("run ha ritornato errore: %v", err)
	}

	if n := seriesCount(batchmetrics.JobItemsClaimed); n != claimed {
		t.Errorf("batch_job_items_claimed_total: %d serie prima, %d dopo un tick a vuoto", claimed, n)
	}
	if n := seriesCount(batchmetrics.JobItemsProcessed); n != processed {
		t.Errorf("batch_job_items_processed_total: %d serie prima, %d dopo un tick a vuoto", processed, n)
	}
	if n := seriesCount(batchmetrics.TaskStarted); n != started {
		t.Errorf("batch_task_started_total: %d serie prima, %d dopo un tick a vuoto", started, n)
	}
}

// counterDelta, counterValue e seriesCount evitano prometheus/testutil, che trascinerebbe
// github.com/kylelemons/godebug nel go.mod della libreria per soli helper di test.

// counterDelta cattura il valore corrente di una serie e ritorna la funzione che ne dà
// l'INCREMENTO. Le asserzioni sui contatori vanno fatte sui delta e mai sui valori assoluti: i
// collector sono globali di processo, quindi con `go test -count=2` il secondo giro li trova già
// valorizzati dal primo.
func counterDelta(t *testing.T, c prometheus.Counter) func() float64 {
	t.Helper()
	before := counterValue(t, c)
	return func() float64 { return counterValue(t, c) - before }
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("lettura del counter fallita: %v", err)
	}
	return m.GetCounter().GetValue()
}

// seriesCount conta le serie esistenti: l'asserzione "la serie non deve nemmeno essere creata"
// non è esprimibile leggendone il valore, perché WithLabelValues la creerebbe a zero.
func seriesCount(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 1024)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	n := 0
	for range ch {
		n++
	}
	return n
}
