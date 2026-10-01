// Package mux è l'instradamento in-process dei work item ai runner registrati, per task name.
package mux

import (
	"context"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Runner instrada l'esecuzione in-process al runner registrato per il task name: è ciò che il
// localdispatcher chiama per ogni item. Il percorso gRPC passa invece da worker.Run.
type Runner struct {
	// routes conserva il *TaskRunner e non il solo ITaskRunner: serve anche il tetto ai
	// ritentativi dell'istanza, che Run passa a store.ApplyResult.
	routes map[string]*taskrunner.TaskRunner
}

// New costruisce il Runner dai TaskRunner del gruppo batch_runners.
func New(runners []*taskrunner.TaskRunner) *Runner {
	routes := make(map[string]*taskrunner.TaskRunner, len(runners))
	for _, tr := range runners {
		routes[tr.TaskName] = tr
	}
	return &Runner{routes: routes}
}

// Run è il punto in cui il percorso in-process (localdispatcher) emette le metriche di task:
// è il solo che ha in mano sia il task name sia la store.Outcome, e resta uno solo anche se il
// dispatcher cambia. Il percorso gRPC NON passa di qui (va su worker.Run), quindi non c'è
// doppio conteggio.
//
// Riceve il WorkItem già claimato dal job: prima lo rileggeva con GetById, che su questo
// percorso era una query per item buttata — l'item era già in memoria, completo, dal claim.
func (r *Runner) Run(ctx context.Context, item *store.WorkItem, items store.IWorkItemStore) error {
	taskName := item.TaskName
	// TaskStart prima di risolvere la route, a specchio di worker.Run che fa LogStart prima di
	// risolvere il runner: un task che non parte è comunque un task fallito, e senza questo
	// sarebbe invisibile alle metriche.
	start := batchmetrics.TaskStart(taskName)
	tr, ok := r.routes[taskName]
	if !ok {
		batchmetrics.ObserveTask(taskName, store.OutcomeFailed, start)
		// L'item va finalizzato comunque, altrimenti resta IN_PROGRESS fino al recupero orfani
		// e riprova all'infinito un task che questo processo non sa eseguire.
		err := fmt.Errorf("no runner registered for task name %q", taskName)
		if markErr := items.MarkFailed(ctx, item.Id, item.LockToken, err.Error()); markErr != nil {
			return markErr
		}
		return err
	}
	runErr := store.CheckExhausted(item, tr.ResolveMaxRetry())
	if runErr == nil {
		runErr = tr.Runner.Run(ctx, item)
	}
	outcome, markErr := store.ApplyResult(ctx, items, item, tr.ResolveMaxRetry(), runErr)
	batchmetrics.ObserveTask(taskName, outcome, start)
	if markErr != nil {
		return markErr
	}
	// Done/Handled → success (SetTaskDone); Retry/Failed → surface the error (SetTaskInError).
	if outcome == store.OutcomeDone || outcome == store.OutcomeHandled {
		return nil
	}
	return runErr
}
