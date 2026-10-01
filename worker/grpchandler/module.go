package grpchandler

import (
	"fmt"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"
	"go.uber.org/fx"
)

type moduleParams struct {
	core.In
	Lifecycle  fx.Lifecycle
	WorkersCfg []worker.Config
	GrpcServer *grpctransport.Server
	Items      store.IWorkItemStore
	Data       store.IData
	Runners    []*runner.TaskRunner `group:"batch_runners"`
}

func wire(p moduleParams) {
	svc := newRunnerService(p.Runners)
	w := worker.NewWorkers(p.Lifecycle, p.WorkersCfg, p.Data, svc, p.Items)
	NewRouter(w, p.GrpcServer, svc)
}

// Module provvede il gRPC Server e wire il worker pool usando i TaskRunner registrati.
// Call once (e.g. in an init()) in the worker process. È modes-only: la *grpc.ServerConfig e
// la []worker.Config (pool size per task type) sono iniettate da fx — le fornisce batch.Module
// (core.Supply della Config unificata) oppure, nel wiring manuale, l'app con
// core.Supply(&cfg.Server) + core.Supply(workersCfg) prima di Module().
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.Provide(grpctransport.NewServer, modes...)
	core.Invoke(wire, modes...)
}

// runnerService bridges []*runner.TaskRunner to worker.ITaskService.
type runnerService struct {
	// routes conserva il *TaskRunner e non il solo ITaskRunner: porta anche il tetto ai
	// ritentativi dell'istanza, che il bridge copia sul worker.Task perché worker.Run —
	// l'unico punto di finalizzazione del pool — possa passarlo a store.ApplyResult.
	routes map[string]*runner.TaskRunner
}

func newRunnerService(runners []*runner.TaskRunner) *runnerService {
	routes := make(map[string]*runner.TaskRunner, len(runners))
	for _, tr := range runners {
		routes[tr.TaskName] = tr
	}
	return &runnerService{routes: routes}
}

func (s *runnerService) GetTaskExecutions(taskName string) (worker.RunTask, bool) {
	tr, ok := s.routes[taskName]
	if !ok {
		return nil, false
	}
	// Solo adattamento: carica il WorkItem ed esegue il runner. La finalizzazione del
	// lifecycle (store.ApplyResult) è centralizzata in worker.Run, che riceve questo errore.
	return func(t *worker.Task, items store.IWorkItemStore) error {
		item, appErr := items.GetById(t.Context, t.WorkItemId)
		if appErr != nil {
			return appErr
		}
		if t.DispatchToken != "" && item.LockToken != t.DispatchToken {
			// Ri-claimato dopo il dispatch (orphan timeout scaduto mentre il task era in coda): il
			// lavoro è di un altro claim. Eseguirlo lo raddoppierebbe, e finalizzarlo col token riletto
			// chiuderebbe l'esecuzione altrui.
			return fmt.Errorf("%w: item %s ri-claimato dopo il dispatch", store.ErrHandled, item.Id)
		}
		// Passa a worker.Run l'item INTERO: è ciò che store.ApplyResult riceve per finalizzare
		// (fencing token, contatore dei tentativi). Il tetto ai ritentativi viaggia a parte,
		// perché è configurazione dell'istanza di task e non un dato dell'item.
		t.Item = item
		t.MaxRetry = tr.MaxRetry
		if err := store.CheckExhausted(item, t.ResolveMaxRetry()); err != nil {
			return err
		}
		return tr.Runner.Run(t.Context, item)
	}, true
}
