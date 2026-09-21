package grpchandler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"
	"go.uber.org/fx"
)

// Provide registers a TaskRunner constructor on the worker side,
// identical to runner.Provide — both sides share the same fx group.
func Provide(constructor any) {
	runner.Provide(constructor)
}

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
	w := worker.NewWorkers[*runnerService](p.Lifecycle, p.WorkersCfg, p.Data, svc, p.Items)
	NewRouter[*runnerService](w, p.GrpcServer, svc)
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

// runnerService bridges []*runner.TaskRunner to worker.ITaskService[*runnerService].
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

func (s *runnerService) GetServices() *runnerService { return s }

func (s *runnerService) GetTaskExecutions(taskName string) (worker.RunTask[*runnerService], bool) {
	tr, ok := s.routes[taskName]
	if !ok {
		return nil, false
	}
	// Solo adattamento: carica il WorkItem ed esegue il runner. La finalizzazione del
	// lifecycle (store.ApplyResult) è centralizzata in worker.Run, che riceve questo errore.
	return func(t *worker.Task, _ *runnerService, items store.IWorkItemStore) error {
		item, appErr := items.GetById(t.Context, t.ObjectId)
		if appErr != nil {
			return appErr
		}
		// Passa a worker.Run l'item INTERO: è ciò che store.ApplyResult riceve per finalizzare
		// (fencing token, contatore dei tentativi). Il tetto ai ritentativi viaggia a parte,
		// perché è configurazione dell'istanza di task e non un dato dell'item.
		t.Item = item
		t.MaxRetry = tr.MaxRetry
		return tr.Runner.Run(t.Context, item)
	}, true
}
