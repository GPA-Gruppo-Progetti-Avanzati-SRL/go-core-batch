package grpchandler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	batchgrpc "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
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
	fx.In
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

// Module provvede il gRPC Server e wire il worker pool usando i TaskRunner registrati,
// incondizionatamente. Call once (e.g. in an init()) in the worker process.
// La *grpc.ServerConfig e la []worker.Config (pool size per task type) sono passate come
// parametri e fornite a fx dal Module stesso (core.Supply interno): l'app non deve più
// fare core.Supply.
func Module(grpcCfg *batchgrpc.ServerConfig, workerCfg []worker.Config) {
	core.Supply(grpcCfg)
	core.Supply(workerCfg)
	core.Provides(grpctransport.NewServer)
	core.Invoke(wire)
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(grpcCfg *batchgrpc.ServerConfig, workerCfg []worker.Config, modes ...string) {
	core.SupplyIf(grpcCfg, modes...)
	core.SupplyIf(workerCfg, modes...)
	core.ProvideIf(grpctransport.NewServer, modes...)
	core.InvokeIf(wire, modes...)
}

// runnerService bridges []*runner.TaskRunner to worker.ITaskService[*runnerService].
type runnerService struct {
	routes map[string]runner.ITaskRunner
}

func newRunnerService(runners []*runner.TaskRunner) *runnerService {
	routes := make(map[string]runner.ITaskRunner, len(runners))
	for _, tr := range runners {
		routes[tr.TaskType] = tr.Runner
	}
	return &runnerService{routes: routes}
}

func (s *runnerService) GetServices() *runnerService { return s }

func (s *runnerService) GetTaskExecutions(taskType string) (worker.RunTask[*runnerService], bool) {
	r, ok := s.routes[taskType]
	if !ok {
		return nil, false
	}
	return func(t *worker.Task, _ *runnerService, items store.IWorkItemStore) *core.ApplicationError {
		item, appErr := items.GetById(t.Context, t.ObjectId)
		if appErr != nil {
			return appErr
		}
		runErr := r.Run(t.Context, item)
		outcome, markErr := store.ApplyResult(t.Context, items, t.ObjectId, runErr)
		if markErr != nil {
			return core.TechnicalErrorWithError(markErr)
		}
		// Done/Handled → success; Retry/Failed → surface the error for task_logs.
		if outcome == store.OutcomeDone || outcome == store.OutcomeHandled {
			return nil
		}
		return core.TechnicalErrorWithError(runErr)
	}, true
}
