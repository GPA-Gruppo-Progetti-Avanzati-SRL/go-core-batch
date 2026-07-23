// Package localdispatcher provides an in-process ITaskDispatcher for distributedjob.
// Use it in single-instance deployments where tasks run in the same process as the scheduler,
// without the gRPC infrastructure required by grpcdispatcher.
package localdispatcher

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

// LocalDispatcher implements distributedjob.ITaskDispatcher by running tasks in-process.
// It logs the full task lifecycle to task_logs via store.IData and updates the source
// record status via store.IWorkItemStore.
type LocalDispatcher struct {
	mux   *runner.MuxRunner
	items store.IWorkItemStore
	data  store.IData
}

var _ distributedjob.ITaskDispatcher = (*LocalDispatcher)(nil)

func New(mux *runner.MuxRunner, items store.IWorkItemStore, data store.IData) *LocalDispatcher {
	return &LocalDispatcher{mux: mux, items: items, data: data}
}

// DispatchTask launches the task in a goroutine and returns immediately.
// Concurrency is naturally bounded by the limit property in the scheduler config
// (ClaimPending returns at most limit items per run).
// context.WithoutCancel detaches the goroutine from the job context, which is
// canceled as soon as the job function returns — before the goroutine completes.
func (d *LocalDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	taskCtx := context.WithoutCancel(ctx)
	go func() {
		d.data.SetTaskStart(taskCtx, taskId, jobId, taskType, objectId)
		if err := d.mux.Run(taskCtx, objectId, taskType, d.items); err != nil {
			d.data.SetTaskInError(taskCtx, taskId, jobId, taskType, objectId, err.Error())
			return
		}
		d.data.SetTaskDone(taskCtx, taskId, jobId, taskType, objectId)
	}()
	return nil
}

type muxParams struct {
	fx.In
	Items   store.IWorkItemStore
	Data    store.IData
	Runners []*runner.TaskRunner `group:"batch_runners"`
}

func newMuxRunner(p muxParams) *runner.MuxRunner {
	return runner.NewMux(p.Runners)
}

func register(d distributedjob.ITaskDispatcher, items store.IWorkItemStore, data store.IData) {
	distributedjob.Register(d, items, data)
}

// Module registers the DistribuiteTask mux dispatcher with the fx application,
// unconditionally. It provides ITaskDispatcher via fx so that queryfeed and s3feed
// modules can depend on it. Call once (e.g. in an init()) before scheduler.NewScheduler.
func Module() {
	core.Provides(
		newMuxRunner,
		fx.Annotate(New, fx.As(new(distributedjob.ITaskDispatcher))),
	)
	core.Invoke(register)
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvideIf(newMuxRunner, modes...)
	core.ProvideIf(fx.Annotate(New, fx.As(new(distributedjob.ITaskDispatcher))), modes...)
	core.InvokeIf(register, modes...)
}
