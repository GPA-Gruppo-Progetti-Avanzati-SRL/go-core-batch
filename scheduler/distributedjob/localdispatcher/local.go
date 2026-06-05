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
func (d *LocalDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	go func() {
		d.data.SetTaskStart(ctx, taskId, jobId, taskType, objectId)
		if err := d.mux.Run(ctx, objectId, taskType, d.items); err != nil {
			d.data.SetTaskInError(ctx, taskId, jobId, taskType, objectId, err.Error())
			return
		}
		d.data.SetTaskDone(ctx, taskId, jobId, taskType, objectId)
	}()
	return nil
}

type muxParams struct {
	fx.In
	Items   store.IWorkItemStore
	Data    store.IData
	Runners []*runner.TaskRunner `group:"batch_runners"`
}

// Module registers the DistribuiteTask mux dispatcher with the fx application.
// Call once (e.g. in an init()) before scheduler.NewScheduler.
func Module() {
	core.Invoke(func(p muxParams) {
		mux := runner.NewMux(p.Runners)
		distributedjob.Register(New(mux, p.Items, p.Data), p.Items, p.Data)
	})
}
