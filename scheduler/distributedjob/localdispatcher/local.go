// Package localdispatcher provides an in-process ITaskDispatcher for distributedjob.
// Use it in single-instance deployments where tasks run in the same process as the scheduler,
// without the gRPC infrastructure required by grpcdispatcher.
package localdispatcher

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
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
	core.In
	Items   store.IWorkItemStore
	Data    store.IData
	Runners []*runner.TaskRunner `group:"batch_runners"`
}

func newMuxRunner(p muxParams) *runner.MuxRunner {
	return runner.NewMux(p.Runners)
}

// Module registers the DistribuiteTask mux dispatcher with the fx application.
// It provides ITaskDispatcher via fx so that queryfeed and s3feed modules can depend
// on it. La JobRegistration del tipo DistribuiteTask confluisce nel value group batch_jobs
// (scheduler.ProvideJob), quindi l'ordine rispetto allo scheduler è indifferente.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.Provide(newMuxRunner, modes...)
	core.ProvideAs[distributedjob.ITaskDispatcher](New, modes...)
	scheduler.ProvideJob(distributedjob.Register, modes...)
}
