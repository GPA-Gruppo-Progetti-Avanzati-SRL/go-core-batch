// Package localdispatcher provides an in-process ITaskDispatcher for distributedjob.
// Use it in single-instance deployments where tasks run in the same process as the scheduler,
// without the gRPC infrastructure required by grpcdispatcher.
package localdispatcher

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// ITaskRunner is the interface the application implements.
// The runner is fully responsible for the workitem lifecycle:
//
//	items.MarkDone(ctx, []string{objectId})        — successo
//	items.MarkPending(ctx, objectId, after)        — errore transiente, riprova dopo delay
//	items.MarkFailed(ctx, objectId, reason)        — errore definitivo
//
// Il runner può anche inserire nuovi workitem (items.Insert) nella stessa operazione,
// raggiungendo l'atomicità con una transazione a livello di DB se necessario.
// Restituire un errore non-nil causa il log su task_logs come InError.
type ITaskRunner interface {
	Run(ctx context.Context, objectId, taskType string, items store.IWorkItemStore) error
}

// LocalDispatcher implements distributedjob.ITaskDispatcher by running tasks in-process.
// It logs the full task lifecycle to task_logs via store.IData and updates the source
// record status via store.IWorkItemStore.
type LocalDispatcher struct {
	runner ITaskRunner
	items  store.IWorkItemStore
	data   store.IData
}

var _ distributedjob.ITaskDispatcher = (*LocalDispatcher)(nil)

// New returns a LocalDispatcher. Both items and data are required:
// items updates the source record (MarkDone/MarkFailed on workitems),
// data writes lifecycle events to task_logs (SetTaskStart/Done/InError).
func New(runner ITaskRunner, items store.IWorkItemStore, data store.IData) *LocalDispatcher {
	return &LocalDispatcher{runner: runner, items: items, data: data}
}

func (d *LocalDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	d.data.SetTaskStart(ctx, taskId, jobId, taskType, objectId)
	if err := d.runner.Run(ctx, objectId, taskType, d.items); err != nil {
		d.data.SetTaskInError(ctx, taskId, jobId, taskType, objectId, err.Error())
		return err
	}
	d.data.SetTaskDone(ctx, taskId, jobId, taskType, objectId)
	return nil
}
