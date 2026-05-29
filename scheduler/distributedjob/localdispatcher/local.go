// Package localdispatcher provides an in-process ITaskDispatcher for distributedjob.
// Use it in single-instance deployments where tasks run in the same process as the scheduler,
// without the gRPC infrastructure required by grpcdispatcher.
package localdispatcher

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
)

// ITaskRunner is the interface the application implements.
// Run is called once per dispatched objectId; return non-nil to mark the item as failed.
type ITaskRunner interface {
	Run(ctx context.Context, objectId, taskType string) error
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

	if err := d.runner.Run(ctx, objectId, taskType); err != nil {
		d.data.SetTaskInError(ctx, taskId, jobId, taskType, objectId, err.Error())
		if markErr := d.items.MarkFailed(ctx, objectId, err.Error()); markErr != nil {
			log.Error().Err(markErr).Msgf("[%s] MarkFailed failed for %s", jobId, objectId)
		}
		return err
	}

	d.data.SetTaskDone(ctx, taskId, jobId, taskType, objectId)
	if markErr := d.items.MarkDone(ctx, []string{objectId}); markErr != nil {
		log.Error().Err(markErr).Msgf("[%s] MarkDone failed for %s", jobId, objectId)
		return markErr
	}
	return nil
}
