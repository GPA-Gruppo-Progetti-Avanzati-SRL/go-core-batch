package worker

import (
	"context"
	"errors"
)

// DispatchTask implements distributedjob.ITaskDispatcher for single-instance deployments.
// Tasks are pushed directly into the local worker channel without gRPC.
func (w *Workers[T]) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	ch := w.GetChannel(taskType)
	if ch == nil {
		return errors.New("no worker for task type: " + taskType)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := GenerateTask(taskId, jobId, taskType, objectId, ctx, cancel)
	select {
	case ch <- &t:
		return nil
	default:
		cancel()
		return errors.New("worker channel full: " + taskType)
	}
}
