package worker

import (
	"context"
	"errors"
)

// DispatchTask implements distributedjob.ITaskDispatcher for single-instance deployments.
// Tasks are pushed directly into the local worker channel without gRPC.
func (w *Workers[T]) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskName string) error {
	ch := w.GetChannel(taskName)
	if ch == nil {
		return errors.New("no worker for task type: " + taskName)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := GenerateTask(taskId, jobId, taskName, objectId, ctx, cancel)
	select {
	case ch <- &t:
		return nil
	default:
		cancel()
		return errors.New("worker channel full: " + taskName)
	}
}
