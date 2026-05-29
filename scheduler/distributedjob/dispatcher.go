package distributedjob

import "context"

// ITaskDispatcher routes a task to its executor.
// Local mode: *worker.Workers[T] implements this directly (channel push, same process).
// Distributed mode: GrpcDispatcher implements this (gRPC client, separate process).
type ITaskDispatcher interface {
	DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error
}
