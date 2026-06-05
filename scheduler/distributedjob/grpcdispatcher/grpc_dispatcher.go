// Package grpcdispatcher provides a gRPC-based ITaskDispatcher for distributed deployments.
// Import this package only when the scheduler dispatches tasks to remote worker processes.
// Single-instance deployments use *worker.Workers[T] directly via worker.DispatchTask.
package grpcdispatcher

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	batchgrpc "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"go.uber.org/fx"
)

// GrpcDispatcher implements distributedjob.ITaskDispatcher by forwarding tasks via gRPC.
type GrpcDispatcher struct {
	client *batchgrpc.Client
}

func NewGrpcDispatcher(client *batchgrpc.Client) *GrpcDispatcher {
	return &GrpcDispatcher{client: client}
}

var _ distributedjob.ITaskDispatcher = (*GrpcDispatcher)(nil)

func (d *GrpcDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	_, err := d.client.DistribuiteTask(ctx, jobId, taskId, objectId, taskType)
	return err
}

// GrpcModule wires the gRPC client and GrpcDispatcher, then registers the job.
// The app must separately provide store.IWorkItemStore and store.IData.
// Deprecated: use Module() for consistency with localdispatcher.
func GrpcModule() fx.Option {
	return fx.Options(
		fx.Provide(batchgrpc.NewClient),
		fx.Provide(NewGrpcDispatcher),
		fx.Invoke(func(d *GrpcDispatcher, items store.IWorkItemStore, data store.IData) {
			distributedjob.Register(d, items, data)
		}),
	)
}

// Module is the idiomatic alternative to GrpcModule: it wires the gRPC client and
// GrpcDispatcher via core.Provides/core.Invoke, consistent with localdispatcher.Module.
// Call once (e.g. in an init()) before scheduler.NewScheduler.
func Module() {
	core.Provides(batchgrpc.NewClient, NewGrpcDispatcher)
	core.Invoke(func(d *GrpcDispatcher, items store.IWorkItemStore, data store.IData) {
		distributedjob.Register(d, items, data)
	})
}
