// Package grpcdispatcher provides a gRPC-based ITaskDispatcher for distributed deployments.
// Import this package only when the scheduler dispatches tasks to remote worker processes.
// Single-instance deployments use *worker.Workers[T] directly via worker.DispatchTask.
package grpcdispatcher

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	batchgrpc "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"go.uber.org/fx"
)

// GrpcDispatcher implements distributedjob.ITaskDispatcher by forwarding tasks via gRPC.
type GrpcDispatcher struct {
	client *grpctransport.Client
}

func NewGrpcDispatcher(client *grpctransport.Client) *GrpcDispatcher {
	return &GrpcDispatcher{client: client}
}

var _ distributedjob.ITaskDispatcher = (*GrpcDispatcher)(nil)

func (d *GrpcDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	_, err := d.client.DistribuiteTask(ctx, jobId, taskId, objectId, taskType)
	return err
}

func register(d distributedjob.ITaskDispatcher, items store.IWorkItemStore, data store.IData) {
	distributedjob.Register(d, items, data)
}

// Module wires the gRPC client and GrpcDispatcher unconditionally, providing
// ITaskDispatcher via fx so that queryfeed and s3feed modules can depend on it.
// La *grpc.ClientConfig è passata come parametro e fornita a fx dal Module stesso
// (core.Supply interno): l'app non deve più fare core.Supply.
// Call once (e.g. in an init()) before scheduler.NewScheduler.
func Module(cfg *batchgrpc.ClientConfig) {
	core.Supply(cfg)
	core.Provides(
		grpctransport.NewClient,
		fx.Annotate(NewGrpcDispatcher, fx.As(new(distributedjob.ITaskDispatcher))),
	)
	core.Invoke(register)
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(cfg *batchgrpc.ClientConfig, modes ...string) {
	core.SupplyIf(cfg, modes...)
	core.ProvideIf(grpctransport.NewClient, modes...)
	core.ProvideIf(fx.Annotate(NewGrpcDispatcher, fx.As(new(distributedjob.ITaskDispatcher))), modes...)
	core.InvokeIf(register, modes...)
}
