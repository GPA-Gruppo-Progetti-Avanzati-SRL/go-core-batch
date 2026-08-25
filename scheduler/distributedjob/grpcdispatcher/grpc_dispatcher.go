// Package grpcdispatcher provides a gRPC-based ITaskDispatcher for distributed deployments.
// Import this package only when the scheduler dispatches tasks to remote worker processes.
// Single-instance deployments use *worker.Workers[T] directly via worker.DispatchTask.
package grpcdispatcher

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
)

// GrpcDispatcher implements distributedjob.ITaskDispatcher by forwarding tasks via gRPC.
type GrpcDispatcher struct {
	client *grpctransport.Client
}

func NewGrpcDispatcher(client *grpctransport.Client) *GrpcDispatcher {
	return &GrpcDispatcher{client: client}
}

var _ distributedjob.ITaskDispatcher = (*GrpcDispatcher)(nil)

func (d *GrpcDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskName string) error {
	_, err := d.client.DistribuiteTask(ctx, jobId, taskId, objectId, taskName)
	return err
}

// Module wires the gRPC client and GrpcDispatcher, providing ITaskDispatcher via fx
// so that queryfeed and s3feed modules can depend on it. La *grpc.ClientConfig è passata
// come parametro e fornita a fx dal Module stesso (core.Supply interno): l'app non deve
// più fare core.Supply. Call once (e.g. in an init()) before scheduler.NewScheduler.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
// Module registra il dispatcher gRPC. È modes-only: il *grpc.ClientConfig NON è più un
// parametro ma viene iniettato da fx — lo fornisce batch.Module (core.Supply della Config
// unificata) oppure, nel wiring manuale standalone, l'app con core.Supply(&cfg.Client) prima
// di chiamare Module().
func Module(modes ...string) {
	core.Provide(grpctransport.NewClient, modes...)
	core.ProvideAs[distributedjob.ITaskDispatcher](NewGrpcDispatcher, modes...)
	scheduler.ProvideJob(distributedjob.Register, modes...)
}
