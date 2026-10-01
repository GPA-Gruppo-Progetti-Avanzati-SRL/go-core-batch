// Package grpcdispatcher provides a gRPC-based ITaskDispatcher for distributed deployments.
// Import this package only when the scheduler dispatches tasks to remote worker processes.
// Un deployment a processo singolo usa localdispatcher, che esegue in-process senza gRPC.
package grpcdispatcher

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
)

// GrpcDispatcher implements distributedjob.ITaskDispatcher by forwarding tasks via gRPC.
type GrpcDispatcher struct {
	client *grpctransport.Client
}

func NewGrpcDispatcher(client *grpctransport.Client) *GrpcDispatcher {
	return &GrpcDispatcher{client: client}
}

var _ distributedjob.ITaskDispatcher = (*GrpcDispatcher)(nil)

// DispatchTask inoltra il task al worker remoto. Sul filo viaggiano l'id dell'item, il suo
// fencing token e il timeout: il bridge lato worker ricarica il WorkItem dall'id, ma finalizza col
// token del dispatch (se nel frattempo l'item è stato ri-claimato, non esegue nulla) e interrompe
// l'esecuzione oltre il timeout — l'orphan timeout del job, lo stesso deadline del percorso
// in-process. Prima né l'uno né l'altro attraversavano il filo: un task lungo veniva ri-dispatchato
// mentre ancora girava, e il worker stale finalizzava col token riletto, cioè quello del nuovo claim.
func (d *GrpcDispatcher) DispatchTask(ctx context.Context, req distributedjob.DispatchRequest) error {
	_, err := d.client.DistribuiteTask(ctx, req.JobId, req.TaskId, req.Item.Id, req.TaskName, req.Item.LockToken, req.Timeout)
	return err
}

// Module fornisce il client gRPC e il GrpcDispatcher come ITaskDispatcher dei DistribuiteTask*.
// Lo chiama batch.Module quando `grpc.client.url` è valorizzato, e gli supplisce il
// *grpc.ClientConfig; le JobRegistration dei job type le registra batch.
func Module(modes ...string) {
	core.Provide(grpctransport.NewClient, modes...)
	core.ProvideAs[distributedjob.ITaskDispatcher](NewGrpcDispatcher, modes...)
}
