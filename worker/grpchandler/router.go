// Package grpchandler wires a gRPC server to a local worker pool.
// Import this package only in distributed worker deployments — it pulls in google.golang.org/grpc.
// Single-instance deployments use localdispatcher instead (no gRPC, no worker pool).
//
// I runner del processo worker si registrano come ovunque: runner.Register[T](taskType), dentro la
// funzione passata a batch.Module. Non esiste una forma "lato worker" — c'era (grpchandler.Provide,
// alias di runner.Provide) e non aggiungeva nulla: il value group batch_runners è lo stesso per le
// due sponde del filo, ed è precisamente il senso di avere un contratto runner solo.
package grpchandler

import (
	"context"
	"errors"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc/proto"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/utils"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Router embedda UnimplementedDistributionChannelServer (default gRPC forward-compatible): i
// metodi non implementati ritornano codes.Unimplemented, e nuovi metodi nel proto non rompono
// la compilazione. Implementa solo DistribuiteTask; DistribuiteSimpleTask resta non implementato.
type Router struct {
	proto.UnimplementedDistributionChannelServer
	workers      *worker.Workers
	taskServices worker.ITaskService
}

func NewRouter(w *worker.Workers, gs *grpctransport.Server, service worker.ITaskService) *Router {
	r := &Router{workers: w, taskServices: service}
	proto.RegisterDistributionChannelServer(gs, r)
	return r
}

func (r *Router) DistribuiteTask(ctx context.Context, s *proto.TaskMessage) (*proto.TaskStatus, error) {
	log.Info().Msgf("G - %s - %s - Distribuisco Task su Worker %s", s.JobId, s.TaskId, s.TaskName)

	// fx ferma prima i worker (appesi dopo) e poi il server gRPC: nella finestra fra i due il server
	// accetta ancora RPC, ma nessun worker preleva più dal canale. Rifiutare qui fa ricevere un
	// errore allo scheduler, che rilascia l'item (Release: PENDING subito, ritentativo non consumato)
	// invece di lasciarlo IN_PROGRESS fino all'orphan timeout.
	if r.workers.Stopping() {
		return nil, status.Error(codes.Unavailable, "worker pool in arresto")
	}

	_, ok := r.taskServices.GetTaskExecutions(s.TaskName)
	if !ok {
		return nil, errors.New("invalid task type: " + s.TaskName)
	}

	// Il context della RPC finisce con la risposta, l'esecuzione no: se ne stacca la cancellazione e
	// le si dà il deadline del dispatch — l'orphan timeout del job. Oltre quello l'item è già
	// ri-claimabile, e proseguire vorrebbe dire due esecutori dello stesso lavoro.
	base := context.WithoutCancel(ctx)
	var cancel context.CancelFunc
	if s.TimeoutMs > 0 {
		ctx, cancel = context.WithTimeout(base, time.Duration(s.TimeoutMs)*time.Millisecond)
	} else {
		ctx, cancel = context.WithCancel(base)
	}
	t := worker.GenerateTask(s.TaskId, s.JobId, s.TaskName, s.WorkItemId, ctx, cancel)
	t.DispatchToken = s.LockToken

	ch := r.workers.GetChannel(s.TaskName)
	if ch == nil {
		cancel()
		log.Error().Msgf("G - no worker channel for task type: %s", s.TaskName)
		return nil, errors.New("no worker channel for task type: " + s.TaskName)
	}
	select {
	case ch <- &t:
	default:
		cancel()
		log.Error().Msgf("G - %s - %s - Worker channel pieno, task rifiutato: %s", s.JobId, s.TaskId, s.TaskName)
		return nil, errors.New("worker channel full: " + s.TaskName)
	}
	return okStatus()
}

func okStatus() (*proto.TaskStatus, error) {
	return &proto.TaskStatus{Status: "OK", Hostname: utils.GetHostname()}, nil
}
