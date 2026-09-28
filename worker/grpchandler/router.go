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

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc/proto"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpctransport"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/rs/zerolog/log"
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

	_, ok := r.taskServices.GetTaskExecutions(s.TaskName)
	if !ok {
		return nil, errors.New("invalid task type: " + s.TaskName)
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := worker.GenerateTask(s.TaskId, s.JobId, s.TaskName, s.ObjectId, ctx, cancel)

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
	return &proto.TaskStatus{Status: "OK", Hostname: core.GetHostname()}, nil
}
