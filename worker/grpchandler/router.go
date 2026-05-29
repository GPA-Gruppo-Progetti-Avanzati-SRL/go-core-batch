// Package grpchandler wires a gRPC server to a local worker pool.
// Import this package only in distributed worker deployments — it pulls in google.golang.org/grpc.
// Single-instance deployments use worker.Workers[T].DispatchTask directly (no gRPC).
package grpchandler

import (
	"context"
	"errors"

	batchgrpc "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc/proto"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/rs/zerolog/log"
)

type Router[T any] struct {
	proto.DistributionChannelServer
	workers      *worker.Workers[T]
	taskServices worker.ITaskService[T]
}

func NewRouter[T any](w *worker.Workers[T], gs *batchgrpc.Server, service worker.ITaskService[T]) *Router[T] {
	r := &Router[T]{workers: w, taskServices: service}
	proto.RegisterDistributionChannelServer(gs, r)
	return r
}

func (r *Router[T]) DistribuiteSimpleTask(ctx context.Context, s *proto.SampleTaskMessage) (*proto.TaskStatus, error) {
	log.Info().Msgf("G - %s - %s - Distribuisco Task su Worker", s.JobId, s.TaskId)
	return okStatus()
}

func (r *Router[T]) DistribuiteTask(ctx context.Context, s *proto.TaskMessage) (*proto.TaskStatus, error) {
	log.Info().Msgf("G - %s - %s - Distribuisco Task su Worker %s", s.JobId, s.TaskId, s.TaskType)

	_, ok := r.taskServices.GetTaskExecutions(s.TaskType)
	if !ok {
		return nil, errors.New("invalid task type: " + s.TaskType)
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := worker.GenerateTask(s.TaskId, s.JobId, s.TaskType, s.ObjectId, ctx, cancel)

	ch := r.workers.GetChannel(s.TaskType)
	if ch == nil {
		cancel()
		log.Error().Msgf("G - no worker channel for task type: %s", s.TaskType)
		return nil, errors.New("no worker channel for task type: " + s.TaskType)
	}
	select {
	case ch <- &t:
	default:
		cancel()
		log.Error().Msgf("G - %s - %s - Worker channel pieno, task rifiutato: %s", s.JobId, s.TaskId, s.TaskType)
		return nil, errors.New("worker channel full: " + s.TaskType)
	}
	return okStatus()
}

func okStatus() (*proto.TaskStatus, error) {
	return &proto.TaskStatus{Status: "OK", Hostname: core.GetHostname()}, nil
}
