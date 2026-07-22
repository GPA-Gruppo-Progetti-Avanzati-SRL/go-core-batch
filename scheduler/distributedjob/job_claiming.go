package distributedjob

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("DistribuiteTask")

const defaultOrphanTimeout = 10 * time.Minute

func jobID(name string) string {
	return fmt.Sprintf("%s-%s", name, time.Now().Format("20060102150405"))
}

func makeClaimingFactory(dispatcher ITaskDispatcher, items store.IWorkItemStore, feed IFeedSource, data store.IData) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return gocron.NewTask(func() error {
			return jobRunWithClaiming(name, dispatcher, items, feed, data, config)
		})
	}
}

func jobRunWithClaiming(name string, dispatcher ITaskDispatcher, items store.IWorkItemStore, feed IFeedSource, data store.IData, config scheduler.Config) error {
	p := config.Properties
	taskType, ok := p["task"]
	if !ok {
		return fmt.Errorf("missing required property 'task'")
	}
	limit, ok := p["limit"]
	if !ok {
		return fmt.Errorf("missing required property 'limit'")
	}
	ilimit, err := strconv.Atoi(limit)
	if err != nil {
		return fmt.Errorf("property 'limit' is not an integer: %s", limit)
	}

	orphanTimeout := config.LockTimeout
	if orphanTimeout == 0 {
		orphanTimeout = defaultOrphanTimeout
	}

	jobId := jobID(name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	spanCtx, span := tracer.Start(ctx, name)
	span.SetAttributes(
		attribute.String("jobName", name),
		attribute.String("jobId", jobId),
		attribute.String("taskType", taskType),
	)
	defer span.End()

	// 0. Feed phase: populate workitems from external source (only when a feed is set)
	if feed != nil {
		runFeedPhase(spanCtx, feed, items, jobId, taskType, p, ilimit)
	}

	// 1+2. Recupero orfani (IN_PROGRESS scaduti) + claim dei PENDING freschi — loop comune (store.ClaimBatch).
	all, norph, nfresh, appErr := store.ClaimBatch(spanCtx, items, jobId, taskType, "", "", orphanTimeout, ilimit)
	if appErr != nil {
		span.RecordError(appErr)
		span.SetStatus(codes.Error, "claim failed")
		log.Error().Err(appErr).Msgf("[%s] ClaimPending failed", jobId)
		return appErr
	}
	if len(all) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobId)
		return nil
	}

	log.Info().Msgf("[%s] processing %d item(s) (%d orphaned, %d fresh)", jobId, len(all), norph, nfresh)

	// 3. Dispatch each item
	for i, item := range all {
		taskId := fmt.Sprintf("%s-task-%d", jobId, i+1)
		if err := dispatcher.DispatchTask(spanCtx, jobId, taskId, item.Id, taskType); err != nil {
			data.SetTaskAssignationKO(spanCtx, taskId, jobId, taskType, item.Id, err.Error())
			scheduler.TaskAssignedKO.With(prometheus.Labels{"type": taskType}).Inc()
			errMsg := fmt.Sprintf("[%s] dispatch failed for item %s", jobId, item.Id)
			log.Error().Msg(errMsg)
			span.RecordError(err)
			span.SetStatus(codes.Error, errMsg)
		} else {
			scheduler.TaskAssigned.With(prometheus.Labels{"type": taskType}).Inc()
			data.SetTaskAssigned(spanCtx, taskId, jobId, taskType, item.Id)
			log.Info().Msgf("[%s] dispatched item %s", jobId, item.Id)
		}
	}
	return nil
}
