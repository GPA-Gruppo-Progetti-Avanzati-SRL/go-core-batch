package distributedjob

import (
	"context"
	"fmt"
	"strconv"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/google/uuid"

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

func makeClaimingFactory(dispatcher ITaskDispatcher, items store.IWorkItemStore, qs IQueryStore, data store.IData) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return gocron.NewTask(jobRunWithClaiming, name, dispatcher, items, qs, data, config)
	}
}

func jobRunWithClaiming(name string, dispatcher ITaskDispatcher, items store.IWorkItemStore, qs IQueryStore, data store.IData, config scheduler.Config) error {
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

	// 0. Feed phase: populate workitems from external source (only when IQueryStore is set)
	if qs != nil {
		collection := p["collection"]
		filter := p["filter"]
		sort := p["sort"]
		var ids []string
		var feedErr *core.ApplicationError
		if sort != "" {
			ids, feedErr = qs.GetIdsSorted(spanCtx, collection, filter, sort, ilimit)
		} else {
			ids, feedErr = qs.GetIds(spanCtx, collection, filter, ilimit)
		}
		if feedErr != nil {
			log.Warn().Err(feedErr).Msgf("[%s] feed query failed", jobId)
		} else if len(ids) > 0 {
			now := time.Now()
			workItems := make([]*store.WorkItem, len(ids))
			for i, id := range ids {
				workItems[i] = &store.WorkItem{
					Id:         uuid.New().String(),
					Type:       taskType,
					ObjectId:   id,
					ObjectType: p["objectType"],
					Status:     store.StatusPending,
					CreateTime: now,
				}
			}
			if n, feedErr := items.InsertIfNotActive(spanCtx, workItems); feedErr != nil {
				log.Warn().Err(feedErr).Msgf("[%s] feed insert failed", jobId)
			} else if n > 0 {
				log.Info().Msgf("[%s] fed %d new workitem(s) from external source", jobId, n)
			}
		}
	}

	// 1. Re-claim orphans from previous crashed runs (refresh locked_at, keep IN_PROGRESS)
	orphans, appErr := items.RecoverOrphans(spanCtx, taskType, orphanTimeout, ilimit)
	if appErr != nil {
		log.Warn().Err(appErr).Msgf("[%s] orphan recovery failed", jobId)
		orphans = nil
	} else if len(orphans) > 0 {
		log.Info().Msgf("[%s] re-claimed %d orphaned task(s) (timeout=%s)", jobId, len(orphans), orphanTimeout)
	}

	// 2. Claim fresh PENDING items up to the remaining capacity
	remaining := ilimit - len(orphans)
	var fresh []*store.WorkItem
	if remaining > 0 {
		fresh, appErr = items.ClaimPending(spanCtx, taskType, remaining)
		if appErr != nil {
			span.RecordError(appErr)
			span.SetStatus(codes.Error, "claim failed")
			log.Error().Msgf("[%s] ClaimPending failed", jobId)
			return appErr
		}
	}

	all := append(orphans, fresh...)
	if len(all) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobId)
		return nil
	}

	log.Info().Msgf("[%s] processing %d item(s) (%d orphaned, %d fresh)", jobId, len(all), len(orphans), len(fresh))

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
