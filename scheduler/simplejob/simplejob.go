// Package simplejob provides an in-process job type for go-core-batch.
// It finds pending WorkItems and executes them locally via a registered ITaskRunner,
// without the need for Kafka or gRPC infrastructure.
//
// Usage — register before NewScheduler:
//
//	simplejob.Register("HelloWorld", workItemStore, myRunner)
//
// Config example:
//
//	scheduler:
//	  - name: "hello-world"
//	    type: "HelloWorld"
//	    cron: "*/5 * * * * *"
//	    properties:
//	      workType: "HelloWorld"
package simplejob

import (
	"context"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

// ITaskRunner is the interface that the application implements.
// Run is called once per pending WorkItem; return non-nil to mark the item as failed.
type ITaskRunner interface {
	Run(ctx context.Context, item *store.WorkItem) error
}

// Register adds a job type with the given name to the global scheduler registry.
// The job finds all pending WorkItems of that type and calls runner.Run for each.
// Items that succeed are marked DONE; items that fail are marked FAILED.
func Register(jobType string, items store.IWorkItemStore, runner ITaskRunner) {
	scheduler.Jobs[jobType] = makeFactory(items, runner)
}

func makeFactory(items store.IWorkItemStore, runner ITaskRunner) scheduler.JobFactory {
	return func(name string, _ *scheduler.Services, config scheduler.Config) gocron.Task {
		workType := config.Properties["workType"]
		if workType == "" {
			workType = name
		}
		return gocron.NewTask(func() error {
			return run(name, workType, items, runner)
		})
	}
}

func run(name, workType string, items store.IWorkItemStore, runner ITaskRunner) error {
	jobID := fmt.Sprintf("%s-%s", name, time.Now().Format("20060102150405"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pending, err := items.FindPending(ctx, workType, "", "")
	if err != nil {
		log.Error().Err(err).Msgf("[%s] FindPending failed", jobID)
		return err
	}
	if len(pending) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobID)
		return nil
	}

	log.Info().Msgf("[%s] processing %d item(s)", jobID, len(pending))

	var doneIDs []string
	for _, item := range pending {
		if runErr := runner.Run(ctx, item); runErr != nil {
			log.Error().Err(runErr).Msgf("[%s] task failed for item %s", jobID, item.Id)
			if markErr := items.MarkFailed(ctx, item.Id, runErr.Error()); markErr != nil {
				log.Error().Err(markErr).Msgf("[%s] MarkFailed failed for item %s", jobID, item.Id)
			}
			continue
		}
		doneIDs = append(doneIDs, item.Id)
	}

	if len(doneIDs) > 0 {
		if markErr := items.MarkDone(ctx, doneIDs); markErr != nil {
			log.Error().Err(markErr).Msgf("[%s] MarkDone failed", jobID)
			return markErr
		}
	}

	log.Info().Msgf("[%s] done=%d failed=%d", jobID, len(doneIDs), len(pending)-len(doneIDs))
	return nil
}
