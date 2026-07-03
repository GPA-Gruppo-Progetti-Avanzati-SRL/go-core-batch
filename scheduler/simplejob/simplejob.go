// Package simplejob provides an in-process job type for go-core-batch.
// It finds pending WorkItems and executes them locally via a registered ITaskRunner,
// without the need for Kafka or gRPC infrastructure.
//
// Usage — register via generic helper in batch.go init():
//
//	simplejob.Module()
//	simplejob.RegisterRunner[myRunner]("HelloWorld")
//
// Config example (workType defaults to the job type, so it can be omitted when they match):
//
//	scheduler:
//	  - name: "hello-world"
//	    type: "HelloWorld"
//	    cron: "*/5 * * * * *"
package simplejob

import (
	"context"
	"fmt"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	gocron "github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Group is the fx group tag used to collect all registered SimpleTaskRunners.
const Group = "batch_simple_runners"

// ITaskRunner is the single runner contract, shared with distributedjob via
// store.ITaskRunner — a runner is interchangeable between the two families.
// The framework applies the lifecycle from the return value (see store.ApplyResult):
// nil→MarkDone, store.Retry→MarkPending, err→MarkFailed, store.ErrHandled→left untouched.
type ITaskRunner = store.ITaskRunner

// SimpleTaskRunner binds an ITaskRunner to the job type it handles.
type SimpleTaskRunner struct {
	JobType string
	Runner  ITaskRunner
}

// New returns a SimpleTaskRunner wrapping runner for the given jobType.
func New(jobType string, r ITaskRunner) *SimpleTaskRunner {
	return &SimpleTaskRunner{JobType: jobType, Runner: r}
}

// ProvideRunner registers a SimpleTaskRunner constructor into the batch_simple_runners fx group.
// The constructor may declare any fx-injectable parameters and must return *SimpleTaskRunner.
func ProvideRunner(constructor any) {
	core.Provides(fx.Annotate(constructor, fx.ResultTags(`group:"`+Group+`"`)))
}

// RegisterRunner registers a struct type T as a simple job runner for the given jobType.
// T must embed fx.In (for dependency injection) and implement ITaskRunner (via pointer receiver).
func RegisterRunner[T any, PT interface {
	*T
	ITaskRunner
}](jobType string) {
	ProvideRunner(func(p T) *SimpleTaskRunner {
		pp := PT(&p)
		return New(jobType, pp)
	})
}

type moduleParams struct {
	fx.In
	Items   store.IWorkItemStore
	Runners []*SimpleTaskRunner `group:"batch_simple_runners"`
}

// Module registers all SimpleTaskRunners collected via the batch_simple_runners fx group.
// Call once in batch.go init() before scheduler.NewScheduler.
func Module() {
	core.Invoke(func(p moduleParams) {
		for _, r := range p.Runners {
			Register(r.JobType, p.Items, r.Runner)
		}
	})
}

// Register adds a job type with the given name to the global scheduler registry.
// The job finds all pending WorkItems of that type and calls runner.Run for each.
// Items that succeed are marked DONE; items that fail are marked FAILED. A runner
// may return store.Retry/store.RetryWithCause to reset the item to PENDING (with a
// scheduled next_run_at) instead of failing it permanently, or store.ErrHandled to
// signal it has already finalized the item itself (e.g. MarkDone together with child
// inserts in a transaction) so the framework applies no default Mark*.
func Register(jobType string, items store.IWorkItemStore, runner ITaskRunner) {
	scheduler.Jobs[jobType] = makeFactory(items, runner)
}

// defaultRunTimeout is used when the job config sets no lock-timeout.
const defaultRunTimeout = 30 * time.Second

func makeFactory(items store.IWorkItemStore, runner ITaskRunner) scheduler.JobFactory {
	return func(name string, _ *scheduler.Services, config scheduler.Config) gocron.Task {
		// workType is the WorkItem.Type queried by FindPending. It defaults to the
		// job type (the registry key), so it only needs to be set explicitly when the
		// WorkItem.Type differs from the configured type.
		workType := config.Properties["workType"]
		if workType == "" {
			workType = config.Type
		}
		selfFeed := config.Properties["selfFeed"] == "true"
		// The run context timeout is taken from lock-timeout so long-running jobs
		// (minutes) are not cut off by the default; falls back to defaultRunTimeout.
		timeout := defaultRunTimeout
		if config.LockTimeout > 0 {
			timeout = config.LockTimeout
		}
		return gocron.NewTask(func() error {
			return run(name, workType, selfFeed, timeout, items, runner)
		})
	}
}

func run(name, workType string, selfFeed bool, timeout time.Duration, items store.IWorkItemStore, runner ITaskRunner) error {
	jobID := fmt.Sprintf("%s-%s", name, time.Now().Format("20060102150405"))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if selfFeed {
		now := time.Now()
		wi := []*store.WorkItem{{
			Id:         uuid.New().String(),
			Type:       workType,
			ObjectId:   workType,
			Status:     store.StatusPending,
			CreateTime: now,
			NextRunAt:  &now,
		}}
		if n, insertErr := items.InsertIfNotActive(ctx, wi); insertErr != nil {
			log.Warn().Err(insertErr).Msgf("[%s] selfFeed insert failed", jobID)
		} else if n > 0 {
			log.Info().Msgf("[%s] selfFeed created %d workitem(s)", jobID, n)
		}
	}

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

	var done, handled, retried, failed int
	for _, item := range pending {
		// Same lifecycle convention as distributedjob (store.ApplyResult):
		// nil→MarkDone, store.Retry→MarkPending, err→MarkFailed, store.ErrHandled→untouched.
		runErr := runner.Run(ctx, item, items)
		outcome, markErr := store.ApplyResult(ctx, items, item.Id, runErr)
		if markErr != nil {
			log.Error().Err(markErr).Msgf("[%s] persisting outcome failed for item %s", jobID, item.Id)
		}
		switch outcome {
		case store.OutcomeDone:
			done++
		case store.OutcomeHandled:
			handled++
		case store.OutcomeRetry:
			log.Warn().Err(runErr).Msgf("[%s] transient failure for item %s, reset to PENDING", jobID, item.Id)
			retried++
		case store.OutcomeFailed:
			log.Error().Err(runErr).Msgf("[%s] task failed for item %s", jobID, item.Id)
			failed++
		}
	}

	log.Info().Msgf("[%s] done=%d handled=%d retry=%d failed=%d", jobID, done, handled, retried, failed)
	return nil
}
