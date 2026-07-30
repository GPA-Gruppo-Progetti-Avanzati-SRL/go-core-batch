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
	"strconv"
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
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+Group+`"`)))
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

// newJobRegistrations trasforma i SimpleTaskRunner raccolti dal gruppo batch_simple_runners
// in JobRegistration, una per runner. Il job trova tutti i WorkItem pending di quel tipo e
// chiama runner.Run per ciascuno. Items che riescono → DONE, che falliscono → FAILED. Un
// runner può ritornare store.Retry/store.RetryWithCause per riportare l'item a PENDING (con
// next_run_at schedulato) invece di fallirlo definitivamente, o store.ErrHandled per segnalare
// di aver già finalizzato l'item (es. MarkDone insieme a insert figli in transazione), così il
// framework non applica alcun Mark* di default.
func newJobRegistrations(items store.IWorkItemStore, runners []*SimpleTaskRunner) []scheduler.JobRegistration {
	regs := make([]scheduler.JobRegistration, len(runners))
	for i, r := range runners {
		regs[i] = scheduler.JobRegistration{Type: r.JobType, Factory: makeFactory(items, r.Runner)}
	}
	return regs
}

// Module registers all SimpleTaskRunners collected via the batch_simple_runners fx group,
// emitting one JobRegistration per runner into the batch_jobs group (flatten). L'ordine
// rispetto allo scheduler è indifferente: fx risolve il gruppo prima di newScheduler.
// Call once in batch.go init().
func Module(modes ...string) {
	core.Provide(fx.Annotate(
		newJobRegistrations,
		fx.ParamTags(``, `group:"`+Group+`"`),
		fx.ResultTags(`group:"`+scheduler.JobGroup+`,flatten"`),
	), modes...)
}

// defaultBatchLimit caps how many items a single tick claims when no "limit" property is set.
const defaultBatchLimit = 100

func makeFactory(items store.IWorkItemStore, runner ITaskRunner) scheduler.JobFactory {
	return func(name string, _ *scheduler.Services, config scheduler.Config) gocron.Task {
		// workType is the WorkItem.Type queried by ClaimPending. It defaults to the
		// job type (the registry key), so it only needs to be set explicitly when the
		// WorkItem.Type differs from the configured type.
		workType := config.Properties["workType"]
		if workType == "" {
			workType = config.Type
		}
		selfFeed := config.Properties["selfFeed"] == "true"
		// limit caps how many items are claimed (and processed) per tick.
		limit := defaultBatchLimit
		if v := config.Properties["limit"]; v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = n
			} else {
				log.Warn().Msgf("[%s] invalid 'limit' property %q, using default %d", name, v, defaultBatchLimit)
			}
		}
		// Convenzione unica (scheduler.Config.ResolveTimeouts): LockTimeout governa sia il
		// timeout del context di run sia l'età di orphan usata da RecoverOrphans.
		timeout, orphanTimeout := config.ResolveTimeouts()
		return gocron.NewTask(func() error {
			return run(name, workType, selfFeed, timeout, orphanTimeout, limit, items, runner)
		})
	}
}

func run(name, workType string, selfFeed bool, timeout, orphanTimeout time.Duration, limit int, items store.IWorkItemStore, runner ITaskRunner) error {
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

	// 1+2. Recupero orfani + claim dei PENDING freschi — loop comune (store.ClaimBatch).
	// ClaimPending marca gli item IN_PROGRESS atomicamente (precondizione per MarkDone/Failed/Pending).
	pending, _, _, claimErr := store.ClaimBatch(ctx, items, jobID, workType, "", "", orphanTimeout, limit)
	if claimErr != nil {
		log.Error().Err(claimErr).Msgf("[%s] ClaimPending failed", jobID)
		if len(pending) == 0 {
			return claimErr
		}
		// altrimenti si processano comunque gli orfani già recuperati
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
		runErr := runner.Run(ctx, item)
		outcome, markErr := store.ApplyResult(ctx, items, item.Id, item.LockToken, runErr)
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
