package distributedjob

import (
	"context"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

// Properties infrastrutturali del job.
const (
	// PropTask nomina l'istanza di task da eseguire: una voce di `tasks:`.
	PropTask = "task"
	// PropLimit è il tetto agli item claimati per tick (backpressure).
	PropLimit = "limit"
)

// makeClaimingFactory costruisce la JobFactory di un job claim-based.
//
// La config del job è risolta QUI, alla costruzione, e non dentro il tick: un refuso in YAML
// deve vedersi all'avvio, quando c'è ancora qualcuno che guarda, non presentarsi come un errore
// di runtime al primo tick e a ogni tick successivo. È la stessa forma di simplejob e feedjob,
// che già facevano così.
func makeClaimingFactory(dispatcher ITaskDispatcher, items store.IWorkItemStore, feed IFeedSource, data store.IData) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		taskName, limit, resolveErr := risolvi(name, config)
		if resolveErr != nil {
			log.Error().Err(resolveErr).Msgf("[%s] il job fallirà a ogni tick", name)
		}
		// Convenzione unica (scheduler.Config.ResolveTimeouts): LockTimeout governa sia il
		// timeout del context di run sia l'età di orphan.
		runTimeout, orphanTimeout := config.ResolveTimeouts()
		tick := scheduler.ClaimingTick{
			JobName:       name,
			JobType:       config.Type,
			TaskName:      taskName,
			Limit:         limit,
			RunTimeout:    runTimeout,
			OrphanTimeout: orphanTimeout,
			Backlog:       config.Properties.GetBool(scheduler.PropBacklogMetrics, false),
			Process: func(ctx context.Context, jobID string, batch []*store.WorkItem) error {
				dispatchBatch(ctx, jobID, taskName, orphanTimeout, batch, dispatcher, items, data)
				return nil
			},
		}
		if feed != nil {
			tick.Feed = func(ctx context.Context, jobID string) {
				runFeedPhase(ctx, feed, items, jobID, taskName, config.Properties, limit)
			}
		}
		return scheduler.LabeledTask(name, config.Type, func() error {
			if resolveErr != nil {
				return resolveErr
			}
			return tick.Run(items)
		})
	}
}

// risolvi legge e valida le property infrastrutturali del job. Fallisce invece di ripiegare: un
// job senza `task` non sa cosa eseguire, e uno senza `limit` valido non sa quanto prenderne.
func risolvi(name string, config scheduler.Config) (taskName string, limit int, err error) {
	p := config.Properties
	if !p.Has(PropTask) {
		return "", 0, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("distributedjob: job %q senza la property %q: non si sa quale task eseguire", name, PropTask))
	}
	taskName = p.GetString(PropTask, "")
	if taskName == "" {
		return "", 0, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("distributedjob: job %q: la property %q è vuota", name, PropTask))
	}
	if !p.Has(PropLimit) {
		return "", 0, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("distributedjob: job %q senza la property %q: non si sa quanti item claimare per tick", name, PropLimit))
	}
	limit = p.GetInt(PropLimit, 0)
	if limit <= 0 {
		return "", 0, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("distributedjob: job %q: la property %q non è un intero positivo: %v", name, PropLimit, p[PropLimit]))
	}
	return taskName, limit, nil
}

// dispatchBatch è la fase di elaborazione di questa famiglia: consegna ogni item al dispatcher.
//
// NON emette batch_job_items_processed_total: il dispatch è asincrono e l'esito dell'esecuzione
// non torna al job. Il livello job è coperto da batch_task_assigned_total, che è ASSEGNAZIONE e
// non esecuzione; l'esito lo emette chi esegue (worker.Run per il percorso gRPC,
// runner.MuxRunner per quello in-process).
func dispatchBatch(ctx context.Context, jobID, taskName string, orphanTimeout time.Duration,
	batch []*store.WorkItem, dispatcher ITaskDispatcher, items store.IWorkItemStore, data store.IData) {

	// Le righe di task_logs sono accumulate e scritte in UNA volta a fine ciclo: erano una
	// insert sincrona per item, dentro il tick e quindi dentro il lock del job — con limit 100,
	// cento round-trip prima che il tick potesse chiudere.
	logs := make([]*store.TaskLog, 0, len(batch))
	for i, item := range batch {
		taskId := fmt.Sprintf("%s-task-%d", jobID, i+1)
		err := dispatcher.DispatchTask(ctx, DispatchRequest{
			JobId: jobID, TaskId: taskId, TaskName: taskName, Item: item, Timeout: orphanTimeout,
		})
		batchmetrics.TaskAssigned(taskName, err)
		if err != nil {
			// L'item è stato claimato ma NESSUNO lo eseguirà: va rilasciato subito, altrimenti
			// resta IN_PROGRESS fino all'orphan timeout e il recupero gli consuma un ritentativo
			// che non ha mai usato. Release è MarkPending senza l'incremento di retry.
			if relErr := items.Release(ctx, item.Id, item.LockToken); relErr != nil {
				log.Error().Err(relErr).Msgf("[%s] rilascio fallito per item %s: resterà IN_PROGRESS fino al recupero orfani", jobID, item.Id)
			}
			logs = append(logs, store.NewTaskLog(taskId, jobID, taskName, item.Id, store.TaskLogAssignedKO, err.Error()))
			log.Error().Err(err).Msgf("[%s] dispatch failed for item %s", jobID, item.Id)
			continue
		}
		logs = append(logs, store.NewTaskLog(taskId, jobID, taskName, item.Id, store.TaskLogAssigned, ""))
		log.Debug().Msgf("[%s] dispatched item %s", jobID, item.Id)
	}
	data.InsertTaskLogs(ctx, logs)
}
