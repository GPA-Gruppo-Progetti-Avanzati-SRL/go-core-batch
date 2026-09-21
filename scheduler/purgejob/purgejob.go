// Package purgejob fornisce il job di retention dei work item e dei task log.
//
// Serve perché senza di lui le due collection crescono per SEMPRE: gli item DONE non venivano
// mai rimossi e task_logs riceve fino a tre righe per item lavorato. È il degrado più prevedibile
// del framework nel tempo — e quello che si nota più tardi, perché insieme alle collection
// crescono gli indici su cui gira il claim di ogni tick.
//
// Non claima e non dispatcha: è una cancellazione a finestra, ripetuta a ogni tick con un tetto
// (`limit`) che tiene corta la singola transazione. Volendo cancellare un arretrato grosso, lo si
// fa in più tick invece che in una botta sola che tiene il database occupato.
//
// La retention NON ha un default: va abilitata scrivendo il job in `jobs:`. Cancellare dati non
// può essere un comportamento che si ottiene aggiornando la libreria.
//
// Wiring:
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(purgejob.Module))
//
// Config:
//
//	jobs:
//	  - name: retention-done
//	    type: PurgeWorkItems
//	    cron: "0 30 3 * * *"
//	    singleton: true
//	    lock-timeout: 10m
//	    properties:
//	      status:     DONE      # DONE | FAILED (o qualunque stato) — obbligatoria
//	      older-than: 168h      # obbligatoria
//	      limit:      5000      # facoltativa, default 1000
//	      task-logs:  true      # facoltativa: cancella anche le righe di task_logs più vecchie
package purgejob

import (
	"context"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

// JobType è il `type` da scrivere nella voce di `jobs:`.
const JobType = "PurgeWorkItems"

// Properties del job. Sono INFRASTRUTTURALI: le legge il framework.
const (
	// PropStatus è lo stato degli item da cancellare (DONE, FAILED, …). Obbligatoria e senza
	// default: "quali item cancellare" non è una domanda a cui la libreria possa rispondere.
	PropStatus = "status"
	// PropOlderThan è l'età minima (durata) oltre la quale un item è cancellabile, misurata
	// sull'update_time. Obbligatoria.
	PropOlderThan = "older-than"
	// PropLimit è il tetto di cancellazioni per tick.
	PropLimit = "limit"
	// PropTaskLogs, se true, cancella anche le righe di task_logs più vecchie di older-than.
	PropTaskLogs = "task-logs"
)

const defaultLimit = 1000

// Register costruisce la JobRegistration del job PurgeWorkItems. È un costruttore fx: il
// risultato confluisce nel value group batch_jobs via scheduler.ProvideJob.
func Register(items store.IWorkItemStore, data store.IData) scheduler.JobRegistration {
	return scheduler.JobRegistration{Type: JobType, Factory: makeFactory(items, data)}
}

// Module registra il job PurgeWorkItems. Se modes è vuoto registra sempre; altrimenti solo
// quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	scheduler.ProvideJob(Register, modes...)
}

type parametri struct {
	status    string
	olderThan time.Duration
	limit     int
	taskLogs  bool
}

func makeFactory(items store.IWorkItemStore, data store.IData) scheduler.JobFactory {
	return func(name string, _ *scheduler.Services, config scheduler.Config) gocron.Task {
		p, resolveErr := risolvi(name, config)
		if resolveErr != nil {
			// Come per le altre famiglie: un job che non può funzionare si vede all'avvio, non
			// al primo tick.
			log.Error().Err(resolveErr).Msgf("[%s] il job fallirà a ogni tick", name)
		}
		timeout, _ := config.ResolveTimeouts()
		return scheduler.LabeledTask(name, config.Type, func() error {
			if resolveErr != nil {
				return resolveErr
			}
			return run(name, timeout, p, items, data)
		})
	}
}

func risolvi(name string, config scheduler.Config) (parametri, error) {
	p := config.Properties
	var out parametri
	if !p.Has(PropStatus) {
		return out, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("purgejob: job %q senza la property %q: non si sa quali item cancellare", name, PropStatus))
	}
	out.status = p.GetString(PropStatus, "")
	if out.status == "" {
		return out, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("purgejob: job %q: la property %q è vuota", name, PropStatus))
	}
	if !p.Has(PropOlderThan) {
		return out, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("purgejob: job %q senza la property %q: non si sa da quanto un item sia cancellabile", name, PropOlderThan))
	}
	out.olderThan = p.GetDuration(PropOlderThan, 0)
	if out.olderThan <= 0 {
		return out, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("purgejob: job %q: la property %q non è una durata positiva: %v", name, PropOlderThan, p[PropOlderThan]))
	}
	out.limit = p.GetInt(PropLimit, defaultLimit)
	if out.limit <= 0 {
		return out, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("purgejob: job %q: la property %q non è un intero positivo: %v", name, PropLimit, p[PropLimit]))
	}
	out.taskLogs = p.GetBool(PropTaskLogs, false)
	return out, nil
}

func run(name string, timeout time.Duration, p parametri, items store.IWorkItemStore, data store.IData) error {
	jobID := scheduler.NewJobID(name)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cutoff := time.Now().Add(-p.olderThan)

	n, appErr := items.Purge(ctx, p.status, cutoff, p.limit)
	if appErr != nil {
		log.Error().Err(appErr).Msgf("[%s] purge dei work item fallita", jobID)
		return appErr
	}
	if n > 0 {
		log.Info().Msgf("[%s] cancellati %d work item %s più vecchi di %s", jobID, n, p.status, p.olderThan)
	}

	if p.taskLogs {
		m, logErr := data.PurgeTaskLogs(ctx, cutoff, p.limit)
		if logErr != nil {
			log.Error().Err(logErr).Msgf("[%s] purge dei task log fallita", jobID)
			return logErr
		}
		if m > 0 {
			log.Info().Msgf("[%s] cancellate %d righe di task_logs più vecchie di %s", jobID, m, p.olderThan)
		}
	}

	// Un tick che cancella esattamente `limit` item è il segnale che l'arretrato non è finito:
	// il tick successivo ne prenderà un altro scaglione, ma se succede sempre la finestra di
	// retention o la cadenza del cron sono sbagliate.
	if n == p.limit {
		log.Warn().Msgf("[%s] raggiunto il limit di %d: c'è ancora arretrato da cancellare", jobID, p.limit)
	}
	return nil
}
