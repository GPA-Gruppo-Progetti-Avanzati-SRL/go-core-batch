package scheduler

import (
	"context"
	"runtime/pprof"

	"github.com/go-co-op/gocron/v2"
)

// Etichette pprof applicate alle goroutine del framework. Sono a bassa cardinalità
// (nome e tipo del job, non gli id di esecuzione) perché il profilo raggruppa per
// set di label.
const (
	LabelJob     = "batch_job"
	LabelJobType = "batch_job_type"
)

// LabeledTask costruisce la gocron.Task di un job type applicando le pprof label
// del job per tutta la durata del tick.
//
// Da Go 1.27 le label delle goroutine compaiono anche nell'header dei traceback,
// oltre che nei profili: su un processo batch long-running sono ciò che rende
// leggibili /debug/pprof/goroutine e /debug/pprof/goroutineleak, dove altrimenti
// tutti i tick si presentano come goroutine gocron indistinguibili.
//
// I job type definiti dalle applicazioni possono usarla al posto di gocron.NewTask:
//
//	return scheduler.LabeledTask(name, config.Type, func() error { return run(...) })
func LabeledTask(name, jobType string, fn func() error) gocron.Task {
	return gocron.NewTask(func() error {
		var err error
		pprof.Do(context.Background(), pprof.Labels(LabelJob, name, LabelJobType, jobType), func(context.Context) {
			err = fn()
		})
		return err
	})
}
