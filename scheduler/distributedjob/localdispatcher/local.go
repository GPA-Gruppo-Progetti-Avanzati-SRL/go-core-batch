// Package localdispatcher provides an in-process ITaskDispatcher for distributedjob.
// Use it in single-instance deployments where tasks run in the same process as the scheduler,
// without the gRPC infrastructure required by grpcdispatcher.
package localdispatcher

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// LabelTaskName è l'etichetta pprof applicata alle goroutine delle task in-process.
const LabelTaskName = "batch_task_name"

const (
	// minMaxConcurrent è il pavimento del cap di concorrenza: sotto questa soglia non si scende
	// nemmeno se i job configurati chiedono meno.
	minMaxConcurrent = 100
	// defaultTaskTimeout è il cap di una task quando il job non dichiara un lock-timeout. Serve
	// solo come rete: la soglia giusta la porta la DispatchRequest, ed è l'orphan timeout del job.
	defaultTaskTimeout = 30 * time.Minute
)

// LocalDispatcher implements distributedjob.ITaskDispatcher by running tasks in-process.
// It logs the full task lifecycle to task_logs via store.IData and updates the source
// record status via store.IWorkItemStore. Le task in volo sono tracciate da un WaitGroup e
// drenate su OnStop; la concorrenza è limitata da un semaforo.
type LocalDispatcher struct {
	mux         *runner.MuxRunner
	items       store.IWorkItemStore
	data        store.IData
	sem         chan struct{}  // cap di concorrenza (non-bloccante)
	wg          sync.WaitGroup // task in volo, per il drain su OnStop
	stopping    atomic.Bool    // dopo OnStop rifiuta nuovi dispatch
	taskTimeout time.Duration
}

var _ distributedjob.ITaskDispatcher = (*LocalDispatcher)(nil)

// New costruisce il dispatcher in-process. Il cap di concorrenza è DERIVATO dalla config dei job
// — la somma dei `limit` dei job attivi, con un pavimento — e non è più una costante: con un
// `limit` più alto del cap, una parte dei dispatch falliva sistematicamente a ogni tick, ed era
// una config che non poteva funzionare senza che nulla lo dicesse. Dimensionato così, il
// dispatcher assorbe per costruzione un giro completo di ogni job; oltre quello la
// back-pressure (rilascio dell'item e ripresa al tick successivo) è la condotta giusta.
func New(lc fx.Lifecycle, jobs []scheduler.Config, mux *runner.MuxRunner, items store.IWorkItemStore, data store.IData) *LocalDispatcher {
	maxConcurrent := capacita(jobs)
	log.Info().Msgf("localdispatcher: cap di concorrenza %d (derivato dai limit dei job attivi)", maxConcurrent)
	d := &LocalDispatcher{
		mux:         mux,
		items:       items,
		data:        data,
		sem:         make(chan struct{}, maxConcurrent),
		taskTimeout: defaultTaskTimeout,
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			// Niente nuovi dispatch, poi attendi il drain delle task in volo fino al deadline
			// del context di stop di fx. Oltre, le task residue vengono abbandonate: i loro item
			// restano IN_PROGRESS e verranno recuperati come orfani al riavvio.
			d.stopping.Store(true)
			done := make(chan struct{})
			go func() { d.wg.Wait(); close(done) }()
			select {
			case <-done:
				log.Info().Msg("localdispatcher: tutte le task in volo drenate")
			case <-ctx.Done():
				log.Warn().Msg("localdispatcher: drain scaduto, task residue abbandonate (saranno recuperate come orfani)")
			}
			return nil
		},
	})
	return d
}

// capacita somma i `limit` dei job non disabilitati: è quanti item, al massimo, un giro completo
// di tutti i job può mettere in volo contemporaneamente.
func capacita(jobs []scheduler.Config) int {
	somma := 0
	for _, j := range jobs {
		if j.Disabled {
			continue
		}
		if n := j.Properties.GetInt(distributedjob.PropLimit, 0); n > 0 {
			somma += n
		}
	}
	return max(somma, minMaxConcurrent)
}

// DispatchTask launches the task in a goroutine and returns immediately. La concorrenza è
// limitata da un semaforo non-bloccante: a slot esauriti ritorna errore (il chiamante rilascia
// l'item con store.Release, che NON gli consuma un ritentativo), come il worker gRPC su canale
// pieno. context.WithoutCancel + WithTimeout scollega la task dal context del tick (cancellato
// appena il tick ritorna) dandole un proprio deadline.
//
// Il deadline è quello dichiarato dal job (l'orphan timeout): oltre quella soglia l'item viene
// ri-claimato da un altro tick, e lasciar proseguire la task qui significherebbe averne due che
// lavorano lo stesso item. Prima era una costante di 30 minuti, tre volte l'orphan timeout di
// default.
func (d *LocalDispatcher) DispatchTask(ctx context.Context, req distributedjob.DispatchRequest) error {
	if d.stopping.Load() {
		return errors.New("localdispatcher: shutting down, dispatch rejected")
	}
	select {
	case d.sem <- struct{}{}:
	default:
		return fmt.Errorf("localdispatcher: max concurrency reached (%d)", cap(d.sem))
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = d.taskTimeout
	}
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	// Etichetta la goroutine col tipo di task (bassa cardinalità): da Go 1.27 la label
	// compare anche nei traceback, oltre che nel profilo goroutineleak.
	labeled := pprof.WithLabels(taskCtx, pprof.Labels(LabelTaskName, req.TaskName))
	d.wg.Go(func() {
		defer cancel()
		defer func() { <-d.sem }()
		pprof.SetGoroutineLabels(labeled)
		d.data.SetTaskStart(taskCtx, req.TaskId, req.JobId, req.TaskName, req.Item.Id)
		if err := d.mux.Run(taskCtx, req.Item, d.items); err != nil {
			d.data.SetTaskInError(taskCtx, req.TaskId, req.JobId, req.TaskName, req.Item.Id, err.Error())
			return
		}
		d.data.SetTaskDone(taskCtx, req.TaskId, req.JobId, req.TaskName, req.Item.Id)
	})
	return nil
}

type muxParams struct {
	core.In
	Items   store.IWorkItemStore
	Data    store.IData
	Runners []*runner.TaskRunner `group:"batch_runners"`
}

func newMuxRunner(p muxParams) *runner.MuxRunner {
	return runner.NewMux(p.Runners)
}

// Module registers the DistribuiteTask mux dispatcher with the fx application.
// It provides ITaskDispatcher via fx so that queryfeed and s3feed modules can depend
// on it. La JobRegistration del tipo DistribuiteTask confluisce nel value group batch_jobs
// (scheduler.ProvideJob), quindi l'ordine rispetto allo scheduler è indifferente.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.Provide(newMuxRunner, modes...)
	core.ProvideAs[distributedjob.ITaskDispatcher](New, modes...)
	scheduler.ProvideJob(distributedjob.Register, modes...)
}
