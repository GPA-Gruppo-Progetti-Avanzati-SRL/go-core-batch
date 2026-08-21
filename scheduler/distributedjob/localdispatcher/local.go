// Package localdispatcher provides an in-process ITaskDispatcher for distributedjob.
// Use it in single-instance deployments where tasks run in the same process as the scheduler,
// without the gRPC infrastructure required by grpcdispatcher.
package localdispatcher

import (
	"context"
	"errors"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// LabelTaskType è l'etichetta pprof applicata alle goroutine delle task in-process.
const LabelTaskType = "batch_task_type"

const (
	// defaultMaxConcurrent limita quante task in-process girano contemporaneamente. Senza,
	// tick lenti che si accumulano farebbero crescere le goroutine senza limite.
	defaultMaxConcurrent = 100
	// defaultTaskTimeout è il cap per una singola task: una task bloccata oltre questo tempo
	// viene interrotta (context cancel), liberando lo slot; l'item resta IN_PROGRESS e sarà
	// recuperato come orfano. Generoso per non troncare task legittimamente lunghe.
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

func New(lc fx.Lifecycle, mux *runner.MuxRunner, items store.IWorkItemStore, data store.IData) *LocalDispatcher {
	d := &LocalDispatcher{
		mux:         mux,
		items:       items,
		data:        data,
		sem:         make(chan struct{}, defaultMaxConcurrent),
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

// DispatchTask launches the task in a goroutine and returns immediately. La concorrenza è
// limitata da un semaforo non-bloccante: a slot esauriti ritorna errore (il chiamante segna
// SetTaskAssignationKO, l'item resta IN_PROGRESS e sarà recuperato), come il worker gRPC su
// canale pieno. context.WithoutCancel + WithTimeout scollega la task dal context del tick
// (cancellato appena il tick ritorna) dandole un proprio deadline.
func (d *LocalDispatcher) DispatchTask(ctx context.Context, jobId, taskId, objectId, taskType string) error {
	if d.stopping.Load() {
		return errors.New("localdispatcher: shutting down, dispatch rejected")
	}
	select {
	case d.sem <- struct{}{}:
	default:
		return errors.New("localdispatcher: max concurrency reached")
	}
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.taskTimeout)
	// Etichetta la goroutine col tipo di task (bassa cardinalità): da Go 1.27 la label
	// compare anche nei traceback, oltre che nel profilo goroutineleak.
	labeled := pprof.WithLabels(taskCtx, pprof.Labels(LabelTaskType, taskType))
	d.wg.Go(func() {
		defer cancel()
		defer func() { <-d.sem }()
		pprof.SetGoroutineLabels(labeled)
		d.data.SetTaskStart(taskCtx, taskId, jobId, taskType, objectId)
		if err := d.mux.Run(taskCtx, objectId, taskType, d.items); err != nil {
			d.data.SetTaskInError(taskCtx, taskId, jobId, taskType, objectId, err.Error())
			return
		}
		d.data.SetTaskDone(taskCtx, taskId, jobId, taskType, objectId)
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
