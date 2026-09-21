package worker

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Etichette pprof delle goroutine del pool: a bassa cardinalità (nome del worker e
// tipo di task), così i profili e i traceback restano raggruppabili.
const (
	LabelWorker   = "batch_worker"
	LabelTaskName = "batch_task_name"
)

type Workers[T any] struct {
	TaskChannel map[string]chan *Task
	StopChannel chan struct{} // closed by OnStop to broadcast shutdown to every worker
	TaskRoutes  map[string]string
	BatchData   store.IData
	WorkItems   store.IWorkItemStore // optional: closes workitem lifecycle after each task
	// wg traccia le task IN VOLO (non quelle in coda), per drenarle su OnStop. Senza, un
	// SIGTERM troncava a metà i task già partiti: i loro item restavano IN_PROGRESS fino al
	// recupero orfani, che oltre all'attesa gli consuma un ritentativo.
	wg sync.WaitGroup
}

func (w *Workers[T]) GetChannel(name string) chan *Task {
	routes, okR := w.TaskRoutes[name]
	if !okR {
		log.Trace().Msgf("No Routes found switching on default worker")
		routes = "Default"
	} else {
		log.Trace().Msgf("Assign task on worker %s", routes)
	}
	if val, okC := w.TaskChannel[routes]; okC {
		return val
	}
	return nil
}

// NewWorkers creates the worker pool. Pass items to enable workitem lifecycle management
// (MarkDone/MarkFailed after each task). Pass nil when not using the claiming pattern.
//
// Il pool NON installa un handler di segnale: i segnali li gestisce l'applicazione (core.Run/fx),
// e l'arresto arriva qui come OnStop. Prima c'era un signal.Notify di libreria — un side-effect
// globale che rubava il segnale all'app — e i worker uscivano PRIMA che OnStop girasse, quindi
// nessuno drenava le task già partite.
func NewWorkers[T any](lc fx.Lifecycle, workersConfig []Config, data store.IData, services ITaskService[T], items store.IWorkItemStore) *Workers[T] {
	w := &Workers[T]{BatchData: data, WorkItems: items}
	w.TaskChannel = make(map[string]chan *Task)
	w.TaskRoutes = make(map[string]string)
	w.StopChannel = make(chan struct{})

	for _, v := range workersConfig {
		value := v
		if value.Size < 1 {
			log.Warn().Msgf("Worker %s: invalid size %d, defaulting to 1", value.Name, value.Size)
			value.Size = 1
		}
		w.TaskChannel[value.Name] = make(chan *Task, value.Size)
		for _, t := range value.Tasks {
			log.Trace().Msgf("Assegno Task %s a Worker %s", t, value.Name)
			w.TaskRoutes[t] = value.Name
		}
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for k, channel := range w.TaskChannel {
				// pprof.Do etichetta la goroutine del worker: da Go 1.27 la label
				// compare anche nei traceback, oltre che nei profili.
				go pprof.Do(context.Background(), pprof.Labels(LabelWorker, k), func(context.Context) {
					w.loop(k, channel, services, data, items)
				})
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// Broadcast shutdown to every worker. Task channels are intentionally
			// NOT closed: producers (DispatchTask / gRPC handler) may still be sending,
			// and a send on a closed channel panics. In-flight buffered tasks are
			// already claimed (IN_PROGRESS) and get re-claimed by RecoverOrphans.
			log.Info().Msg("Stopping worker pool")
			close(w.StopChannel)
			// Poi si attende il drain delle task IN VOLO, fino al deadline del context di stop
			// di fx — stesso contratto del localdispatcher. Oltre il deadline le residue sono
			// abbandonate: i loro item restano IN_PROGRESS e li recupera RecoverOrphans.
			done := make(chan struct{})
			go func() { w.wg.Wait(); close(done) }()
			select {
			case <-done:
				log.Info().Msg("worker pool: tutte le task in volo drenate")
			case <-ctx.Done():
				log.Warn().Msg("worker pool: drain scaduto, task residue abbandonate (saranno recuperate come orfani)")
			}
			return nil
		},
	})
	return w
}

// loop è il ciclo di un singolo worker: preleva dal canale e lancia l'esecuzione, con la
// concorrenza limitata dalla capacità del canale.
func (w *Workers[T]) loop(k string, channel chan *Task, services ITaskService[T], batchData store.IData, items store.IWorkItemStore) {
	log.Info().Msgf("Starting %s worker", k)
	capacity := cap(channel)
	log.Info().Msgf("Capacity Channel %d", capacity)
	semaphore := make(chan struct{}, capacity)
	for {
		select {
		case <-w.StopChannel:
			log.Info().Msgf("Worker %s: stop signal received, terminating", k)
			return
		case ch, ok := <-channel:
			if !ok {
				log.Info().Msgf("Worker %s: task channel closed, terminating", k)
				return
			}
			if ch == nil {
				continue
			}
			// Acquire the semaphore slot only when a real task will be launched,
			// so it is always released by Run's deferred <-semaphore.
			semaphore <- struct{}{}
			log.Trace().Msgf("W - %s - %s - Green Signal Executing task in worker channel", ch.GetJobId(), ch.GetId())
			// Il set di label è esplicito (worker + tipo di task): pprof.Do lo
			// sostituisce a quello ereditato dalla goroutine del worker.
			w.wg.Go(func() {
				pprof.Do(context.Background(), pprof.Labels(LabelWorker, k, LabelTaskName, ch.TaskName), func(context.Context) {
					Run(semaphore, ch, services, batchData, items)
				})
			})
		}
	}
}

func Run[T any](semaphore chan struct{}, t *Task, services ITaskService[T], data store.IData, items store.IWorkItemStore) {
	defer func() {
		t.CancelContext()
		<-semaphore
	}()
	log.Trace().Msgf("W - %s - %s - Executing task %T", t.GetJobId(), t.GetId(), t)
	t.LogStart(data)

	// Esegue il task. Il tipo sconosciuto è trattato come un errore normale: confluisce nello
	// stesso punto di finalizzazione sotto (ApplyResult → MarkFailed), niente ramo separato.
	var runErr error
	if run, ok := services.GetTaskExecutions(t.TaskName); ok {
		// La RunTask (es. bridge grpchandler) carica il WorkItem e popola t.Item.
		runErr = run(t, services.GetServices(), items)
	} else {
		log.Error().Msgf("W - %s - %s - Esecuzione non trovata per tipo: %s", t.GetJobId(), t.GetId(), t.TaskName)
		runErr = fmt.Errorf("execution type not found: %s", t.TaskName)
		// Nessuna RunTask ha caricato l'item: lo si recupera per poterlo comunque finalizzare
		// (MarkFailed) in modo fenced, evitando un orphan-loop sul tipo sconosciuto.
		if t.Item == nil && items != nil {
			if it, e := items.GetById(t.Context, t.ObjectId); e == nil {
				t.Item = it
			}
		}
	}

	// worker.Run è l'UNICO punto che finalizza il lifecycle del workitem per il worker pool:
	// applica la convenzione condivisa store.ApplyResult (nil→Done, ErrHandled→no-op,
	// RetryError→Pending fino al tetto del task e poi Exhausted, altro→Failed), fenced dal token
	// del claim. Senza items (no claiming) o senza item caricato si salta la finalizzazione.
	outcome := store.OutcomeDone
	if items != nil && t.Item != nil {
		o, markErr := store.ApplyResult(t.Context, items, t.Item, t.ResolveMaxRetry(), runErr)
		outcome = o
		if markErr != nil {
			log.Error().Msgf("W - %s - %s - finalizzazione lifecycle fallita: %v", t.GetJobId(), t.GetId(), markErr)
		}
	} else if runErr != nil {
		outcome = store.OutcomeFailed
	}

	// Task log + metriche (osservabilità): Done/Handled = successo, Retry/Failed = errore.
	// La classificazione fine (done|handled|retry|failed) la fa LogOutcome sulla Outcome.
	if outcome == store.OutcomeDone || outcome == store.OutcomeHandled {
		log.Trace().Msgf("W - %s - %s - Executed task %T", t.GetJobId(), t.GetId(), t)
	} else {
		log.Error().Msgf("W - %s - %s - Error executing task: %v", t.GetJobId(), t.GetId(), runErr)
	}
	t.LogOutcome(data, outcome, runErr)
}
