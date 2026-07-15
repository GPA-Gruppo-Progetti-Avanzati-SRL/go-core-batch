package worker

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

type Workers[T any] struct {
	TaskChannel map[string]chan *Task
	OsChannel   chan os.Signal
	StopChannel chan struct{} // closed by OnStop to broadcast shutdown to every worker
	TaskRoutes  map[string]string
	BatchData   store.IData
	WorkItems   store.IWorkItemStore // optional: closes workitem lifecycle after each task
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
func NewWorkers[T any](lc fx.Lifecycle, workersConfig []Config, data store.IData, services ITaskService[T], items store.IWorkItemStore) *Workers[T] {
	w := &Workers[T]{BatchData: data, WorkItems: items}
	w.TaskChannel = make(map[string]chan *Task)
	w.TaskRoutes = make(map[string]string)
	w.StopChannel = make(chan struct{})
	osCh := make(chan os.Signal, 1)
	signal.Notify(osCh, syscall.SIGINT, syscall.SIGTERM)
	w.OsChannel = osCh

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
				go NewWorker[T](k, channel, w.StopChannel, w.OsChannel, services, data, items)
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
			return nil
		},
	})
	return w
}

func NewWorker[T any](k string, channel chan *Task, stopCh <-chan struct{}, osCh <-chan os.Signal, services ITaskService[T], batchData store.IData, items store.IWorkItemStore) *Workers[T] {
	log.Info().Msgf("Starting %s worker", k)
	capacity := cap(channel)
	log.Info().Msgf("Capacity Channel %d", capacity)
	semaphore := make(chan struct{}, capacity)
	for {
		select {
		case <-stopCh:
			log.Info().Msgf("Worker %s: stop signal received, terminating", k)
			return nil
		case <-osCh:
			log.Info().Msgf("Worker %s: OS signal received, terminating", k)
			return nil
		case ch, ok := <-channel:
			if !ok {
				log.Info().Msgf("Worker %s: task channel closed, terminating", k)
				return nil
			}
			if ch == nil {
				continue
			}
			// Acquire the semaphore slot only when a real task will be launched,
			// so it is always released by Run's deferred <-semaphore.
			semaphore <- struct{}{}
			log.Trace().Msgf("W - %s - %s - Green Signal Executing task in worker channel", ch.GetJobId(), ch.GetId())
			go Run(semaphore, ch, services, batchData, items)
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
	run, ok := services.GetTaskExecutions(t.Type)
	if !ok {
		errMsg := "Execution type not found"
		log.Error().Msgf("W - %s - %s - Esecuzione non trovata per tipo: %s", t.GetJobId(), t.GetId(), t.Type)
		t.LogTaskError(data, errMsg)
		if items != nil {
			items.MarkFailed(t.Context, t.ObjectId, errMsg)
		}
		return
	}
	err := run(t, services.GetServices(), items)
	if err != nil {
		log.Error().Msgf("W - %s - %s - Error executing task: %s", t.GetJobId(), t.GetId(), err.Error())
		t.LogTaskError(data, err.Error())
	} else {
		log.Trace().Msgf("W - %s - %s - Executed task %T", t.GetJobId(), t.GetId(), t)
		t.LogDone(data)
	}
}
