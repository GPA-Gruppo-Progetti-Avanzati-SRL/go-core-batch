package worker

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type ITaskService[T any] interface {
	GetTaskExecutions(taskName string) (RunTask[T], bool)
	GetServices() T
}

// RunTask esegue il task e ritorna l'esito secondo la convenzione runner condivisa
// (vedi store.ApplyResult): nil → done, store.ErrHandled → già finalizzato dal runner,
// *store.RetryError → retry, qualsiasi altro errore → failed. NON deve chiamare i Mark*
// da sé: è worker.Run l'UNICO punto che applica store.ApplyResult sul valore di ritorno.
type RunTask[T any] func(t *Task, s T, items store.IWorkItemStore) error

type Task struct {
	Id    string
	JobId string
	// TaskName è il nome dell'istanza di task da eseguire (non un "tipo"): arriva dal
	// WorkItem ed è la chiave con cui il pool trova la RunTask registrata.
	TaskName string
	ObjectId string
	// LockToken è il fencing token del claim dell'item: lo popola chi carica il WorkItem
	// (es. grpchandler dopo GetById) e worker.Run lo passa a store.ApplyResult per finalizzare
	// in modo fenced. Vuoto finché non impostato.
	LockToken string
	StartTime time.Time
	Context   context.Context
	Cancel    context.CancelFunc
}

func GenerateTask(id, jobid, taskName, objectid string, ctx context.Context, cancel context.CancelFunc) Task {
	return Task{
		Id:        id,
		JobId:     jobid,
		TaskName:  taskName,
		ObjectId:  objectid,
		StartTime: time.Now(),
		Context:   ctx,
		Cancel:    cancel,
	}
}

func (w *Task) CancelContext() {
	w.Cancel()
}

func (w *Task) GetId() string {
	return w.Id
}

func (w *Task) GetJobId() string {
	return w.JobId
}

func (w *Task) LogStart(data store.IData) {
	TaskStart.With(prometheus.Labels{"type": w.TaskName}).Inc()
	w.StartTime = time.Now()
	data.SetTaskStart(w.Context, w.Id, w.JobId, w.TaskName, w.ObjectId)
}

func (w *Task) LogTaskError(data store.IData, errMsg string) {
	log.Error().Msg(errMsg)
	TaskError.With(prometheus.Labels{"type": w.TaskName}).Inc()
	data.SetTaskInError(w.Context, w.Id, w.JobId, w.TaskName, w.ObjectId, errMsg)
	w.evaluateDuration("KO")
}

func (w *Task) LogDone(data store.IData) {
	TaskDone.With(prometheus.Labels{"type": w.TaskName}).Inc()
	data.SetTaskDone(w.Context, w.Id, w.JobId, w.TaskName, w.ObjectId)
	w.evaluateDuration("OK")
}

func (w *Task) evaluateDuration(result string) {
	duration := time.Since(w.StartTime)
	TaskDuration.With(prometheus.Labels{"type": w.TaskName, "result": result}).Observe(duration.Seconds())
}
