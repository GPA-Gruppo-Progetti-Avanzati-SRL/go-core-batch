package worker

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type ITaskService[T any] interface {
	GetTaskExecutions(taskType string) (RunTask[T], bool)
	GetServices() T
}

type RunTask[T any] func(t *Task, s T, items store.IWorkItemStore) *core.ApplicationError

type Task struct {
	Id        string
	JobId     string
	Type      string
	ObjectId  string
	StartTime time.Time
	Context   context.Context
	Cancel    context.CancelFunc
}

func GenerateTask(id, jobid, tasktype, objectid string, ctx context.Context, cancel context.CancelFunc) Task {
	return Task{
		Id:        id,
		JobId:     jobid,
		Type:      tasktype,
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
	TaskStart.With(prometheus.Labels{"type": w.Type}).Inc()
	w.StartTime = time.Now()
	data.SetTaskStart(w.Context, w.Id, w.JobId, w.Type, w.ObjectId)
}

func (w *Task) LogTaskError(data store.IData, errMsg string) {
	log.Error().Msg(errMsg)
	TaskError.With(prometheus.Labels{"type": w.Type}).Inc()
	data.SetTaskInError(w.Context, w.Id, w.JobId, w.Type, w.ObjectId, errMsg)
	w.evaluateDuration("KO")
}

func (w *Task) LogDone(data store.IData) {
	TaskDone.With(prometheus.Labels{"type": w.Type}).Inc()
	data.SetTaskDone(w.Context, w.Id, w.JobId, w.Type, w.ObjectId)
	w.evaluateDuration("OK")
}

func (w *Task) evaluateDuration(result string) {
	duration := time.Since(w.StartTime)
	TaskDuration.With(prometheus.Labels{"type": w.Type, "result": result}).Observe(duration.Seconds())
}
