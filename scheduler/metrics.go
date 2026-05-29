package scheduler

import (
	"time"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	TaskAssigned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_assigned",
			Help: "Number of Tasks Assigned",
		},
		[]string{"type"},
	)

	TaskAssignedKO = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_assigned_ko",
			Help: "Number of Tasks Not Assigned",
		},
		[]string{"type"},
	)

	IncrementJob = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "job_launched",
		Help: "Number of jobs launched",
	},
		[]string{"name", "status"},
	)

	JobExecution = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "job_execution",
		Help: "Duration of jobs in seconds",
	},
		[]string{"name"},
	)
)

type SchedulerMetrics struct{}

func NewSchedulerMetrics() *SchedulerMetrics {
	prometheus.MustRegister(TaskAssigned)
	prometheus.MustRegister(TaskAssignedKO)
	prometheus.MustRegister(IncrementJob)
	prometheus.MustRegister(JobExecution)
	return &SchedulerMetrics{}
}

func (sm *SchedulerMetrics) IncrementJob(id uuid.UUID, name string, tags []string, status gocron.JobStatus) {
	IncrementJob.With(prometheus.Labels{"name": name, "status": string(status)}).Inc()
}

func (sm *SchedulerMetrics) RecordJobTiming(startTime, endTime time.Time, _ uuid.UUID, name string, tags []string) {
	duration := endTime.Sub(startTime)
	JobExecution.With(prometheus.Labels{"name": name}).Observe(duration.Seconds())
}
