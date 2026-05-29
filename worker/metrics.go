package worker

import "github.com/prometheus/client_golang/prometheus"

var (
	TaskStart = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_start",
			Help: "Number of Tasks Start",
		},
		[]string{"type"},
	)

	TaskDone = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_done",
			Help: "Number of Tasks Done",
		},
		[]string{"type"},
	)

	TaskError = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_error",
			Help: "Number of Tasks in Error",
		},
		[]string{"type"},
	)

	TaskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "task_duration_seconds",
		Help: "Task duration in seconds",
	}, []string{"type", "result"})
)

func init() {
	prometheus.MustRegister(TaskStart)
	prometheus.MustRegister(TaskDone)
	prometheus.MustRegister(TaskError)
	prometheus.MustRegister(TaskDuration)
}
