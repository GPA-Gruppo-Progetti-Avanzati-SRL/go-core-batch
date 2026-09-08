package scheduler

import (
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

// SchedulerMetrics è il solo adattatore fra il Monitor di gocron e i collector di batchmetrics:
// i collector non stanno più qui perché ne ha bisogno anche chi non importa scheduler.
//
// Attenzione a come si leggono le due metriche che emette: gocron invoca il Monitor a OGNI tick,
// quindi batch_job_ticks_total e batch_job_duration_seconds contano anche i tick che non hanno
// trovato nulla da fare. È voluto — sono il segnale di liveness ("il cron sta girando"). Il
// throughput reale è batch_job_items_claimed_total, che sui tick a vuoto non si muove.
type SchedulerMetrics struct{}

func NewSchedulerMetrics() *SchedulerMetrics {
	return &SchedulerMetrics{}
}

func (sm *SchedulerMetrics) IncrementJob(_ uuid.UUID, name string, _ []string, status gocron.JobStatus) {
	batchmetrics.JobTicks.WithLabelValues(name, string(status)).Inc()
}

func (sm *SchedulerMetrics) RecordJobTiming(startTime, endTime time.Time, _ uuid.UUID, name string, _ []string) {
	batchmetrics.JobDuration.WithLabelValues(name).Observe(endTime.Sub(startTime).Seconds())
}
