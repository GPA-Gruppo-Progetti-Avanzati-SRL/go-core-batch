// Package batchmetrics è l'unico proprietario dei collector Prometheus di go-core-batch.
//
// Sta in un package foglia — dipende solo da store e da client_golang — perché le metriche di
// task devono essere emesse sia dal worker pool sia dalle famiglie di job lato scheduler
// (simplejob, localdispatcher, kafkajob): tenerle in worker costringerebbe tre package dello
// scheduler a dipendere dal pool (Workers[T], canali, semafori) solo per delle var.
//
// La tassonomia è una sola:
//
//   - outcome ∈ done | handled | retry | exhausted | failed  — l'esito fine di store.Outcome
//   - status  ∈ success | error                              — lo split grossolano
//     (done|handled → success)
//
// Le label sono job (nome del job di config) e task (NOME dell'istanza di task, cioè la voce di
// `tasks:` che finisce in WorkItem.TaskName — non il task type: due istanze dello stesso type si
// distinguono solo per nome, ed è la ragione per cui il rename è stato fatto). Non usare mai
// jobId/taskId: contengono un timestamp e come label farebbero esplodere le serie.
package batchmetrics

import (
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"github.com/prometheus/client_golang/prometheus"
)

// Etichette del vocabolario, esportate perché i test e le dashboard le nominano.
const (
	OutcomeDone    = "done"
	OutcomeHandled = "handled"
	OutcomeRetry   = "retry"
	// OutcomeExhausted distingue "ha smesso di riprovare" da un fallimento secco: l'errore era
	// transiente, ma il task ha esaurito i ritentativi previsti. Sulla dashboard è il segnale
	// che un tetto è troppo basso o che un sistema a valle è giù da troppo tempo.
	OutcomeExhausted = "exhausted"
	OutcomeFailed    = "failed"

	StatusSuccess = "success"
	StatusError   = "error"
)

// Livello job: una serie per esecuzione del cron o per item finalizzato da quella esecuzione.
var (
	// JobTicks conta i tick cron eseguiti. status è la gocron.JobStatus
	// (success/fail/skip/singleton_rescheduled). Conta ANCHE i tick a vuoto, di proposito:
	// è l'unico segnale che dice "il cron sta girando", e un counter piatto sarebbe
	// altrimenti indistinguibile da un processo morto.
	JobTicks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_job_ticks_total",
		Help: "Number of cron ticks executed, including ticks that found no work",
	}, []string{"job", "status"})

	// JobDuration è la durata del tick intero, misurata da gocron.
	JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "batch_job_duration_seconds",
		Help: "Duration of a cron tick in seconds",
	}, []string{"job"})

	// JobItemsClaimed conta gli item presi in carico da un tick. NON si muove sui tick a
	// vuoto: è il segnale di throughput reale, da leggere accanto a JobTicks.
	JobItemsClaimed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_job_items_claimed_total",
		Help: "Number of work items claimed by a job tick",
	}, []string{"job", "task"})

	// JobItemsProcessed conta gli item finalizzati da un tick. Emessa solo dalle famiglie
	// SINCRONE (simplejob, kafkajob): distributedjob dispatcha in modo asincrono e l'esito
	// non gli torna indietro.
	JobItemsProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_job_items_processed_total",
		Help: "Number of work items finalized by a job tick",
	}, []string{"job", "task", "status"})

	// JobItemsProcessedDuration è la durata di elaborazione del singolo item, con il nome del
	// job. Si sovrappone di proposito a TaskDuration: la label job NON è ottenibile sul
	// percorso worker gRPC (lì l'unico identificativo è un jobId con dentro un timestamp),
	// quindi TaskDuration copre tutti i percorsi e questa aggiunge lo slice per job dove il
	// job è noto.
	JobItemsProcessedDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "batch_job_items_processed_duration_seconds",
		Help: "Processing duration of a single work item, sliced by job",
	}, []string{"job", "task", "status"})
)

// Livello task: una serie per esecuzione di un runner, su ogni percorso (worker pool incluso).
var (
	TaskStarted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_task_started_total",
		Help: "Number of task executions started",
	}, []string{"task"})

	// TaskOutcome sostituisce la vecchia coppia task_done/task_error: erano due counter dove
	// serviva una label, e fondevano un retry transitorio con un fallimento definitivo.
	TaskOutcome = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_task_outcome_total",
		Help: "Number of task executions by classified outcome",
	}, []string{"task", "outcome"})

	TaskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "batch_task_duration_seconds",
		Help: "Task execution duration in seconds",
	}, []string{"task", "status"})
)

// Livello dispatch: solo distributedjob.
var (
	// TaskAssignedTotal fonde le vecchie task_assigned e task_assigned_ko, che erano una
	// metrica sorella dove bastava una label.
	TaskAssignedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "batch_task_assigned_total",
		Help: "Number of task dispatch attempts by outcome",
	}, []string{"task", "status"})
)

// La registrazione è in init() e non in un costruttore: MustRegister panica alla seconda
// chiamata, e questi collector hanno più utilizzatori in package diversi.
func init() {
	prometheus.MustRegister(
		JobTicks,
		JobDuration,
		JobItemsClaimed,
		JobItemsProcessed,
		JobItemsProcessedDuration,
		TaskStarted,
		TaskOutcome,
		TaskDuration,
		TaskAssignedTotal,
	)
}

// OutcomeName mappa l'Outcome sull'etichetta di batch_task_outcome_total.
func OutcomeName(o store.Outcome) string {
	switch o {
	case store.OutcomeDone:
		return OutcomeDone
	case store.OutcomeHandled:
		return OutcomeHandled
	case store.OutcomeRetry:
		return OutcomeRetry
	case store.OutcomeExhausted:
		return OutcomeExhausted
	case store.OutcomeFailed:
		return OutcomeFailed
	default:
		return "unknown"
	}
}

// Status è lo split grossolano dell'Outcome: done/handled sono successi (l'item è finalizzato),
// retry/exhausted/failed no.
func Status(o store.Outcome) string {
	switch o {
	case store.OutcomeDone, store.OutcomeHandled:
		return StatusSuccess
	default:
		return StatusError
	}
}

// TaskStart incrementa batch_task_started_total e ritorna l'istante d'inizio da passare a
// ObserveTask e JobProcessed.
func TaskStart(taskName string) time.Time {
	TaskStarted.WithLabelValues(taskName).Inc()
	return time.Now()
}

// ObserveTask emette l'esito e la durata di un task concluso, su qualunque percorso.
func ObserveTask(taskName string, o store.Outcome, started time.Time) {
	TaskOutcome.WithLabelValues(taskName, OutcomeName(o)).Inc()
	TaskDuration.WithLabelValues(taskName, Status(o)).Observe(time.Since(started).Seconds())
}

// JobClaimed registra gli item presi in carico da un tick. È no-op per n <= 0: è la garanzia,
// in un posto solo, che un tick a vuoto non tocchi la metrica — invece di ripetere l'if in
// ogni famiglia e dimenticarlo in una.
func JobClaimed(job, taskName string, n int) {
	if n <= 0 {
		return
	}
	JobItemsClaimed.WithLabelValues(job, taskName).Add(float64(n))
}

// JobProcessed registra un item finalizzato da un tick, con la sua durata. started è lo stesso
// valore passato a ObserveTask, così le due durate misurano per costruzione la stessa finestra.
func JobProcessed(job, taskName string, o store.Outcome, started time.Time) {
	status := Status(o)
	JobItemsProcessed.WithLabelValues(job, taskName, status).Inc()
	JobItemsProcessedDuration.WithLabelValues(job, taskName, status).Observe(time.Since(started).Seconds())
}

// TaskAssigned registra l'esito di un dispatch: err nil è un'assegnazione riuscita.
func TaskAssigned(taskName string, err error) {
	status := StatusSuccess
	if err != nil {
		status = StatusError
	}
	TaskAssignedTotal.WithLabelValues(taskName, status).Inc()
}
