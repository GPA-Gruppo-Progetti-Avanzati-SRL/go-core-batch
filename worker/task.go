package worker

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"

	"github.com/rs/zerolog/log"
)

type ITaskService[T any] interface {
	GetTaskExecutions(taskName string) (RunTask[T], bool)
	GetServices() T
}

// RunTask esegue il task e ritorna l'esito secondo la convenzione runner condivisa
// (vedi store.ApplyResult): nil → done, store.ErrHandled → già finalizzato dal runner,
// *store.RetryError → retry finché il tetto del task lo consente, poi failed, qualsiasi altro
// errore → failed. NON deve chiamare i Mark*
// da sé: è worker.Run l'UNICO punto che applica store.ApplyResult sul valore di ritorno.
type RunTask[T any] func(t *Task, s T, items store.IWorkItemStore) error

type Task struct {
	Id    string
	JobId string
	// TaskName è il nome dell'istanza di task da eseguire (non un "tipo"): arriva dal
	// WorkItem ed è la chiave con cui il pool trova la RunTask registrata.
	TaskName string
	ObjectId string
	// Item è il WorkItem su cui il task lavora. Lo popola chi lo carica — il bridge grpchandler
	// dopo la GetById, o il dispatch in-process che lo riceve già claimato dal job — e worker.Run
	// lo passa INTERO a store.ApplyResult.
	//
	// Prima qui c'erano tre campi copiati a mano (LockToken, Retry) e ApplyResult riceveva un
	// WorkItem ricostruito con quelli: funzionava solo perché ApplyResult leggeva esattamente
	// quei campi, e il giorno in cui ne avesse letto un quarto il worker pool avrebbe sbagliato
	// in silenzio. Resta nil finché nessuno ha caricato l'item (task type sconosciuto).
	Item *store.WorkItem
	// MaxRetry è il tetto ai ritentativi del task, copiato dal wrapper del runner da chi
	// instrada (il bridge grpchandler): nil = illimitato, per la stessa ragione di
	// task.Config.MaxRetry — lo zero-value di un int direbbe "nessun ritentativo". Non sta sul
	// WorkItem perché non è un dato dell'item: è configurazione dell'istanza di task.
	MaxRetry  *int
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

// ResolveMaxRetry applica la convenzione dell'assenza: nil = illimitato.
func (w *Task) ResolveMaxRetry() int {
	if w == nil || w.MaxRetry == nil {
		return task.MaxRetryUnlimited
	}
	return *w.MaxRetry
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
	w.StartTime = batchmetrics.TaskStart(w.TaskName)
	data.SetTaskStart(w.Context, w.Id, w.JobId, w.TaskName, w.ObjectId)
}

// LogOutcome scrive la riga di task_log ed emette le metriche di task per l'esito classificato.
// Sostituisce la vecchia coppia LogDone/LogTaskError, che non conosceva la store.Outcome e
// quindi collassava un retry transitorio in un fallimento definitivo. La riga di task_log resta
// DONE per done/handled ed ERROR per retry/failed: cambia solo che la metrica ora li distingue.
func (w *Task) LogOutcome(data store.IData, o store.Outcome, runErr error) {
	batchmetrics.ObserveTask(w.TaskName, o, w.StartTime)
	if batchmetrics.Status(o) == batchmetrics.StatusSuccess {
		data.SetTaskDone(w.Context, w.Id, w.JobId, w.TaskName, w.ObjectId)
		return
	}
	errMsg := ""
	if runErr != nil {
		errMsg = runErr.Error()
	}
	log.Error().Msg(errMsg)
	data.SetTaskInError(w.Context, w.Id, w.JobId, w.TaskName, w.ObjectId, errMsg)
}
