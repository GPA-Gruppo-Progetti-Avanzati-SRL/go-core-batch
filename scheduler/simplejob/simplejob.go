// Package simplejob fornisce il job SingleTask: esegue in-process UN work item per tick del
// task che la sua voce di `jobs:` nomina, senza Kafka né gRPC.
//
// Sta in contrapposizione a DistribuiteTask, e la differenza è tutta nel nome: lì molti item
// vengono reclamati e DISTRIBUITI a un dispatcher (in-process o gRPC), qui ne viene preso uno e
// eseguito in linea, dentro il tick. Chi ha volumi usa l'altro.
//
// I perimetri sono tre e non si mescolano:
//
//   - COSA SI SA FARE — il task type, registrato una volta sola con runner.Register[T]; è
//     agnostico, non dice da chi verrà eseguito;
//   - COSA ACCODARE — il job FeedTask (package scheduler/feedjob), o l'applicazione, o l'API;
//   - COME ESEGUIRE — questo job, oppure DistribuiteTask, oppure un worker pool.
//
// Passare dall'uno all'altro è una riga di `jobs:`, non una ricompilazione.
//
// Wiring: simplejob.Module() in un init() oppure via batch.WithModule; i runner si registrano
// con runner.Register dentro la funzione passata a batch.Module (è lì che `tasks:` è nota):
//
//	simplejob.Module()
//	func Register() { runner.Register[myRunner]("HelloWorld") }
//
// Config — il `type` del job è SEMPRE "SingleTask", e `properties.task` nomina l'istanza da
// eseguire fra quelle dichiarate in `tasks:`:
//
//	tasks:
//	  - name: "hello-world"
//	    type: "HelloWorld"
//	    properties:
//	      saluto: "ciao"
//	jobs:
//	  - name: "hello-world"
//	    type: "SingleTask"
//	    cron: "*/5 * * * * *"
//	    properties:
//	      task: "hello-world"
package simplejob

import (
	"context"
	"fmt"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// JobType è il `type` da scrivere nella voce di `jobs:`. È un JOB type, non un task type: quale
// task eseguire lo dice `properties.task`.
const JobType = "SingleTask"

// Properties del job.
const (
	// PropTask nomina l'istanza di task da eseguire: una voce di `tasks:`. È OBBLIGATORIA e non
	// ha ripieghi — prima mancando si eseguiva il task omonimo al job type, ed era il punto in
	// cui i due perimetri si confondevano.
	PropTask = "task"
	// PropLimit è letta solo per dire che è ignorata: SingleTask esegue un item per tick.
	PropLimit = "limit"
)

// ITaskRunner è il contratto dei runner, condiviso con distributedjob via store.ITaskRunner: lo
// stesso runner è eseguibile dalle due famiglie senza modifiche.
// Il framework applica il ciclo di vita dal valore di ritorno (vedi store.ApplyResult):
// nil→MarkDone, store.Retry→MarkPending, err→MarkFailed, store.ErrHandled→lasciato intatto.
type ITaskRunner = store.ITaskRunner

// newJobRegistration trasforma i runner raccolti dal gruppo batch_runners in UNA JobRegistration
// per il job type SingleTask, con le istanze indicizzate per nome.
//
// Una sola registrazione, e non più una per task type: il task type è il perimetro di CHI SA
// FARE, il job type quello di COME ESEGUIRE, e prima il primo finiva per fare da secondo.
// L'indicizzazione per nome è la stessa di runner.NewMux, cioè quella che distributedjob e il
// worker gRPC usano già.
func newJobRegistration(items store.IWorkItemStore, runners []*runner.TaskRunner) scheduler.JobRegistration {
	byName := make(map[string]*runner.TaskRunner, len(runners))
	for _, r := range runners {
		byName[r.TaskName] = r
	}
	return scheduler.JobRegistration{Type: JobType, Factory: makeFactory(items, byName)}
}

// Module registra il job SingleTask, che esegue i runner raccolti nel gruppo batch_runners —
// lo stesso gruppo di distributedjob e del worker gRPC, perché la registrazione di un task non
// dice da chi verrà eseguito. Se modes è vuoto registra sempre; altrimenti solo quando
// core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.Provide(fx.Annotate(
		newJobRegistration,
		fx.ParamTags(``, `group:"`+runner.Group+`"`),
		fx.ResultTags(`group:"`+scheduler.JobGroup+`"`),
	), modes...)
}

func makeFactory(items store.IWorkItemStore, instances map[string]*runner.TaskRunner) scheduler.JobFactory {
	return func(name string, _ *scheduler.Services, config scheduler.Config) gocron.Task {
		taskName, tr, resolveErr := risolvi(name, instances, config)
		if resolveErr != nil {
			// Si logga già alla costruzione, non solo al primo tick: un job che non può
			// funzionare deve vedersi all'avvio, quando c'è ancora qualcuno che guarda.
			log.Error().Err(resolveErr).Msgf("[%s] il job fallirà a ogni tick", name)
		}
		if config.Properties.Has(PropLimit) {
			log.Warn().Msgf("[%s] la property %q è ignorata: %s esegue un item per tick; "+
				"per lavorarne molti si usa DistribuiteTask", name, PropLimit, JobType)
		}
		// Convenzione unica (scheduler.Config.ResolveTimeouts): LockTimeout governa sia il
		// timeout del context di run sia l'età di orphan usata da RecoverOrphans.
		timeout, orphanTimeout := config.ResolveTimeouts()
		return scheduler.LabeledTask(name, config.Type, func() error {
			if resolveErr != nil {
				return resolveErr
			}
			return run(name, taskName, timeout, orphanTimeout, items, tr)
		})
	}
}

// risolvi trova il task che il job deve eseguire. Fallisce, invece di ripiegare: il ripiego che
// c'era — il task omonimo al `type` del job — è esattamente ciò che confondeva i due perimetri,
// e faceva sì che un refuso in `properties.task` eseguisse silenziosamente qualcos'altro.
func risolvi(name string, instances map[string]*runner.TaskRunner, config scheduler.Config) (string, *runner.TaskRunner, error) {
	taskName := config.Properties.GetString(PropTask, "")
	if taskName == "" {
		return "", nil, fmt.Errorf("simplejob: job %q di type %q senza la property %q: non si sa quale task eseguire",
			name, JobType, PropTask)
	}
	tr, ok := instances[taskName]
	if !ok {
		return "", nil, fmt.Errorf("simplejob: job %q: nessun task %q fra le istanze registrate", name, taskName)
	}
	return taskName, tr, nil
}

// run esegue un tick: reclama un item del task e lo lavora in linea.
//
// Un item per tick è ciò che il nome del job type promette. Prima il tetto era la property
// `limit` (default 100) e gli item venivano lavorati in SERIE dentro lo stesso tick, quindi
// sotto lo stesso lock-timeout: un batch nascosto, con un timeout che valeva per tutti insieme.
func run(name, taskName string, timeout, orphanTimeout time.Duration, items store.IWorkItemStore, tr *runner.TaskRunner) error {
	jobID := fmt.Sprintf("%s-%s", name, time.Now().Format("20060102150405"))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 1+2. Recupero orfani + claim del PENDING più vecchio — loop comune (store.ClaimBatch).
	// ClaimPending marca l'item IN_PROGRESS atomicamente (precondizione per MarkDone/Failed/Pending).
	pending, _, _, claimErr := store.ClaimBatch(ctx, items, jobID, taskName, "", "", orphanTimeout, 1)
	if claimErr != nil {
		log.Error().Err(claimErr).Msgf("[%s] ClaimPending failed", jobID)
		if len(pending) == 0 {
			return claimErr
		}
		// altrimenti si processa comunque l'orfano già recuperato
	}
	if len(pending) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobID)
		return nil
	}

	// Le metriche di job usano il NOME del job (name), non jobID: quest'ultimo contiene un
	// timestamp e come label farebbe esplodere le serie.
	batchmetrics.JobClaimed(name, taskName, len(pending))

	maxRetry := tr.ResolveMaxRetry()
	item := pending[0]

	// Stessa convenzione di ciclo di vita di distributedjob (store.ApplyResult):
	// nil→MarkDone, store.Retry→MarkPending, err→MarkFailed, store.ErrHandled→intatto.
	// Un store.Retry oltre il tetto del task diventa MarkFailed: vedi store.ApplyResult.
	start := batchmetrics.TaskStart(taskName)
	runErr := tr.Runner.Run(ctx, item)
	outcome, markErr := store.ApplyResult(ctx, items, item, maxRetry, runErr)
	// Lo stesso start alle due: misurano per costruzione la stessa finestra.
	batchmetrics.ObserveTask(taskName, outcome, start)
	batchmetrics.JobProcessed(name, taskName, outcome, start)
	if markErr != nil {
		log.Error().Err(markErr).Msgf("[%s] persisting outcome failed for item %s", jobID, item.Id)
	}

	switch outcome {
	case store.OutcomeRetry:
		log.Warn().Err(runErr).Msgf("[%s] transient failure for item %s, reset to PENDING", jobID, item.Id)
	case store.OutcomeExhausted:
		log.Error().Err(runErr).Msgf("[%s] item %s: esauriti i %d ritentativi previsti, FAILED",
			jobID, item.Id, maxRetry)
	case store.OutcomeFailed:
		log.Error().Err(runErr).Msgf("[%s] task failed for item %s", jobID, item.Id)
	}

	log.Info().Msgf("[%s] item %s: %s", jobID, item.Id, batchmetrics.OutcomeName(outcome))
	return nil
}
