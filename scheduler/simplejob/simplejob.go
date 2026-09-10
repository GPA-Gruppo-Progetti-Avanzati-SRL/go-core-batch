// Package simplejob provides an in-process job type for go-core-batch.
// It finds pending WorkItems and executes them locally via a registered ITaskRunner,
// without the need for Kafka or gRPC infrastructure.
//
// Wiring: simplejob.Module() in init(), RegisterRunner dentro la funzione di registrazione passata a
// batch.Module (è lì che la sezione `tasks:` è nota):
//
//	simplejob.Module()
//	func Register() { simplejob.RegisterRunner[myRunner]("HelloWorld") }
//
// Config: il task va SEMPRE dichiarato; `taskName` nomina l'istanza da eseguire e vale di default il
// `type` del job, quindi si omette quando la voce di `tasks:` non ha un `name` proprio.
//
//	tasks:
//	  - type: "HelloWorld"
//	    properties:
//	      saluto: "ciao"
//	jobs:
//	  - name: "hello-world"
//	    type: "HelloWorld"
//	    cron: "*/5 * * * * *"
package simplejob

import (
	"context"
	"fmt"
	"time"
	"uuid"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
	"go.uber.org/fx"
)

// Group is the fx group tag used to collect all registered SimpleTaskRunners.
const Group = "batch_simple_runners"

// ITaskRunner is the single runner contract, shared with distributedjob via
// store.ITaskRunner — a runner is interchangeable between the two families.
// The framework applies the lifecycle from the return value (see store.ApplyResult):
// nil→MarkDone, store.Retry→MarkPending, err→MarkFailed, store.ErrHandled→left untouched.
type ITaskRunner = store.ITaskRunner

// SimpleTaskRunner lega un ITaskRunner al task che esegue: TaskType è il tipo registrato con
// RegisterRunner — per simplejob è anche il `type` della voce di `jobs:`, ed è quindi la chiave della
// JobRegistration — e TaskName è il nome dell'istanza, cioè la voce della sezione `tasks:` da cui
// arrivano le properties. È lo stesso nome che il job indica con `taskName` e che finisce in
// WorkItem.Type.
type SimpleTaskRunner struct {
	TaskType string
	TaskName string
	Runner   ITaskRunner
	// MaxRetry è il tetto ai ritentativi dell'istanza, copiato da task.Config alla
	// registrazione: task.Instances funziona solo dentro task.Apply, quindi dopo il boot non
	// esiste più una lookup della config per nome e il limite deve viaggiare col runner.
	//
	// È un puntatore per la stessa ragione di task.Config.MaxRetry: nil vale illimitato, così
	// nemmeno una struct costruita a mano finisce per negare ogni ritentativo.
	MaxRetry *int
}

// ResolveMaxRetry applica la convenzione dell'assenza: nil = illimitato.
func (r *SimpleTaskRunner) ResolveMaxRetry() int {
	if r == nil || r.MaxRetry == nil {
		return task.MaxRetryUnlimited
	}
	return *r.MaxRetry
}

// WithMaxRetry fissa il tetto ai ritentativi e restituisce il runner, per comporre con New.
func (r *SimpleTaskRunner) WithMaxRetry(n int) *SimpleTaskRunner {
	r.MaxRetry = &n
	return r
}

// New returns a SimpleTaskRunner wrapping runner for the given taskType (istanza col nome = tipo).
func New(taskType string, r ITaskRunner) *SimpleTaskRunner {
	return NewNamed(taskType, taskType, r)
}

// NewNamed è New per una istanza nominata (più voci in `tasks:` con lo stesso type).
func NewNamed(taskType, taskName string, r ITaskRunner) *SimpleTaskRunner {
	return &SimpleTaskRunner{TaskType: taskType, TaskName: taskName, Runner: r}
}

// ProvideRunner registers a SimpleTaskRunner constructor into the batch_simple_runners fx group.
// The constructor may declare any fx-injectable parameters and must return *SimpleTaskRunner.
func ProvideRunner(constructor any) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+Group+`"`)))
}

// RegisterRunner registra il tipo struct T come runner del task type indicato — che per simplejob è
// anche il `type` della voce di `jobs:`, non essendoci dispatch. T deve implementare
// ITaskRunner (via receiver a puntatore) e dichiarare i suoi campi con i tag di go-core-app:
//
//	`inject:""` / `inject:"nome"` / `from:"gruppo"`  → dipendenza iniettata da fx
//	`prop:"chiave"`                                   → property applicativa del task (sezione `tasks:`)
//	nessun tag                                        → campo di lavorazione, ignorato dal grafo
//
// Viene fornito un runner per ogni ISTANZA attiva: ogni voce della sezione `tasks:` con quel type e
// referenziata da un job, che la indica con la property infrastrutturale `taskName` (di default il
// `type` del job). La dichiarazione in `tasks:` è obbligatoria. L'istanza è condivisa fra i tick,
// quindi i campi di lavorazione NON sono per-esecuzione.
func RegisterRunner[T any, PT interface {
	*T
	ITaskRunner
}](taskType string) {
	for _, tc := range task.Instances(taskType) {
		core.ProvideStruct(func(p *T) *SimpleTaskRunner {
			return NewNamed(taskType, tc.Name, PT(p)).WithMaxRetry(tc.ResolveMaxRetry())
		},
			fmt.Sprintf("batch: simplejob task %q (type %q)", tc.Name, taskType), tc.Properties, Group)
	}
}

// newJobRegistrations trasforma i SimpleTaskRunner raccolti dal gruppo batch_simple_runners
// in JobRegistration, una per runner. Il job trova tutti i WorkItem pending di quel tipo e
// chiama runner.Run per ciascuno. Items che riescono → DONE, che falliscono → FAILED. Un
// runner può ritornare store.Retry/store.RetryWithCause per riportare l'item a PENDING (con
// next_run_at schedulato) invece di fallirlo definitivamente, o store.ErrHandled per segnalare
// di aver già finalizzato l'item (es. MarkDone insieme a insert figli in transazione), così il
// framework non applica alcun Mark* di default.
func newJobRegistrations(items store.IWorkItemStore, runners []*SimpleTaskRunner) []scheduler.JobRegistration {
	// Un task type può avere più istanze (più voci in `tasks:`): la JobRegistration resta una per
	// task type — che per simplejob è il `type` del job — e la factory sceglie l'istanza col
	// `taskName` della singola voce di `jobs:`.
	// La mappa conserva il *SimpleTaskRunner e non il solo ITaskRunner: serve anche il tetto ai
	// ritentativi dell'istanza, che il job passa a store.ApplyResult.
	byType := make(map[string]map[string]*SimpleTaskRunner)
	var order []string
	for _, r := range runners {
		if _, seen := byType[r.TaskType]; !seen {
			byType[r.TaskType] = make(map[string]*SimpleTaskRunner)
			order = append(order, r.TaskType)
		}
		byType[r.TaskType][r.TaskName] = r
	}

	regs := make([]scheduler.JobRegistration, 0, len(order))
	for _, taskType := range order {
		regs = append(regs, scheduler.JobRegistration{Type: taskType, Factory: makeFactory(items, byType[taskType])})
	}
	return regs
}

// Module registers all SimpleTaskRunners collected via the batch_simple_runners fx group,
// emitting one JobRegistration per runner into the batch_jobs group (flatten). L'ordine
// rispetto allo scheduler è indifferente: fx risolve il gruppo prima di newScheduler.
// Call once in batch.go init().
func Module(modes ...string) {
	core.Provide(fx.Annotate(
		newJobRegistrations,
		fx.ParamTags(``, `group:"`+Group+`"`),
		fx.ResultTags(`group:"`+scheduler.JobGroup+`,flatten"`),
	), modes...)
}

// defaultBatchLimit caps how many items a single tick claims when no "limit" property is set.
const defaultBatchLimit = 100

func makeFactory(items store.IWorkItemStore, instances map[string]*SimpleTaskRunner) scheduler.JobFactory {
	return func(name string, _ *scheduler.Services, config scheduler.Config) gocron.Task {
		// taskName nomina il TASK da eseguire: è il nome della voce di `tasks:` ed è anche il
		// WorkItem.Type su cui filtra ClaimPending. Di default è il `type` del job, che copre il
		// caso comune (voce di `tasks:` senza `name`, quindi nome uguale al type).
		taskName := config.Properties.GetString("task", config.Type)
		runner := instances[taskName]
		selfFeed := config.Properties.GetBool("selfFeed", false)
		// limit caps how many items are claimed (and processed) per tick.
		limit := config.Properties.GetInt("limit", defaultBatchLimit)
		if limit <= 0 {
			log.Warn().Msgf("[%s] invalid 'limit' property, using default %d", name, defaultBatchLimit)
			limit = defaultBatchLimit
		}
		// Convenzione unica (scheduler.Config.ResolveTimeouts): LockTimeout governa sia il
		// timeout del context di run sia l'età di orphan usata da RecoverOrphans.
		timeout, orphanTimeout := config.ResolveTimeouts()
		if runner == nil {
			log.Error().Msgf("[%s] nessun task %q fra le istanze registrate per il type %q: il job fallirà a ogni tick",
				name, taskName, config.Type)
		}
		return scheduler.LabeledTask(name, config.Type, func() error {
			if runner == nil {
				return fmt.Errorf("simplejob: job %q: nessun task %q registrato per il type %q", name, taskName, config.Type)
			}
			return run(name, taskName, selfFeed, timeout, orphanTimeout, limit, items, runner)
		})
	}
}

func run(name, taskName string, selfFeed bool, timeout, orphanTimeout time.Duration, limit int, items store.IWorkItemStore, runner *SimpleTaskRunner) error {
	jobID := fmt.Sprintf("%s-%s", name, time.Now().Format("20060102150405"))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if selfFeed {
		now := time.Now()
		wi := []*store.WorkItem{{
			Id:         uuid.NewV7().String(),
			TaskName:   taskName,
			ObjectId:   taskName,
			Status:     store.StatusPending,
			CreateTime: now,
			NextRunAt:  &now,
		}}
		if n, insertErr := items.InsertIfNotActive(ctx, wi); insertErr != nil {
			log.Warn().Err(insertErr).Msgf("[%s] selfFeed insert failed", jobID)
		} else if n > 0 {
			log.Info().Msgf("[%s] selfFeed created %d workitem(s)", jobID, n)
		}
	}

	// 1+2. Recupero orfani + claim dei PENDING freschi — loop comune (store.ClaimBatch).
	// ClaimPending marca gli item IN_PROGRESS atomicamente (precondizione per MarkDone/Failed/Pending).
	pending, _, _, claimErr := store.ClaimBatch(ctx, items, jobID, taskName, "", "", orphanTimeout, limit)
	if claimErr != nil {
		log.Error().Err(claimErr).Msgf("[%s] ClaimPending failed", jobID)
		if len(pending) == 0 {
			return claimErr
		}
		// altrimenti si processano comunque gli orfani già recuperati
	}
	if len(pending) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobID)
		return nil
	}

	// Le metriche di job usano il NOME del job (name), non jobID: quest'ultimo contiene un
	// timestamp e come label farebbe esplodere le serie.
	batchmetrics.JobClaimed(name, taskName, len(pending))

	log.Info().Msgf("[%s] processing %d item(s)", jobID, len(pending))

	maxRetry := runner.ResolveMaxRetry()

	var done, handled, retried, failed, exhausted int
	for _, item := range pending {
		// Same lifecycle convention as distributedjob (store.ApplyResult):
		// nil→MarkDone, store.Retry→MarkPending, err→MarkFailed, store.ErrHandled→untouched.
		// Un store.Retry oltre il tetto del task diventa MarkFailed: vedi store.ApplyResult.
		start := batchmetrics.TaskStart(taskName)
		runErr := runner.Runner.Run(ctx, item)
		outcome, markErr := store.ApplyResult(ctx, items, item, maxRetry, runErr)
		// Lo stesso start alle due: misurano per costruzione la stessa finestra.
		batchmetrics.ObserveTask(taskName, outcome, start)
		batchmetrics.JobProcessed(name, taskName, outcome, start)
		if markErr != nil {
			log.Error().Err(markErr).Msgf("[%s] persisting outcome failed for item %s", jobID, item.Id)
		}
		switch outcome {
		case store.OutcomeDone:
			done++
		case store.OutcomeHandled:
			handled++
		case store.OutcomeRetry:
			log.Warn().Err(runErr).Msgf("[%s] transient failure for item %s, reset to PENDING", jobID, item.Id)
			retried++
		case store.OutcomeExhausted:
			log.Error().Err(runErr).Msgf("[%s] item %s: esauriti i %d ritentativi previsti, FAILED",
				jobID, item.Id, maxRetry)
			exhausted++
		case store.OutcomeFailed:
			log.Error().Err(runErr).Msgf("[%s] task failed for item %s", jobID, item.Id)
			failed++
		}
	}

	log.Info().Msgf("[%s] done=%d handled=%d retry=%d exhausted=%d failed=%d",
		jobID, done, handled, retried, exhausted, failed)
	return nil
}
