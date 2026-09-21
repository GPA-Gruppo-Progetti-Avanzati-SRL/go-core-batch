// Package runner provides the shared ITaskRunner interface and registration
// infrastructure used by all distributedjob dispatcher implementations
// (localdispatcher, grpcdispatcher worker side, and any future transport).
package runner

import (
	"context"
	"fmt"
	"io"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"go.uber.org/fx"
)

// Group is the fx group tag used to collect all registered TaskRunners.
const Group = "batch_runners"

// ITaskRunner is the single runner contract, shared with simplejob via store.ITaskRunner.
// A runner is interchangeable between distributedjob and simplejob without code changes.
//
// The framework applies the lifecycle from the return value (see store.ApplyResult):
//
//	return nil                                   → MarkDone
//	return store.Retry(d) / RetryWithCause(d, e) → MarkPending (transient, retry later)
//	return err                                   → MarkFailed
//	return store.ErrHandled                      → left untouched (runner finalized it,
//	                                               e.g. MarkDone + child inserts in a TX,
//	                                               using an fx-injected IWorkItemStore)
type ITaskRunner = store.ITaskRunner

// TaskRunner binds an ITaskRunner to the task NAME it handles — cioè al nome dell'istanza
// dichiarata nella sezione `tasks:` (senza quella sezione il nome coincide col task type).
// È il nome che viaggia in WorkItem.Type e che governa claiming e instradamento.
type TaskRunner struct {
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
func (t *TaskRunner) ResolveMaxRetry() int {
	if t == nil || t.MaxRetry == nil {
		return task.MaxRetryUnlimited
	}
	return *t.MaxRetry
}

// WithMaxRetry fissa il tetto ai ritentativi e restituisce il runner, per comporre con New.
func (t *TaskRunner) WithMaxRetry(n int) *TaskRunner {
	t.MaxRetry = &n
	return t
}

// New returns a TaskRunner wrapping runner for the given task name.
func New(taskName string, r ITaskRunner) *TaskRunner {
	return &TaskRunner{TaskName: taskName, Runner: r}
}

// MuxRunner routes task execution to the registered ITaskRunner by task name.
type MuxRunner struct {
	// routes conserva il *TaskRunner e non il solo ITaskRunner: serve anche il tetto ai
	// ritentativi dell'istanza, che Run passa a store.ApplyResult.
	routes map[string]*TaskRunner
}

// NewMux builds a MuxRunner from a slice of TaskRunners (typically collected via fx.Group).
func NewMux(runners []*TaskRunner) *MuxRunner {
	routes := make(map[string]*TaskRunner, len(runners))
	for _, tr := range runners {
		routes[tr.TaskName] = tr
	}
	return &MuxRunner{routes: routes}
}

// Run è il punto in cui il percorso in-process (localdispatcher) emette le metriche di task:
// è il solo che ha in mano sia il task name sia la store.Outcome, e resta uno solo anche se il
// dispatcher cambia. Il percorso gRPC NON passa di qui (va su worker.Run), quindi non c'è
// doppio conteggio.
//
// Riceve il WorkItem già claimato dal job: prima lo rileggeva con GetById, che su questo
// percorso era una query per item buttata — l'item era già in memoria, completo, dal claim.
func (r *MuxRunner) Run(ctx context.Context, item *store.WorkItem, items store.IWorkItemStore) error {
	taskName := item.TaskName
	// TaskStart prima di risolvere la route, a specchio di worker.Run che fa LogStart prima di
	// risolvere il runner: un task che non parte è comunque un task fallito, e senza questo
	// sarebbe invisibile alle metriche.
	start := batchmetrics.TaskStart(taskName)
	runner, ok := r.routes[taskName]
	if !ok {
		batchmetrics.ObserveTask(taskName, store.OutcomeFailed, start)
		// L'item va finalizzato comunque, altrimenti resta IN_PROGRESS fino al recupero orfani
		// e riprova all'infinito un task che questo processo non sa eseguire.
		err := fmt.Errorf("no runner registered for task name %q", taskName)
		if markErr := items.MarkFailed(ctx, item.Id, item.LockToken, err.Error()); markErr != nil {
			return markErr
		}
		return err
	}
	runErr := runner.Runner.Run(ctx, item)
	outcome, markErr := store.ApplyResult(ctx, items, item, runner.ResolveMaxRetry(), runErr)
	batchmetrics.ObserveTask(taskName, outcome, start)
	if markErr != nil {
		return markErr
	}
	// Done/Handled → success (SetTaskDone); Retry/Failed → surface the error (SetTaskInError).
	if outcome == store.OutcomeDone || outcome == store.OutcomeHandled {
		return nil
	}
	return runErr
}

// Provide registers a TaskRunner constructor into the batch_runners fx group.
// The constructor may declare any fx-injectable parameters.
//
// Example:
//
//	func init() { runner.Provide(newMyRunner) }
//
//	func newMyRunner(svc myPkg.IService) *runner.TaskRunner {
//	    return runner.New("MY_TASK", &myRunner{svc: svc})
//	}
func Provide(constructor any) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+Group+`"`)))
}

// Register registra il tipo struct T come task runner per il task type indicato. T deve implementare
// ITaskRunner (via receiver a puntatore) e dichiarare i suoi campi con i tag di go-core-app:
//
//	`inject:""` / `inject:"nome"` / `from:"gruppo"`  → dipendenza iniettata da fx
//	`prop:"chiave"`                                   → property applicativa del task (sezione `tasks:`)
//	nessun tag                                        → campo di lavorazione, ignorato dal grafo
//
// Viene fornito a fx un runner per ogni ISTANZA attiva del task type, cioè per ogni voce della
// sezione `tasks:` con quel type che un job o un worker referenzia. Ogni istanza riceve le proprie
// properties: due job possono quindi eseguire lo stesso task type con configurazioni diverse.
//
// La dichiarazione in `tasks:` è OBBLIGATORIA: un task type registrato e non dichiarato fa fallire
// l'avvio. Un task dichiarato che nessuno referenzia non viene istanziato: le sue dipendenze non
// entrano nel grafo. L'istanza è condivisa fra le esecuzioni, quindi i campi di lavorazione NON sono
// per-esecuzione.
//
// Va chiamata dentro la funzione di registrazione passata a batch.Module: è lì che la config è nota.
//
//	func Register() { runner.Register[myRunner]("IMPORT") }
//
//	type myRunner struct {
//	    Svc    myPkg.IService `inject:""`
//	    Folder string         `prop:"folder" validate:"required"`
//	}
//	func (r *myRunner) Run(ctx context.Context, item *store.WorkItem) error { ... }
func Register[T any, PT interface {
	*T
	ITaskRunner
}](taskType string) {
	for _, tc := range task.Instances(taskType) {
		core.ProvideStruct(func(p *T) *TaskRunner {
			return New(tc.Name, PT(p)).WithMaxRetry(tc.ResolveMaxRetry())
		},
			owner(tc.Name, taskType), tc.Properties, Group)
	}
}

// owner è l'etichetta con cui core.ProvideStruct contestualizza i suoi errori (dipendenza mancante,
// property non valida): senza, fx riporterebbe solo `reflect.makeFuncStub`.
func owner(taskName, taskType string) string {
	return fmt.Sprintf("batch: task %q (type %q)", taskName, taskType)
}

// IFileRunner is the interface for file-based task runners (e.g. S3).
// The runner receives the file key and an io.Reader with the file content.
type IFileRunner interface {
	Run(ctx context.Context, key string, content io.Reader) error
}

// FileTaskRunner binds an IFileRunner to the task name it handles.
type FileTaskRunner struct {
	TaskName string
	Runner   IFileRunner
}

// NewFile returns a FileTaskRunner wrapping runner for the given task name.
func NewFile(taskName string, r IFileRunner) *FileTaskRunner {
	return &FileTaskRunner{TaskName: taskName, Runner: r}
}

// FileGroup is the fx group tag used to collect all registered FileTaskRunners.
const FileGroup = "batch_file_runners"

// ProvideFile registers a FileTaskRunner constructor into the batch_file_runners fx group.
// The constructor may declare any fx-injectable parameters.
func ProvideFile(constructor any) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+FileGroup+`"`)))
}

// RegisterFile è l'analogo di Register per i runner su file (es. S3): T deve implementare IFileRunner.
// Vale lo stesso contratto sui tag e la stessa istanziazione per voce della sezione `tasks:` (che va
// dichiarata anche qui).
//
//	func Register() { runner.RegisterFile[myS3Runner]("S3_IMPORT") }
//
//	type myS3Runner struct {
//	    Svc mysvc.IService `inject:""`
//	}
//	func (r *myS3Runner) Run(ctx context.Context, key string, content io.Reader) error { ... }
func RegisterFile[T any, PT interface {
	*T
	IFileRunner
}](taskType string) {
	for _, tc := range task.Instances(taskType) {
		core.ProvideStruct(func(p *T) *FileTaskRunner { return NewFile(tc.Name, PT(p)) },
			owner(tc.Name, taskType), tc.Properties, FileGroup)
	}
}
