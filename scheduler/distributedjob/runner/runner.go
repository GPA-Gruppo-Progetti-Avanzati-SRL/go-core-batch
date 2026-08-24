// Package runner provides the shared ITaskRunner interface and registration
// infrastructure used by all distributedjob dispatcher implementations
// (localdispatcher, grpcdispatcher worker side, and any future transport).
package runner

import (
	"context"
	"fmt"
	"io"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
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
}

// New returns a TaskRunner wrapping runner for the given task name.
func New(taskName string, r ITaskRunner) *TaskRunner {
	return &TaskRunner{TaskName: taskName, Runner: r}
}

// MuxRunner routes task execution to the registered ITaskRunner by task name.
type MuxRunner struct {
	routes map[string]ITaskRunner
}

// NewMux builds a MuxRunner from a slice of TaskRunners (typically collected via fx.Group).
func NewMux(runners []*TaskRunner) *MuxRunner {
	routes := make(map[string]ITaskRunner, len(runners))
	for _, tr := range runners {
		routes[tr.TaskName] = tr.Runner
	}
	return &MuxRunner{routes: routes}
}

func (r *MuxRunner) Run(ctx context.Context, objectId, taskType string, items store.IWorkItemStore) error {
	runner, ok := r.routes[taskType]
	if !ok {
		return fmt.Errorf("no runner registered for taskType %q", taskType)
	}
	item, appErr := items.GetById(ctx, objectId)
	if appErr != nil {
		return appErr
	}
	runErr := runner.Run(ctx, item)
	outcome, markErr := store.ApplyResult(ctx, items, item.Id, item.LockToken, runErr)
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
		core.ProvideStruct(func(p *T) *TaskRunner { return New(tc.TaskName(), PT(p)) },
			owner(tc.TaskName(), taskType), tc.Properties, Group)
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
		core.ProvideStruct(func(p *T) *FileTaskRunner { return NewFile(tc.TaskName(), PT(p)) },
			owner(tc.TaskName(), taskType), tc.Properties, FileGroup)
	}
}
