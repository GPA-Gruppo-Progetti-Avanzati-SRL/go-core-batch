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

// TaskRunner binds an ITaskRunner to the task type it handles.
type TaskRunner struct {
	TaskType string
	Runner   ITaskRunner
}

// New returns a TaskRunner wrapping runner for the given taskType.
func New(taskType string, r ITaskRunner) *TaskRunner {
	return &TaskRunner{TaskType: taskType, Runner: r}
}

// MuxRunner routes task execution to the registered ITaskRunner by taskType.
type MuxRunner struct {
	routes map[string]ITaskRunner
}

// NewMux builds a MuxRunner from a slice of TaskRunners (typically collected via fx.Group).
func NewMux(runners []*TaskRunner) *MuxRunner {
	routes := make(map[string]ITaskRunner, len(runners))
	for _, tr := range runners {
		routes[tr.TaskType] = tr.Runner
	}
	return &MuxRunner{routes: routes}
}

// Run è il punto in cui il percorso in-process (localdispatcher) emette le metriche di task:
// è il solo che ha in mano sia il taskType sia la store.Outcome, e resta uno solo anche se il
// dispatcher cambia. Il percorso gRPC NON passa di qui (va su worker.Run), quindi non c'è
// doppio conteggio.
func (r *MuxRunner) Run(ctx context.Context, objectId, taskType string, items store.IWorkItemStore) error {
	// TaskStart prima di risolvere la route, a specchio di worker.Run che fa LogStart prima di
	// risolvere il tipo: un task che non parte è comunque un task fallito, e senza questo
	// sarebbe invisibile alle metriche.
	start := batchmetrics.TaskStart(taskType)
	runner, ok := r.routes[taskType]
	if !ok {
		batchmetrics.ObserveTask(taskType, store.OutcomeFailed, start)
		return fmt.Errorf("no runner registered for taskType %q", taskType)
	}
	item, appErr := items.GetById(ctx, objectId)
	if appErr != nil {
		batchmetrics.ObserveTask(taskType, store.OutcomeFailed, start)
		return appErr
	}
	runErr := runner.Run(ctx, item)
	outcome, markErr := store.ApplyResult(ctx, items, item.Id, item.LockToken, runErr)
	batchmetrics.ObserveTask(taskType, outcome, start)
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

// Register registers a struct type T as a task runner for the given taskType.
// T must embed fx.In (for dependency injection) and implement ITaskRunner (via pointer receiver).
//
// Example:
//
//	func init() { runner.Register[myRunner]("MY_TASK") }
//
//	type myRunner struct {
//	    fx.In
//	    Svc myPkg.IService
//	}
//	func (r *myRunner) Run(ctx context.Context, item *store.WorkItem) error { ... }
func Register[T any, PT interface {
	*T
	ITaskRunner
}](taskType string) {
	Provide(func(p T) *TaskRunner {
		pp := PT(&p)
		return New(taskType, pp)
	})
}

// IFileRunner is the interface for file-based task runners (e.g. S3).
// The runner receives the file key and an io.Reader with the file content.
type IFileRunner interface {
	Run(ctx context.Context, key string, content io.Reader) error
}

// FileTaskRunner binds an IFileRunner to the task type it handles.
type FileTaskRunner struct {
	TaskType string
	Runner   IFileRunner
}

// NewFile returns a FileTaskRunner wrapping runner for the given taskType.
func NewFile(taskType string, r IFileRunner) *FileTaskRunner {
	return &FileTaskRunner{TaskType: taskType, Runner: r}
}

// FileGroup is the fx group tag used to collect all registered FileTaskRunners.
const FileGroup = "batch_file_runners"

// ProvideFile registers a FileTaskRunner constructor into the batch_file_runners fx group.
// The constructor may declare any fx-injectable parameters.
func ProvideFile(constructor any) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+FileGroup+`"`)))
}

// RegisterFile registers a struct type T as a file-based task runner for the given taskType.
// T must embed fx.In (for dependency injection) and implement IFileRunner (via pointer receiver).
//
// Example:
//
//	func init() { runner.RegisterFile[myS3Runner]("S3_IMPORT") }
//
//	type myS3Runner struct {
//	    fx.In
//	    Svc mysvc.IService
//	}
//	func (r *myS3Runner) Run(ctx context.Context, key string, content io.Reader) error { ... }
func RegisterFile[T any, PT interface {
	*T
	IFileRunner
}](taskType string) {
	ProvideFile(func(p T) *FileTaskRunner {
		pp := PT(&p)
		return NewFile(taskType, pp)
	})
}
