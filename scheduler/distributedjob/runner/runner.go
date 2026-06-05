// Package runner provides the shared ITaskRunner interface and registration
// infrastructure used by all distributedjob dispatcher implementations
// (localdispatcher, grpcdispatcher worker side, and any future transport).
package runner

import (
	"context"
	"fmt"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

// Group is the fx group tag used to collect all registered TaskRunners.
const Group = "batch_runners"

// ITaskRunner is the interface each task type implements.
// The runner owns the full workitem lifecycle:
//
//	items.MarkDone(ctx, []string{objectId})       — success
//	items.MarkPending(ctx, objectId, delay)        — transient error, retry later
//	items.MarkFailed(ctx, objectId, reason)        — permanent failure
type ITaskRunner interface {
	Run(ctx context.Context, objectId string, items store.IWorkItemStore) error
}

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

func (r *MuxRunner) Run(ctx context.Context, objectId, taskType string, items store.IWorkItemStore) error {
	runner, ok := r.routes[taskType]
	if !ok {
		return fmt.Errorf("no runner registered for taskType %q", taskType)
	}
	return runner.Run(ctx, objectId, items)
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
	core.Provides(fx.Annotate(constructor, fx.ResultTags(`group:"`+Group+`"`)))
}
