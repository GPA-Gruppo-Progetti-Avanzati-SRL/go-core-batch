// Package taskrunner lega un runner registrato al task NAME che serve, col suo tetto ai
// ritentativi: è ciò che runner.Register fornisce al value group e che i job della libreria (mux
// in-process, SingleTask, worker pool gRPC, s3feed) consumano. Un'app non lo nomina: registra i
// suoi runner con runner.Register/RegisterFile.
package taskrunner

import (
	"context"
	"io"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// Group is the fx group tag used to collect all registered TaskRunners.
const Group = "batch_runners"

// TaskRunner binds an ITaskRunner to the task NAME it handles — cioè al nome dell'istanza
// dichiarata nella sezione `tasks:` (senza quella sezione il nome coincide col task type).
// È il nome che viaggia in WorkItem.Type e che governa claiming e instradamento.
type TaskRunner struct {
	TaskName string
	Runner   store.ITaskRunner
	// MaxRetry è il tetto ai ritentativi dell'istanza, copiato da task.Config alla
	// registrazione: taskreg.Instances funziona solo dentro taskreg.Apply, quindi dopo il boot non
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
func New(taskName string, r store.ITaskRunner) *TaskRunner {
	return &TaskRunner{TaskName: taskName, Runner: r}
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
	// MaxRetry è il tetto ai ritentativi dell'istanza, con la stessa semantica e la stessa ragione
	// di TaskRunner.MaxRetry: nil = illimitato. Sta anche qui perché un file runner finisce comunque
	// nel gruppo batch_runners — s3feed lo avvolge in un *TaskRunner — e senza il campo il tetto si
	// perdeva nel passaggio, rendendo `max-retry:` efficace per Register e inefficace per
	// RegisterFile: un knob che vale a metà è peggio di un knob che non vale.
	MaxRetry *int
}

// ResolveMaxRetry applica la convenzione dell'assenza: nil = illimitato.
func (t *FileTaskRunner) ResolveMaxRetry() int {
	if t == nil || t.MaxRetry == nil {
		return task.MaxRetryUnlimited
	}
	return *t.MaxRetry
}

// WithMaxRetry fissa il tetto ai ritentativi e restituisce il runner, per comporre con NewFile.
func (t *FileTaskRunner) WithMaxRetry(n int) *FileTaskRunner {
	t.MaxRetry = &n
	return t
}

// NewFile returns a FileTaskRunner wrapping runner for the given task name.
func NewFile(taskName string, r IFileRunner) *FileTaskRunner {
	return &FileTaskRunner{TaskName: taskName, Runner: r}
}

// FileGroup is the fx group tag used to collect all registered FileTaskRunners.
const FileGroup = "batch_file_runners"
