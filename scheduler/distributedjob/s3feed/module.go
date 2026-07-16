// Package s3feed provides the Fx module for the DistribuiteTaskByS3File job type.
// It wires an S3-based feed into the distributedjob claiming pipeline and wraps
// file runners with S3 download/move lifecycle.
//
// Usage:
//
//	func init() {
//	    s3feed.Module()
//	    runner.RegisterFile[myS3Runner]("S3_IMPORT")
//	}
//
// File runners are registered with runner.RegisterFile or runner.ProvideFile.
// They are collected from the batch_file_runners fx group and wrapped with
// S3 download/move lifecycle, then injected into the batch_runners group
// so the localdispatcher's MuxRunner can route them.
package s3feed

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	batch "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

func provideRegistry(cfg batch.Config) (*s3.Registry, error) {
	return s3.NewRegistry(&cfg.S3)
}

func wrapFileRunners(reg *s3.Registry, fileRunners []*runner.FileTaskRunner) []*runner.TaskRunner {
	wrapped := make([]*runner.TaskRunner, len(fileRunners))
	for i, fr := range fileRunners {
		wrapped[i] = runner.New(fr.TaskType, newFileRunner(reg, fr.Runner))
	}
	return wrapped
}

// wrappedRunnersProvide restituisce il provider annotato dei file runner (gruppo batch_runners).
func wrappedRunnersProvide() interface{} {
	return fx.Annotate(
		wrapFileRunners,
		fx.ParamTags(``, `group:"`+runner.FileGroup+`"`),
		fx.ResultTags(`group:"`+runner.Group+`,flatten"`),
	)
}

func registerS3(d distributedjob.ITaskDispatcher, items store.IWorkItemStore, data store.IData, reg *s3.Registry) {
	feed := New(reg)
	distributedjob.RegisterByS3File(d, items, feed, data)
}

// Module registers the DistribuiteTaskByS3File job type unconditionally.
// It provides the S3 Registry, builds the S3Feed, registers the job,
// and wraps all FileTaskRunners with S3 download/move lifecycle,
// injecting them into the batch_runners group for the MuxRunner.
func Module() {
	core.Provides(provideRegistry)
	core.Provides(wrappedRunnersProvide())
	core.Invoke(registerS3)
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvidesIf(provideRegistry, modes...)
	core.ProvidesIf(wrappedRunnersProvide(), modes...)
	core.InvokeIf(registerS3, modes...)
}
