// Package s3feed provides the Fx module for the DistribuiteTaskByS3File job type.
// It wires an S3-based feed into the distributedjob claiming pipeline and wraps
// file runners with S3 download/move lifecycle.
//
// Usage (modes-only: la s3.Config è iniettata da fx — la fornisce batch.Module, o l'app con
// core.Supply(cfg.S3) prima di Module() nel wiring manuale):
//
//	func init() {
//	    core.Supply(cfg.S3)
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
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

func provideRegistry(cfg s3.Config) (*s3client.Registry, error) {
	return s3client.NewRegistry(&cfg)
}

func wrapFileRunners(reg *s3client.Registry, fileRunners []*runner.FileTaskRunner) []*runner.TaskRunner {
	wrapped := make([]*runner.TaskRunner, len(fileRunners))
	for i, fr := range fileRunners {
		wrapped[i] = runner.New(fr.TaskType, newFileRunner(reg, fr.Runner))
	}
	return wrapped
}

// wrappedRunnersProvide restituisce il provider annotato dei file runner (gruppo batch_runners).
func wrappedRunnersProvide() any {
	return fx.Annotate(
		wrapFileRunners,
		fx.ParamTags(``, `group:"`+runner.FileGroup+`"`),
		fx.ResultTags(`group:"`+runner.Group+`,flatten"`),
	)
}

func registerS3(d distributedjob.ITaskDispatcher, items store.IWorkItemStore, data store.IData, reg *s3client.Registry) scheduler.JobRegistration {
	feed := New(reg)
	return distributedjob.RegisterByS3File(d, items, feed, data)
}

// Module registers the DistribuiteTaskByS3File job type unconditionally.
// It provides the S3 Registry, builds the S3Feed, registers the job,
// and wraps all FileTaskRunners with S3 download/move lifecycle,
// injecting them into the batch_runners group for the MuxRunner.
// La s3.Config è iniettata da fx (la fornisce batch.Module con core.Supply della Config
// unificata, oppure l'app con core.Supply(cfg.S3) nel wiring manuale): NON è più un parametro.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.Provide(provideRegistry, modes...)
	core.Provide(wrappedRunnersProvide(), modes...)
	scheduler.ProvideJob(registerS3, modes...)
}
