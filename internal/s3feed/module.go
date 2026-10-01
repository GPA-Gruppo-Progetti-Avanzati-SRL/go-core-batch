// Package s3feed è l'implementazione del job DistribuiteTaskByS3File: un feed S3 dentro la pipeline
// di claiming di distributedjob, e l'avvolgimento dei file runner (runner.RegisterFile) col ciclo
// download/spostamento del file. I file runner arrivano dal gruppo batch_file_runners e, avvolti,
// finiscono nel gruppo batch_runners, da cui li instrada il dispatcher. Il guscio pubblico che l'app
// passa a batch.WithModule è scheduler/distributedjob/s3feed.
package s3feed

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

func provideRegistry(cfg s3.Config) (*s3client.Registry, error) {
	return s3client.NewRegistry(&cfg)
}

func wrapFileRunners(reg *s3client.Registry, fileRunners []*taskrunner.FileTaskRunner) []*taskrunner.TaskRunner {
	wrapped := make([]*taskrunner.TaskRunner, len(fileRunners))
	for i, fr := range fileRunners {
		// Il tetto ai ritentativi viaggia con l'avvolgimento: è il *TaskRunner a finire nel gruppo
		// batch_runners, quindi è il suo MaxRetry quello che mux.Runner.Run passa a ApplyResult.
		wrapped[i] = taskrunner.New(fr.TaskName, newFileRunner(reg, fr.Runner)).WithMaxRetry(fr.ResolveMaxRetry())
	}
	return wrapped
}

// wrappedRunnersProvide restituisce il provider annotato dei file runner (gruppo batch_runners).
func wrappedRunnersProvide() any {
	return fx.Annotate(
		wrapFileRunners,
		fx.ParamTags(``, `group:"`+taskrunner.FileGroup+`"`),
		fx.ResultTags(`group:"`+taskrunner.Group+`,flatten"`),
	)
}

func registerS3(d distributedjob.ITaskDispatcher, items store.IWorkItemStore, data store.IData, reg *s3client.Registry) scheduler.JobRegistration {
	feed := newFeed(reg)
	return distributedjob.RegisterByS3File(d, items, feed, data)
}

// Module registra il job DistribuiteTaskByS3File: il Registry S3, il feed, la JobRegistration e
// l'avvolgimento dei file runner (download/spostamento S3) nel gruppo batch_runners, da cui li
// instrada il dispatcher. È il corpo di scheduler/distributedjob/s3feed.Module, il guscio pubblico
// che l'app passa a batch.WithModule; la s3.Config la supplisce batch.Module.
func Module(modes ...string) {
	core.Provide(provideRegistry, modes...)
	core.Provide(wrappedRunnersProvide(), modes...)
	scheduler.ProvideJob(registerS3, modes...)
}
