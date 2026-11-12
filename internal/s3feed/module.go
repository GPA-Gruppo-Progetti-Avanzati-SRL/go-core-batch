// Package s3feed è l'implementazione dei flussi su file S3: il feed (elenco del bucket → un work
// item per file, dal blocco s3: del task), il job FeedS3Task che lo esegue a cron, il s3.Feeder con
// cui go-core-trigger (o l'app) lo esegue su richiesta,
// il feed del job DistribuiteTaskByS3File e l'avvolgimento dei file runner (runner.RegisterFile) col
// ciclo download/spostamento del file. I file runner arrivano dal gruppo batch_file_runners e,
// avvolti, finiscono nel gruppo batch_runners, da cui li instradano SingleTask e i dispatcher. Il
// guscio pubblico che l'app passa a batch.WithS3 è scheduler/distributedjob/s3feed.
package s3feed

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
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

// feedSourceProvide fornisce il feed del job DistribuiteTaskByS3File, col nome che il job — wirato
// da batch.Module, che non importa l'SDK AWS — si fa iniettare.
func feedSourceProvide() any {
	return fx.Annotate(
		func(reg *s3client.Registry) distributedjob.IFeedSource { return newFeed(reg) },
		fx.ResultTags(`name:"`+distributedjob.S3FeedName+`"`),
	)
}

// Module registra i flussi su file S3: il job FeedS3Task, il s3.Feeder, il feed di
// DistribuiteTaskByS3File e l'avvolgimento dei file runner. Registry e implementazione del feeder
// sono privati al modulo: sono ingranaggi, e all'app arriva il solo seam s3.Feeder. È il corpo di
// scheduler/distributedjob/s3feed.Module; la s3.Config (e la sezione jobs:, per il Feeder) la
// supplisce batch.Module, con la sezione tasks: (i blocchi s3: dei task).
//
// Tutto è lazy: il job si costruisce solo dove gira lo scheduler, l'avvolgimento solo dove qualcuno
// esegue i runner, il Feeder solo se l'app lo inietta. È ciò che permette a batch.WithS3 di
// registrarlo senza mode gate.
func Module(modes ...string) {
	core.Private(func() { core.Provide(provideRegistry, modes...) })
	core.Provide(wrappedRunnersProvide(), modes...)
	core.Provide(feedSourceProvide(), modes...)
	core.Private(func() { core.Provide(newFeeder, modes...) })
	core.Provide(provideFeeder, modes...)
	scheduler.ProvideJob(registerFeedS3, modes...)
}
