package s3feed

import (
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"go.uber.org/fx"
)

// La forma in cui batch.WithS3 registra Module: un modulo APERTO a root con s3.Config, jobs e
// Registry privati e il resto esportato, accanto al sottosistema chiuso "batch" che consuma i value
// group. Il test la riproduce con fx nudo (core tiene le liste in un registry globale non
// esportato) e verifica le tre cose da cui dipende:
//   - l'app, a root, inietta il Feeder;
//   - il sottosistema chiuso vede la JobRegistration di FeedS3Task (batch_jobs), il runner avvolto
//     (batch_runners) e il feed nominato di DistribuiteTaskByS3File;
//   - Registry e implementazione del feeder restano privati.
func TestWiring_FormaDiWithS3(t *testing.T) {
	cfg := s3.Config{Services: map[string]*s3.ServiceConfig{"main": {Bucket: "b", Region: "eu-south-1"}}}
	tasks := []task.Config{{
		Name: "import", Type: "IMPORT", S3: &s3.Source{Service: "main", DestPath: "done"},
	}}

	var (
		feeder  s3.Feeder
		seen    struct{ jobs, runners int }
		hasFeed bool
	)
	type inner struct {
		fx.In
		Jobs    []scheduler.JobRegistration `group:"batch_jobs"`
		Runners []*taskrunner.TaskRunner    `group:"batch_runners"`
		Feed    distributedjob.IFeedSource  `name:"s3feed" optional:"true"`
	}

	app := fx.New(
		fx.NopLogger,
		// root: lo store e un runner su file registrato dall'app (runner.RegisterFile).
		fx.Provide(func() store.IWorkItemStore { return &storeFake{} }),
		fx.Provide(fx.Annotate(
			func() *taskrunner.FileTaskRunner { return taskrunner.NewFile("import", &fileRunnerSpy{}) },
			fx.ResultTags(`group:"`+taskrunner.FileGroup+`"`))),
		fx.Module("batch-s3",
			fx.Supply(cfg, fx.Private),
			fx.Supply(tasks, fx.Private),
			fx.Provide(provideRegistry, fx.Private),
			fx.Provide(newFeeder, fx.Private),
			fx.Provide(provideFeeder),
			fx.Provide(wrappedRunnersProvide()),
			fx.Provide(feedSourceProvide()),
			fx.Provide(fx.Annotate(registerFeedS3, fx.ResultTags(`group:"`+scheduler.JobGroup+`"`))),
		),
		fx.Module("batch",
			fx.Invoke(func(p inner) {
				seen.jobs, seen.runners, hasFeed = len(p.Jobs), len(p.Runners), p.Feed != nil
			}),
		),
		fx.Populate(&feeder),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("wiring: %v", err)
	}
	if feeder == nil {
		t.Fatal("il Feeder non è iniettabile a root")
	}
	if seen.jobs != 1 || seen.runners != 1 || !hasFeed {
		t.Fatalf("il sottosistema chiuso vede jobs=%d runners=%d feed=%v, attesi 1, 1, true", seen.jobs, seen.runners, hasFeed)
	}

	// Il Registry è un ingranaggio: fuori dal modulo non si vede.
	leak := fx.New(fx.NopLogger,
		fx.Module("batch-s3", fx.Supply(cfg, fx.Private), fx.Provide(provideRegistry, fx.Private)),
		fx.Invoke(func(*registryProbe) {}),
		fx.Provide(func(r registryIn) *registryProbe { return &registryProbe{} }),
	)
	if leak.Err() == nil {
		t.Fatal("il Registry S3 è visibile fuori dal modulo")
	}
}

type registryProbe struct{}

type registryIn struct {
	fx.In
	Reg *s3client.Registry
}
