package scheduler

import (
	"context"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/lock"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/gocronlock"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
)

type Scheduler struct {
	gocron.Scheduler
}

var tracer = otel.Tracer("Scheduler")

// schedulerParams raccoglie le dipendenze di newScheduler. Jobs arriva dal value group
// batch_jobs: fx risolve tutti i contributori del gruppo prima di costruire lo scheduler,
// quindi l'ordine di registrazione dei job non è più load-bearing.
type schedulerParams struct {
	fx.In
	LC     fx.Lifecycle
	Config []Config
	Locker lock.Locker
	Data   store.IData
	Jobs   []JobRegistration `group:"batch_jobs"`
}

// newScheduler è un costruttore fx: ritorna un errore invece di log.Fatal, così una config
// malformata (scheduler non inizializzabile, job type sconosciuto, build job fallito) fa fallire
// lo startup dell'app in modo pulito (fail-fast) invece di terminare il processo dalla libreria.
func newScheduler(p schedulerParams) (*Scheduler, error) {
	sm := NewSchedulerMetrics()
	opts := make([]gocron.SchedulerOption, 0)
	logger := gocron.NewLogger(-1)
	// The distributed lock is a dispatch-dedup optimization across replicas, not
	// the correctness mechanism (that is the DB claiming in the job runners). The
	// concrete backend (Redis/Mongo/SQL) is injected as a neutral lock.Locker and
	// adapted to gocron here.
	locker := gocronlock.Adapt(p.Locker)
	opts = append(opts, gocron.WithDistributedLocker(locker), gocron.WithMonitor(sm), gocron.WithLogger(logger))

	scheduler, err := gocron.NewScheduler(opts...)
	if err != nil {
		return nil, fmt.Errorf("init scheduler: %w", err)
	}

	factories := make(map[string]JobFactory, len(p.Jobs))
	for _, jr := range p.Jobs {
		factories[jr.Type] = jr.Factory
	}
	s := Services{Data: p.Data}

	for _, jobConfig := range p.Config {
		if jobConfig.Disabled {
			continue
		}
		runjob, ok := factories[jobConfig.Type]
		if !ok {
			// Fail-fast: una config con un job type sconosciuto non deve far partire l'app con
			// job silenziosamente mancanti (coerente col build-job-failed sotto).
			return nil, fmt.Errorf("job %q: type %q non registrato", jobConfig.Name, jobConfig.Type)
		}
		log.Info().Msgf("Building job '%s' - type: %s", jobConfig.Name, jobConfig.Type)
		jobOptions := makeOptions(jobConfig, locker)
		j, err := scheduler.NewJob(gocron.CronJob(jobConfig.ScheduledCron, true), runjob(jobConfig.Name, &s, jobConfig), jobOptions...)
		if err != nil {
			return nil, fmt.Errorf("build job %q: %w", jobConfig.Name, err)
		}
		log.Info().Msgf("Istanced job %s", j.Name())
	}

	p.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			scheduler.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if errS := scheduler.Shutdown(); errS != nil {
				log.Error().Err(errS).Msg("Shutdown scheduler failed")
				return errS
			}
			return nil
		},
	})

	return &Scheduler{scheduler}, nil
}

func makeOptions(jobConfig Config, locker gocron.Locker) []gocron.JobOption {
	jobOptions := make([]gocron.JobOption, 0)
	jobOptions = append(jobOptions, gocron.WithName(jobConfig.Name))
	if locker != nil {
		jobOptions = append(jobOptions, gocron.WithDistributedJobLocker(locker))
	}
	if jobConfig.SingletonMode {
		jobOptions = append(jobOptions, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}
	return jobOptions
}
