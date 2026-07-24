package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	redislock "github.com/go-co-op/gocron-redis-lock/v2"
	gocron "github.com/go-co-op/gocron/v2"
	redis "github.com/redis/go-redis/v9"
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
	LC          fx.Lifecycle
	Config      []Config
	RedisClient *redis.Client
	Data        store.IData
	Jobs        []JobRegistration `group:"batch_jobs"`
}

func newScheduler(p schedulerParams) *Scheduler {
	sm := NewSchedulerMetrics()
	opts := make([]gocron.SchedulerOption, 0)
	logger := gocron.NewLogger(-1)
	locker, err := redislock.NewRedisLocker(p.RedisClient, redislock.WithTries(1))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to redis locker")
	}
	opts = append(opts, gocron.WithDistributedLocker(locker), gocron.WithMonitor(sm), gocron.WithLogger(logger))

	scheduler, err := gocron.NewScheduler(opts...)
	if err != nil {
		log.Fatal().Err(err).Msg("Init scheduler")
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
			log.Error().Msgf("Job Type '%s' not found", jobConfig.Type)
			continue
		}
		log.Info().Msgf("Building job '%s' - type: %s", jobConfig.Name, jobConfig.Type)
		jobOptions := makeOptions(jobConfig, locker)
		j, err := scheduler.NewJob(gocron.CronJob(jobConfig.ScheduledCron, true), runjob(jobConfig.Name, &s, jobConfig), jobOptions...)
		if err != nil {
			log.Fatal().Err(err).Msgf("Build job '%s' failed", jobConfig.Name)
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

	return &Scheduler{scheduler}
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

func getJobId(jobName string) string {
	return fmt.Sprintf("%s-%s", jobName, time.Now().Format("20060102150405"))
}
