package scheduler

import (
	"context"
	"fmt"
	"time"

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

func newScheduler(lc fx.Lifecycle, config []Config, redisClient *redis.Client, s Services) *Scheduler {
	sm := NewSchedulerMetrics()
	opts := make([]gocron.SchedulerOption, 0)
	logger := gocron.NewLogger(-1)
	locker, err := redislock.NewRedisLocker(redisClient, redislock.WithTries(1))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to redis locker")
	}
	opts = append(opts, gocron.WithDistributedLocker(locker), gocron.WithMonitor(sm), gocron.WithLogger(logger))

	scheduler, err := gocron.NewScheduler(opts...)
	if err != nil {
		log.Fatal().Err(err).Msg("Init scheduler")
	}

	for _, jobConfig := range config {
		if jobConfig.Disabled {
			continue
		}
		runjob, ok := Jobs[jobConfig.Type]
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

	lc.Append(fx.Hook{
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
