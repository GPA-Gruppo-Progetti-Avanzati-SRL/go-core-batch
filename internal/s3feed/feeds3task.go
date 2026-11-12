package s3feed

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	pub "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

// JobTypeFeedS3Task è il `type` da scrivere nella voce di `jobs:`.
const JobTypeFeedS3Task = pub.JobTypeFeedS3Task

// registerFeedS3 costruisce la JobRegistration di FeedS3Task. È un costruttore fx: il risultato
// confluisce nel value group batch_jobs via scheduler.ProvideJob.
//
// Il job non porta la sorgente: la legge dal blocco `s3:` del suo `task`. Il tick è quindi esattamente
// Feeder.Feed(task) — lo stesso giro che go-core-trigger esegue su richiesta.
func registerFeedS3(f *feeder) scheduler.JobRegistration {
	return scheduler.JobRegistration{
		Type: JobTypeFeedS3Task,
		Factory: func(name string, config scheduler.Config) gocron.Task {
			return scheduler.LabeledTask(name, config.Type, func() error {
				return runFeed(name, config, f)
			})
		},
		Check: func(name string, config scheduler.Config) error {
			_, err := feedTask(name, config, f)
			return err
		},
	}
}

// feedTask risolve il task del job e pretende che abbia un blocco `s3:`.
func feedTask(name string, config scheduler.Config, f *feeder) (string, error) {
	p := scheduler.JobProps(name, config)
	taskName, err := p.RequiredString(scheduler.PropTask, "è il task da alimentare (una voce di tasks: con un blocco s3:)")
	if err != nil {
		return "", err
	}
	if _, ok := f.source(taskName); !ok {
		return "", p.Invalid("il task %q non ha un blocco s3: (o non è dichiarato in tasks:)", taskName)
	}
	return taskName, nil
}

func runFeed(name string, config scheduler.Config, f *feeder) error {
	taskName, err := feedTask(name, config, f)
	if err != nil {
		return err
	}
	// Convenzione unica (scheduler.Config.ResolveTimeouts): il timeout del tick è quello del job.
	timeout, _ := config.ResolveTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, appErr := f.Feed(ctx, taskName); appErr != nil {
		log.Error().Err(appErr).Str("job", name).Str("task", taskName).Msg("feed S3 fallito")
		return appErr
	}
	return nil
}

// logResult registra l'esito di un giro, a Info se ha accodato e a Debug se non c'era nulla: un
// feed a cron fitto su un bucket vuoto non deve riempire i log.
func logResult(taskName string, src s3.Source, res s3.FeedResult) {
	ev := log.Info()
	if res.Outcome() == s3.NothingToDo {
		ev = log.Debug()
	}
	ev.Str("task", taskName).Str("path", src.Path).
		Int("found", res.Found).Int("queued", res.Queued).Str("outcome", string(res.Outcome())).
		Msg("feed S3")
}
