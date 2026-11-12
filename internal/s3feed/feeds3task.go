package s3feed

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	pub "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

// JobTypeFeedS3Task è il `type` da scrivere nella voce di `jobs:`.
const JobTypeFeedS3Task = pub.JobTypeFeedS3Task

// defaultFeedLimit è il tetto ai file accodati per giro quando `limit` non è scritto.
const defaultFeedLimit = 100

// feedSpec è una voce FeedS3Task di `jobs:` risolta: il task su cui accodare, la sorgente e il tetto.
type feedSpec struct {
	job   string
	task  string
	src   source
	limit int
}

// specFrom valida le property di un FeedS3Task. La stessa validazione serve al Check del job (al
// boot dello scheduler) e al Feeder (alla sua costruzione): job e API leggono le stesse property e
// devono rifiutarle allo stesso modo.
func specFrom(name string, config scheduler.Config) (feedSpec, error) {
	p := scheduler.JobProps(name, config)
	taskName, err := p.RequiredString(scheduler.PropTask, "è il task su cui accodare i file")
	if err != nil {
		return feedSpec{}, err
	}
	if _, err := p.RequiredString(pub.PropService, "è il servizio S3 da elencare (una chiave di s3.services)"); err != nil {
		return feedSpec{}, err
	}
	// Obbligatorio: è lo spostamento a togliere dal prefisso un file elaborato. Senza, il giro
	// successivo lo riaccoderebbe, e NOTHINGTODO non arriverebbe mai.
	if _, err := p.RequiredString(pub.PropDestPath, "è dove il file viene spostato dopo l'elaborazione: senza, un file già elaborato verrebbe riaccodato al giro successivo"); err != nil {
		return feedSpec{}, err
	}
	limit, err := p.PositiveInt(scheduler.PropLimit, defaultFeedLimit)
	if err != nil {
		return feedSpec{}, err
	}
	src := sourceFrom(config.Properties)
	if err := checkPattern(src.Pattern); err != nil {
		return feedSpec{}, p.Invalid("%v", err)
	}
	return feedSpec{job: name, task: taskName, src: src, limit: limit}, nil
}

// feedOnce è un giro del feed: elenca, accoda ciò che non è già attivo, conta. È il corpo comune
// del tick di FeedS3Task e di Feeder.Feed.
func feedOnce(ctx context.Context, svcs services, items store.IWorkItemStore, spec feedSpec) (s3.FeedResult, error) {
	found, err := scan(ctx, svcs, spec.task, spec.src, spec.limit)
	if err != nil {
		return s3.FeedResult{}, err
	}
	if len(found) == 0 {
		return s3.FeedResult{}, nil
	}
	// I file già in coda o in lavorazione li scarta l'indice unico su (task, objectId): è ciò che
	// rende ripetibile il giro, e ciò che distingue INPROGRESS da QUEUED.
	queued, appErr := items.InsertIfNotActive(ctx, found)
	if appErr != nil {
		return s3.FeedResult{Found: len(found)}, appErr
	}
	return s3.FeedResult{Found: len(found), Queued: queued}, nil
}

// checkService verifica che il servizio della sorgente sia configurato in `s3.services`.
func checkService(name string, config scheduler.Config, spec feedSpec, svcs services) error {
	if _, ok := svcs.lookup(spec.src.Service); !ok {
		return scheduler.JobProps(name, config).Invalid("il servizio %q non è dichiarato in s3.services", spec.src.Service)
	}
	return nil
}

// registerFeedS3 costruisce la JobRegistration di FeedS3Task. È un costruttore fx: il risultato
// confluisce nel value group batch_jobs via scheduler.ProvideJob.
func registerFeedS3(items store.IWorkItemStore, reg *s3client.Registry) scheduler.JobRegistration {
	return feedS3Registration(items, registryServices{reg})
}

func feedS3Registration(items store.IWorkItemStore, svcs services) scheduler.JobRegistration {
	return scheduler.JobRegistration{
		Type: JobTypeFeedS3Task,
		Factory: func(name string, config scheduler.Config) gocron.Task {
			return scheduler.LabeledTask(name, config.Type, func() error {
				return runFeed(name, config, svcs, items)
			})
		},
		Check: func(name string, config scheduler.Config) error {
			spec, err := specFrom(name, config)
			if err != nil {
				return err
			}
			return checkService(name, config, spec, svcs)
		},
	}
}

func runFeed(name string, config scheduler.Config, svcs services, items store.IWorkItemStore) error {
	spec, err := specFrom(name, config)
	if err != nil {
		return err
	}
	// Convenzione unica (scheduler.Config.ResolveTimeouts): il timeout del tick è quello del job.
	timeout, _ := config.ResolveTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := feedOnce(ctx, svcs, items, spec)
	if err != nil {
		log.Error().Err(err).Str("job", name).Str("task", spec.task).Msg("feed S3 fallito")
		return err
	}
	logResult(name, spec, res)
	return nil
}

// logResult registra l'esito di un giro, a Info se ha accodato e a Debug se non c'era nulla: un
// feed a cron fitto su un bucket vuoto non deve riempire i log.
func logResult(job string, spec feedSpec, res s3.FeedResult) {
	ev := log.Info()
	if res.Outcome() == s3.NothingToDo {
		ev = log.Debug()
	}
	ev.Str("job", job).Str("task", spec.task).Str("path", spec.src.Path).
		Int("found", res.Found).Int("queued", res.Queued).Str("outcome", string(res.Outcome())).
		Msg("feed S3")
}
