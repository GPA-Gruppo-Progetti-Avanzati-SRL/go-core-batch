package s3feed

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// feeder implementa s3.Feeder: il feed S3 dei task che dichiarano un blocco `s3:`. Lo usano il job
// FeedS3Task (a cron) e go-core-trigger (su richiesta): è lo stesso giro.
type feeder struct {
	sources map[string]feedSource // per nome del task, minuscolo
	svcs    services
	items   store.IWorkItemStore
}

type feedSource struct {
	task string // il nome come scritto in config: è il WorkItem.TaskName
	src  s3.Source
}

var _ s3.Feeder = (*feeder)(nil)

// newFeeder è il costruttore fx. Valida il blocco `s3:` di ogni task e che il suo servizio sia
// dichiarato in s3.services: un errore fa fallire l'avvio del processo che costruisce il feed (lo
// scheduler, o l'API che lo inietta), invece del primo giro.
func newFeeder(tasks []task.Config, reg *s3client.Registry, items store.IWorkItemStore) (*feeder, error) {
	return buildFeeder(tasks, registryServices{reg}, items)
}

// provideFeeder espone l'implementazione come s3.Feeder, il seam pubblico.
func provideFeeder(f *feeder) s3.Feeder { return f }

func buildFeeder(tasks []task.Config, svcs services, items store.IWorkItemStore) (*feeder, error) {
	f := &feeder{sources: map[string]feedSource{}, svcs: svcs, items: items}
	for _, t := range tasks {
		if t.S3 == nil {
			continue
		}
		if err := t.S3.Validate(); err != nil {
			return nil, fmt.Errorf("task %q: %w", t.Name, err)
		}
		if _, ok := svcs.lookup(t.S3.Service); !ok {
			return nil, fmt.Errorf("task %q: il servizio %q non è dichiarato in s3.services", t.Name, t.S3.Service)
		}
		f.sources[strings.ToLower(t.Name)] = feedSource{task: t.Name, src: *t.S3}
	}
	return f, nil
}

func (f *feeder) source(taskName string) (feedSource, bool) {
	s, ok := f.sources[strings.ToLower(taskName)]
	return s, ok
}

// Sources ritorna i task con un blocco `s3:`, per nome come scritto in config.
func (f *feeder) Sources() map[string]s3.Source {
	out := make(map[string]s3.Source, len(f.sources))
	for _, s := range f.sources {
		out[s.task] = s.src
	}
	return out
}

// Feed esegue un giro del feed del task: elenca, accoda ciò che non è già attivo, conta.
func (f *feeder) Feed(ctx context.Context, taskName string) (s3.FeedResult, *core.Error) {
	fs, ok := f.source(taskName)
	if !ok {
		return s3.FeedResult{}, errs.Tech(errs.CodeS3Feed).WithMessage(
			fmt.Sprintf("feed S3: il task %q non ha un blocco s3: (task con s3: %s)", taskName, f.names()))
	}
	res, err := feedOnce(ctx, f.svcs, f.items, fs.task, fs.src)
	if err != nil {
		if appErr, isApp := errors.AsType[*core.Error](err); isApp {
			return res, appErr
		}
		return res, errs.Tech(errs.CodeS3List).WithCause(err)
	}
	logResult(fs.task, fs.src, res)
	return res, nil
}

// feedOnce è un giro del feed: elenca, accoda ciò che non è già attivo, conta.
func feedOnce(ctx context.Context, svcs services, items store.IWorkItemStore, taskName string, src s3.Source) (s3.FeedResult, error) {
	found, err := scan(ctx, svcs, taskName, src, src.ResolvedLimit())
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

func (f *feeder) names() string {
	if len(f.sources) == 0 {
		return "nessuno"
	}
	return strings.Join(slices.Sorted(maps.Keys(f.Sources())), ", ")
}
