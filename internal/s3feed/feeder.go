package s3feed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Feeder esegue su richiesta il feed di un job FeedS3Task: lo stesso giro del suo tick — elenco del
// bucket, un work item per file, ciò che è già attivo non si riaccoda — con le property di quella
// voce di `jobs:`. È il seam con cui un'API fa partire un flusso su file: accoda, e a eseguire resta
// il job che serve il task (un SingleTask o un DistribuiteTask che gira ogni tot).
//
// Il riferimento è il nome del job, e non una sorgente passata dal chiamante, perché API e cron
// devono fare la stessa cosa per costruzione: leggono la stessa voce. Un flusso alimentato solo da
// API dichiara comunque la sua voce FeedS3Task, con `disabled: true` — il Feeder la accetta, lo
// scheduler no.
type Feeder interface {
	// Feed esegue un giro del feed del job indicato. L'esito si legge con FeedResult.Outcome: con
	// s3.NothingToDo nel bucket non c'è nulla da elaborare.
	Feed(ctx context.Context, job string) (s3.FeedResult, *core.Error)
}

type feeder struct {
	specs map[string]feedSpec // per nome del job, minuscolo
	svcs  services
	items store.IWorkItemStore
}

// newFeeder è il costruttore fx. Valida TUTTE le voci FeedS3Task, anche quelle disabilitate (che
// esistono proprio per il Feeder): una property sbagliata fa fallire l'avvio del processo che
// inietta il Feeder, invece della prima chiamata dell'API.
func newFeeder(jobs []scheduler.Config, reg *s3client.Registry, items store.IWorkItemStore) (Feeder, error) {
	return buildFeeder(jobs, registryServices{reg}, items)
}

func buildFeeder(jobs []scheduler.Config, svcs services, items store.IWorkItemStore) (*feeder, error) {
	f := &feeder{specs: map[string]feedSpec{}, svcs: svcs, items: items}
	for _, j := range jobs {
		if j.Type != JobTypeFeedS3Task {
			continue
		}
		spec, err := specFrom(j.Name, j)
		if err != nil {
			return nil, err
		}
		if err := checkService(j.Name, j, spec, svcs); err != nil {
			return nil, err
		}
		f.specs[strings.ToLower(j.Name)] = spec
	}
	return f, nil
}

func (f *feeder) Feed(ctx context.Context, job string) (s3.FeedResult, *core.Error) {
	spec, ok := f.specs[strings.ToLower(job)]
	if !ok {
		return s3.FeedResult{}, errs.Tech(errs.CodeS3Feed).WithMessage(
			fmt.Sprintf("feed S3: %q non è un job FeedS3Task della sezione jobs: (dichiarati: %s)", job, f.names()))
	}
	res, err := feedOnce(ctx, f.svcs, f.items, spec)
	if err != nil {
		if appErr, isApp := errors.AsType[*core.Error](err); isApp {
			return res, appErr
		}
		return res, errs.Tech(errs.CodeS3List).WithCause(err)
	}
	logResult(spec.job, spec, res)
	return res, nil
}

func (f *feeder) names() string {
	if len(f.specs) == 0 {
		return "nessuno"
	}
	out := make([]string, 0, len(f.specs))
	for _, s := range f.specs {
		out = append(out, s.job)
	}
	slices.Sort(out)
	return strings.Join(out, ", ")
}
