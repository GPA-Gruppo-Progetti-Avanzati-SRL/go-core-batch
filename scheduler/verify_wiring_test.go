package scheduler

import (
	"testing"

	gocron "github.com/go-co-op/gocron/v2"
	"go.uber.org/fx"
)

// jobGroupConsumer mirrors the batch_jobs field of schedulerParams: it is what
// newScheduler uses to receive the registered jobs from the fx value group.
type jobGroupConsumer struct {
	fx.In
	Jobs []JobRegistration `group:"batch_jobs"`
}

// TestJobValueGroupOrderIndependent verifica il cuore dell'item 1: le JobRegistration
// fornite via il group tag batch_jobs (come fa scheduler.ProvideJob) sono raccolte da un
// consumer con lo stesso tag ANCHE quando sono fornite a fx DOPO il consumer nella lista di
// opzioni. È la prova che il vecchio vincolo "scheduler per ultimo" (mappa globale letta a
// build time) è rimosso: fx risolve l'intero gruppo prima di iniettarlo. La costruzione dello
// Scheduler da parte di newScheduler richiede un lock.Locker iniettato (backend redis/mongo/sql)
// ed è coperta a livello integrazione; qui si verifica il wiring fx del gruppo.
func TestJobValueGroupOrderIndependent(t *testing.T) {
	var got jobGroupConsumer
	provideJob := func(typ string) fx.Option {
		return fx.Provide(fx.Annotate(
			func() JobRegistration {
				return JobRegistration{Type: typ, Factory: func(name string, s *Services, c Config) gocron.Task {
					return gocron.NewTask(func() error { return nil })
				}}
			},
			fx.ResultTags(`group:"`+JobGroup+`"`),
		))
	}

	app := fx.New(
		fx.NopLogger,
		// Il consumer è dichiarato PRIMA dei provider dei job: ordine che con la vecchia
		// mappa globale avrebbe dato "Job Type not found".
		fx.Invoke(func(c jobGroupConsumer) { got = c }),
		provideJob("JobA"),
		provideJob("JobB"),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("fx graph error: %v", err)
	}
	if len(got.Jobs) != 2 {
		t.Fatalf("expected 2 job registrations from batch_jobs group, got %d", len(got.Jobs))
	}
	seen := map[string]bool{}
	for _, jr := range got.Jobs {
		if jr.Factory == nil {
			t.Fatalf("registration %q has nil factory", jr.Type)
		}
		seen[jr.Type] = true
	}
	if !seen["JobA"] || !seen["JobB"] {
		t.Fatalf("missing registrations, got %v", seen)
	}
}
