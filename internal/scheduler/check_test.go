package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"

	corelock "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-locker"
	gocron "github.com/go-co-op/gocron/v2"
	"go.uber.org/fx/fxtest"
)

// nopLocker soddisfa il grafo: alla costruzione lo scheduler non acquisisce nulla.
type nopLocker struct{}

func (nopLocker) Acquire(context.Context, string, ...corelock.AcquireOption) (corelock.Handle, error) {
	return nil, corelock.ErrNotAcquired
}

// Un job che in questo processo non può eseguire ciò che nomina deve fermare l'AVVIO: è il
// contratto di JobRegistration.Check. Un job disabilitato non viene controllato, come non viene
// costruito.
func TestNewScheduler_CheckFermaLAvvio(t *testing.T) {
	built := false
	reg := JobRegistration{
		Type: "Local",
		Factory: func(string, Config) gocron.Task {
			built = true
			return gocron.NewTask(func() error { return nil })
		},
		Check: func(string, Config) error { return errors.New("nessun runner per il task \"x\"") },
	}
	job := Config{Name: "j", Type: "Local", ScheduledCron: "0 * * * * *"}

	_, err := newScheduler(schedulerParams{LC: fxtest.NewLifecycle(t), Config: []Config{job}, Locker: nopLocker{}, Jobs: []JobRegistration{reg}})
	if err == nil || !strings.Contains(err.Error(), `job "j"`) || !strings.Contains(err.Error(), `"x"`) {
		t.Fatalf("atteso un errore d'avvio che nomini job e task, ottenuto %v", err)
	}
	if built {
		t.Error("la factory non deve girare per un job che non supera il Check")
	}

	job.Disabled = true
	if _, err := newScheduler(schedulerParams{LC: fxtest.NewLifecycle(t), Config: []Config{job}, Locker: nopLocker{}, Jobs: []JobRegistration{reg}}); err != nil {
		t.Fatalf("un job disabilitato non va controllato: %v", err)
	}
}
