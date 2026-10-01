package scheduler

import (
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

func TestResolveTimeouts(t *testing.T) {
	t.Run("LockTimeout non impostato → default separati", func(t *testing.T) {
		run, orphan := Config{}.ResolveTimeouts()
		if run != DefaultRunTimeout {
			t.Fatalf("run = %v, want %v", run, DefaultRunTimeout)
		}
		if orphan != DefaultOrphanTimeout {
			t.Fatalf("orphan = %v, want %v", orphan, DefaultOrphanTimeout)
		}
	})

	t.Run("LockTimeout impostato → governa entrambi", func(t *testing.T) {
		run, orphan := Config{LockTimeout: 2 * time.Minute}.ResolveTimeouts()
		if run != 2*time.Minute || orphan != 2*time.Minute {
			t.Fatalf("(run, orphan) = (%v, %v), want (2m, 2m)", run, orphan)
		}
	})
}

// Il nome è la chiave del lock distribuito: due job omonimi si contendono lo stesso lock, e uno dei
// due non gira mai. Un job attivo senza cron non ha quando girare.
func TestCheckJobs(t *testing.T) {
	ok := []Config{{Name: "a", Type: "T", ScheduledCron: "* * * * * *"}, {Name: "b", Type: "T", Disabled: true}}
	if err := CheckJobs(ok); err != nil {
		t.Fatalf("config valida rifiutata: %v", err)
	}
	for nome, jobs := range map[string][]Config{
		"nome duplicato": {{Name: "a", ScheduledCron: "* * * * * *"}, {Name: "a", ScheduledCron: "* * * * * *"}},
		"cron mancante":  {{Name: "a"}},
	} {
		if err := CheckJobs(jobs); err == nil {
			t.Errorf("%s: atteso errore", nome)
		}
	}
}

// I tag validate: fermano l'avvio su un job senza nome o type, o con lock-timeout negativo.
func TestConfig_Validate(t *testing.T) {
	for nome, c := range map[string]Config{
		"senza nome":            {Type: "T"},
		"senza type":            {Name: "a"},
		"lock-timeout negativo": {Name: "a", Type: "T", LockTimeout: -time.Second},
	} {
		if err := core.ValidateStruct(c); err == nil {
			t.Errorf("%s: atteso errore di validazione", nome)
		}
	}
	if err := core.ValidateStruct(Config{Name: "a", Type: "T"}); err != nil {
		t.Errorf("config valida rifiutata: %v", err)
	}
}
