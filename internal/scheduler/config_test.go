package scheduler

import "testing"

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
