package scheduler

import (
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
)

const (
	// DefaultRunTimeout è il timeout del context di un singolo tick quando LockTimeout non è impostato.
	DefaultRunTimeout = 30 * time.Second
	// DefaultOrphanTimeout è l'età oltre la quale un item IN_PROGRESS è considerato orfano
	// (run crashato/scaduto) e ri-claimato da RecoverOrphans, quando LockTimeout non è impostato.
	DefaultOrphanTimeout = 10 * time.Minute
)

type Config struct {
	// Name è anche la chiave del lock distribuito del job (gocron la usa come chiave): per questo è
	// obbligatorio e univoco fra i job (verificato all'avvio da CheckJobs).
	Name          string `mapstructure:"name" validate:"required"`
	Type          string `mapstructure:"type" validate:"required"`
	ScheduledCron string `mapstructure:"cron"`
	Disabled      bool   `mapstructure:"disabled"`
	SingletonMode bool   `mapstructure:"singleton"`
	// LockTimeout is the maximum time an item can stay IN_PROGRESS before being
	// considered orphaned and re-claimed by RecoverOrphans. Configurable per job.
	LockTimeout time.Duration `mapstructure:"lock-timeout" validate:"gte=0"`
	// Properties è la configurazione INFRASTRUTTURALE del job type (`task`, `limit`, `collection`,
	// `topic`, `taskName`, …): la legge il framework, non l'applicazione. La configurazione
	// applicativa del runner sta nella sezione `tasks:` (vedi package task).
	//
	// NB: viper abbassa le chiavi della config, quindi le letture passano dai getter
	// case-insensitive di properties.Properties e non dall'indicizzazione diretta.
	Properties properties.Properties `mapstructure:"properties"`
}

// CheckJobs verifica i vincoli che i tag `validate:` non sanno esprimere, perché riguardano la lista
// e non la singola voce: i nomi sono univoci — due job con lo stesso nome condividono la chiave del
// lock distribuito, quindi a ogni tick uno dei due trova il lock preso e non gira mai — e un job
// attivo ha un `cron`.
func CheckJobs(jobs []Config) error {
	seen := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		if seen[j.Name] {
			return fmt.Errorf("job %q dichiarato due volte: il nome è la chiave del lock distribuito, e uno dei due non girerebbe mai", j.Name)
		}
		seen[j.Name] = true
		if !j.Disabled && j.ScheduledCron == "" {
			return fmt.Errorf("job %q: cron obbligatorio per un job attivo (disabled: true per spegnerlo)", j.Name)
		}
	}
	return nil
}

// ResolveTimeouts deriva, con convenzione UNICA per tutte le famiglie di job (distributedjob,
// simplejob, kafkajob), il timeout del context di run e l'età di orphan. LockTimeout (se > 0)
// governa entrambi; altrimenti si usano i default (run breve, orphan lungo).
func (c Config) ResolveTimeouts() (run, orphan time.Duration) {
	run, orphan = DefaultRunTimeout, DefaultOrphanTimeout
	if c.LockTimeout > 0 {
		run = c.LockTimeout
		orphan = c.LockTimeout
	}
	return
}
