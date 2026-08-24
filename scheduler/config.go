package scheduler

import (
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

const (
	// DefaultRunTimeout è il timeout del context di un singolo tick quando LockTimeout non è impostato.
	DefaultRunTimeout = 30 * time.Second
	// DefaultOrphanTimeout è l'età oltre la quale un item IN_PROGRESS è considerato orfano
	// (run crashato/scaduto) e ri-claimato da RecoverOrphans, quando LockTimeout non è impostato.
	DefaultOrphanTimeout = 10 * time.Minute
)

type Config struct {
	Name          string `mapstructure:"name"`
	Type          string `mapstructure:"type"`
	ScheduledCron string `mapstructure:"cron"`
	Disabled      bool   `mapstructure:"disabled"`
	SingletonMode bool   `mapstructure:"singleton"`
	// LockTimeout is the maximum time an item can stay IN_PROGRESS before being
	// considered orphaned and re-claimed by RecoverOrphans. Configurable per job.
	LockTimeout time.Duration `mapstructure:"lock-timeout"`
	// Properties è la configurazione INFRASTRUTTURALE del job type (`task`, `limit`, `collection`,
	// `topic`, `workType`, …): la legge il framework, non l'applicazione. La configurazione
	// applicativa del runner sta nella sezione `tasks:` (vedi package task).
	//
	// NB: viper abbassa le chiavi della config, quindi le letture passano dai getter
	// case-insensitive di core.Properties e non dall'indicizzazione diretta.
	Properties core.Properties `mapstructure:"properties"`
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
