package scheduler

import (
	"fmt"

	pub "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
)

// Config è la voce di `jobs:`. Il tipo è pubblico (sta in batch.Config) e vive nel package
// scheduler pubblico; qui è un alias, così la macchina dei job lo nomina come prima.
type Config = pub.Config

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
