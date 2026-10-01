package scheduler

import (
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
)

// Property INFRASTRUTTURALI comuni a più job type. Stanno qui, e non una volta per package, perché
// sono la stessa chiave dello stesso YAML: `task` era dichiarata in distributedjob, simplejob e
// feedjob, `limit` in distributedjob, simplejob e purgejob, e in batch.ActiveSet compariva pure
// come stringa nuda. Tre costanti per una chiave sono tre posti da cui può divergere.
//
// Restano locali al proprio package le property che un solo job type conosce (`older-than`,
// `task-logs`, `topic`, `stream`, `objectId`, `payload`, …).
//
// NOTA: qui c'erano anche `destination` e `objectType`, i due filtri di claim facoltativi che solo
// NotificationKafka valorizzava — e che quel job chiamava `object` mentre FeedTask li chiamava
// `objectType`, due nomi in YAML per la stessa colonna. Sono spariti insieme ai campi
// WorkItem.Destination/ObjectType: la coda si nomina col solo `task` (per NotificationKafka,
// `stream`), che è anche ciò su cui la deduplica di InsertIfNotActive si basa.
const (
	// PropTask nomina l'istanza di task su cui il job lavora: una voce di `tasks:`. È anche il
	// WorkItem.TaskName degli item che il job claima.
	PropTask = "task"
	// PropLimit è il tetto al lavoro che un tick prende in carico.
	PropLimit = "limit"
	// PropBacklogMetrics abilita le gauge di coda su un job claim-based. Di default sono spente:
	// sono una query in più per tick, e la paga chi la vuole.
	PropBacklogMetrics = "backlog-metrics"
)

// Props legge le property infrastrutturali di un job applicando UNA regola — presente, non vuota,
// del tipo e del segno giusti — e producendo errori che nominano job, job type, property e il
// motivo per cui quella property serve.
//
// Esiste perché la stessa regola era riscritta in ognuno dei cinque job type, e le cinque copie
// erano già divergite: simplejob ritornava un `fmt.Errorf` (quindi senza il codice
// BATCH-JOB-PROPS che gli altri quattro allegavano), feedjob validava dentro il tick invece che
// alla costruzione, e lo stesso `limit` era obbligatorio in un job, con default 100 in un altro e
// 1000 in un terzo. La regola è una; i default, che sono davvero specifici, restano al chiamante.
type Props struct {
	job string
	typ string
	p   properties.Properties
}

// JobProps lega le property di una voce di `jobs:` al nome e al type del job, che sono ciò con cui
// si nomina il colpevole in un messaggio d'errore.
func JobProps(name string, config Config) Props {
	return Props{job: name, typ: config.Type, p: config.Properties}
}

// Has dice se la property è stata scritta (case-insensitive, come tutti i getter di properties.Properties).
func (j Props) Has(key string) bool { return j.p.Has(key) }

// String legge una property facoltativa.
func (j Props) String(key, def string) string { return j.p.GetString(key, def) }

// Bool legge una property booleana facoltativa.
func (j Props) Bool(key string, def bool) bool { return j.p.GetBool(key, def) }

// RequiredString pretende una property valorizzata. `perche` dice a cosa serve, ed è ciò che
// distingue un messaggio utile da «property mancante»: finisce nel testo dell'errore.
func (j Props) RequiredString(key, perche string) (string, error) {
	if !j.p.Has(key) {
		return "", j.mancante(key, perche)
	}
	v := j.p.GetString(key, "")
	if v == "" {
		return "", j.errore("la property %q è vuota", key)
	}
	return v, nil
}

// RequiredPositiveInt pretende un intero positivo.
func (j Props) RequiredPositiveInt(key, perche string) (int, error) {
	if !j.p.Has(key) {
		return 0, j.mancante(key, perche)
	}
	return j.PositiveInt(key, 0)
}

// PositiveInt legge un intero positivo con un default; una property scritta ma non convertibile
// (o non positiva) è un errore e NON ricade sul default: un refuso deve fermare l'avvio, non
// cambiare in silenzio quanto lavoro fa un tick.
func (j Props) PositiveInt(key string, def int) (int, error) {
	if !j.p.Has(key) {
		return def, nil
	}
	n := j.p.GetInt(key, 0)
	if n <= 0 {
		return 0, j.errore("la property %q non è un intero positivo: %v", key, j.p[key])
	}
	return n, nil
}

// RequiredPositiveDuration pretende una durata positiva (es. `168h`).
func (j Props) RequiredPositiveDuration(key, perche string) (time.Duration, error) {
	if !j.p.Has(key) {
		return 0, j.mancante(key, perche)
	}
	d := j.p.GetDuration(key, 0)
	if d <= 0 {
		return 0, j.errore("la property %q non è una durata positiva: %v", key, j.p[key])
	}
	return d, nil
}

// Invalid è l'errore per una property che il chiamante ha letto da sé e ha trovato incoerente —
// un riferimento che non risolve, una combinazione impossibile — così anche quei casi portano il
// codice BATCH-JOB-PROPS e nominano il job allo stesso modo.
func (j Props) Invalid(format string, args ...any) error { return j.errore(format, args...) }

func (j Props) mancante(key, perche string) error {
	return j.errore("property %q mancante: %s", key, perche)
}

func (j Props) errore(format string, args ...any) error {
	return errs.Tech(errs.CodeJobProperties).WithMessage(
		fmt.Sprintf("job %q (type %q): ", j.job, j.typ) + fmt.Sprintf(format, args...))
}
