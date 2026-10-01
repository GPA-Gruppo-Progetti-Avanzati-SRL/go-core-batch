// Package taskreg è il registro che, durante il wiring, lega ogni task type registrato con
// runner.Register alle sue istanze della sezione `tasks:`. È un ingranaggio di batch.Module (che
// apre la finestra con Apply) e di runner.Register (che ci legge le istanze con Instances): un'app
// non lo nomina mai.
package taskreg

import (
	"fmt"
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"github.com/rs/zerolog/log"
)

// Config è la voce di `tasks:`, alias del tipo pubblico.
type Config = task.Config

// ActiveSet è la fotografia della config che Apply mette a disposizione dei registratori. La
// costruisce batch.Module, che sa quali job e quali worker pool girano in questo processo.
type ActiveSet struct {
	// Tasks è la sezione `tasks:`.
	Tasks []Config
	// Referenced sono i task name citati da jobs:/workers: — la property `task` di un job, le
	// `tasks` di un worker pool —, in qualunque processo li esegua. Sono nomi scritti a mano: se non
	// esistono in `tasks:` è un typo, e l'avvio si ferma.
	Referenced []string
	// Executed sono i task name eseguiti in QUESTO processo: il `task` dei job che eseguono in linea
	// (SingleTask, DistribuiteTask* col dispatch in-process) se il MODE è di scheduler, le `tasks`
	// dei worker pool se il MODE è di worker. Solo queste istanze diventano runner.
	Executed []string
}

// active è valido SOLO durante l'esecuzione sincrona di Apply; nil altrimenti. La registrazione
// avviene single-thread, quindi lo stato globale è sicuro.
var active *ActiveSet

// declaredTypes/typeOf sono derivate da active per non riscandire le slice a ogni Instances.
var (
	declaredTypes map[string]bool
	typeOf        map[string]string // task name (minuscolo) → type (minuscolo)
	registered    map[string]bool
	undeclared    []string // task type registrati senza alcuna voce in `tasks:`
)

// Apply esegue register() con l'ActiveSet disponibile a Instances: le RegisterRunner/Register al suo
// interno forniscono a fx solo le istanze dei task eseguiti in questo processo, con le loro properties.
// Chiamata una sola volta da batch.Module.
func Apply(register func(), a ActiveSet) {
	checkNames(a)
	active = &a
	declaredTypes = make(map[string]bool, len(a.Tasks))
	typeOf = make(map[string]string, len(a.Tasks))
	registered = make(map[string]bool)
	undeclared = nil
	for _, c := range a.Tasks {
		declaredTypes[strings.ToLower(c.Type)] = true
		typeOf[strings.ToLower(c.Name)] = strings.ToLower(c.Type)
	}
	defer func() { active, declaredTypes, typeOf, registered, undeclared = nil, nil, nil, nil, nil }()

	register()
	check(a)
}

// InApply indica se siamo dentro la finestra sincrona aperta da Apply.
func InApply() bool { return active != nil }

// Instances ritorna le istanze ATTIVE del task type indicato: le voci di `tasks:` con quel type che
// un job o un worker di questo processo esegue (ActiveSet.Executed). Un task type senza alcuna voce
// dichiarata è un errore di configurazione, raccolto e segnalato a fine Apply.
//
// Va chiamata SOLO dentro la funzione di registrazione passata a batch.Module (che apre la finestra
// con Apply): panica altrimenti, perché fuori da lì la sezione `tasks:` non è nota.
func Instances(taskType string) []Config {
	if active == nil {
		panic("batch: task " + taskType + " registrato fuori dalla funzione passata a batch.Module (la sezione `tasks:` non è ancora nota)")
	}
	registered[strings.ToLower(taskType)] = true

	if !declaredTypes[strings.ToLower(taskType)] {
		undeclared = append(undeclared, taskType)
		return nil
	}

	var out []Config
	for _, c := range active.Tasks {
		if !strings.EqualFold(c.Type, taskType) {
			continue
		}
		name := c.Name
		if !active.executes(name) {
			log.Info().Str("task", name).Str("type", taskType).
				Msg("batch: task dichiarato ma non eseguito in questo processo: costruzione saltata (dipendenze non istanziate)")
			continue
		}
		// La voce passa INTERA. Ricostruirla campo per campo significa dimenticarne uno al primo
		// campo nuovo, ed era già successo: MaxRetry non veniva copiato, quindi ResolveMaxRetry
		// vedeva sempre nil e il tetto ai ritentativi di OGNI task valeva illimitato, qualunque
		// cosa dicesse `max-retry:` nello YAML.
		out = append(out, c)
	}
	return out
}

// executes indica se il task name è eseguito in questo processo da un job o da un worker pool.
func (a ActiveSet) executes(name string) bool {
	for _, r := range a.Executed {
		if strings.EqualFold(r, name) {
			return true
		}
	}
	return false
}

// checkNames pretende un `name` su ogni voce di `tasks:`. Il name è la chiave di instradamento —
// lo referenziano i job (`properties.task`) e i worker pool (`workers[].tasks`), viaggia in
// WorkItem.Type, ci filtra il claiming e ci instrada il mux.Runner — quindi una voce senza name non
// è raggiungibile da nessuno. Non c'è fallback sul type: prima c'era, e nascondeva la chiave di
// routing rendendo invisibile il caso di due istanze dello stesso type. Girando prima di register()
// l'errore arriva sulla config, non su un runner costruito a metà.
func checkNames(a ActiveSet) {
	var anonymous []string
	for i, c := range a.Tasks {
		if strings.TrimSpace(c.Name) == "" {
			anonymous = append(anonymous, fmt.Sprintf("voce #%d (type %q)", i+1, c.Type))
		}
	}
	if len(anonymous) > 0 {
		panic("batch: la sezione `tasks:` richiede un `name` su ogni voce (è la chiave referenziata da jobs:/workers: e usata come WorkItem.Type; scrivilo anche quando coincide col `type`). Voci senza name: " +
			strings.Join(anonymous, ", "))
	}
}

// check verifica la coerenza fra `tasks:`, i task type registrati e i riferimenti di jobs:/workers:.
// I task vanno SEMPRE dichiarati, quindi un type registrato senza voce e un riferimento a un nome
// inesistente sono errori di configurazione: panic al wiring, l'app non parte (in caso contrario il
// job girerebbe a vuoto, senza mai trovare un runner).
//
// Un task ESEGUITO qui il cui type nessun runner registra è un errore: un job o un worker di questo
// processo lo eseguirebbe, e ogni item fallirebbe con "no runner registered for task name". Resta
// un Warn invece una voce di `tasks:` dichiarata, non eseguita qui e il cui type nessuno registra,
// perché lo stesso YAML è condiviso fra i MODE e fra binari diversi: uno scheduler che dispatcha
// via gRPC non registra i runner del worker.
func check(a ActiveSet) {
	var problems []string
	if len(undeclared) > 0 {
		problems = append(problems, fmt.Sprintf("task type registrati ma non dichiarati nella sezione `tasks:`: %s",
			strings.Join(undeclared, ", ")))
	}
	var unknown []string
	for _, r := range a.Referenced {
		if _, ok := typeOf[strings.ToLower(r)]; !ok {
			unknown = append(unknown, r)
		}
	}
	if len(unknown) > 0 {
		problems = append(problems, fmt.Sprintf("task referenziati da jobs:/workers: ma non dichiarati in `tasks:`: %s",
			strings.Join(unknown, ", ")))
	}
	var orphan []string
	for _, e := range a.Executed {
		if typ, ok := typeOf[strings.ToLower(e)]; ok && !registered[typ] {
			orphan = append(orphan, fmt.Sprintf("%s (type %s)", e, typ))
		}
	}
	if len(orphan) > 0 {
		problems = append(problems, fmt.Sprintf("task eseguiti in questo processo da un job o da un worker pool, ma senza un runner registrato per il loro type (runner.Register mancante in questo binario): %s",
			strings.Join(orphan, ", ")))
	}
	if len(problems) > 0 {
		panic("batch: " + strings.Join(problems, "; "))
	}

	for _, c := range a.Tasks {
		if !registered[strings.ToLower(c.Type)] && !a.executes(c.Name) {
			log.Warn().Str("task", c.Name).Str("type", c.Type).
				Msg("batch: la sezione `tasks:` dichiara un type che nessun runner ha registrato in questo processo (typo, o runner in un altro binario)")
		}
	}
}
