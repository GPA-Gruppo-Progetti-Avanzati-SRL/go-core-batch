// Package task contiene la configurazione applicativa dei task batch — la sezione `tasks:` — e il
// registro che, durante il wiring, lega ogni task type registrato alle sue istanze configurate.
//
// È un package foglia (importa solo go-core-app) proprio perché lo usano sia i registratori dei
// runner (scheduler/distributedjob/runner, scheduler/simplejob) sia l'orchestratore batch.Module.
//
// Il modello:
//
//	tasks:                     # istanze di task, con la loro configurazione APPLICATIVA
//	  - name: import-in        # identificativo dell'istanza, referenziato dai job/worker
//	    type: IMPORT           # task type registrato da runner.Register[importRunner]("IMPORT")
//	    properties:
//	      folder: /data/in
//	  - name: import-bulk      # stesso type, properties diverse → istanza distinta
//	    type: IMPORT
//	    properties:
//	      folder: /data/bulk
//
// Da non confondere col blocco `properties:` di un JOB, che è INFRASTRUTTURALE (configura il job
// type: `task`, `limit`, `collection`, `topic`, …) e resta letto dal framework.
//
// La dichiarazione è OBBLIGATORIA: ogni task type registrato deve avere almeno una voce in `tasks:`,
// e ogni task referenziato da jobs:/workers: deve esistere. Le incoerenze fanno fallire l'avvio.
package task

import (
	"fmt"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/rs/zerolog/log"
)

// Config è una voce della sezione `tasks:`: un'istanza di task type, con la sua configurazione
// applicativa. Le Properties sono mappate sui campi `prop:` della struct del runner (core.BindProps).
type Config struct {
	// Name identifica l'istanza ed è ciò che job e worker referenziano; è anche il WorkItem.Type
	// usato da claiming e instradamento. Vuoto = uguale a Type (unica scorciatoia ammessa: la voce
	// va comunque dichiarata).
	Name       string          `yaml:"name" mapstructure:"name" json:"name"`
	Type       string          `yaml:"type" mapstructure:"type" json:"type" validate:"required"`
	Properties core.Properties `yaml:"properties" mapstructure:"properties" json:"properties"`
}

// TaskName ritorna il nome effettivo dell'istanza (Name, o Type se non valorizzato).
func (c Config) TaskName() string {
	if c.Name != "" {
		return c.Name
	}
	return c.Type
}

// ActiveSet è la fotografia della config che Apply mette a disposizione dei registratori.
type ActiveSet struct {
	// Tasks è la sezione `tasks:`.
	Tasks []Config
	// Referenced sono i task name citati ESPLICITAMENTE da jobs:/workers: — la property `task`
	// di un distributedjob, il `workType` di un simplejob, le `tasks` di un worker pool. Sono
	// riferimenti che l'autore della config ha scritto per nome: se non esistono è un typo.
	Referenced []string
	// Implied sono i nomi DEDOTTI dal job type quando nessuna property nomina il task (un
	// simplejob senza `workType` gira il task omonimo). Attivano il task omonimo se dichiarato,
	// ma non pretendono che esista: lo stesso posto è occupato dai job type del framework
	// (NotificationKafka, DistribuiteTask, …), che non nominano alcun task.
	Implied []string
}

// references indica se l'ActiveSet permette di dedurre quali task sono in uso. Se non nomina
// nulla — né esplicitamente né per deduzione — non c'è filtro da applicare: tutto attivo.
func (a ActiveSet) references() bool { return len(a.Referenced) > 0 || len(a.Implied) > 0 }

// active è valido SOLO durante l'esecuzione sincrona di Apply; nil altrimenti. La registrazione
// avviene single-thread, quindi lo stato globale è sicuro.
var active *ActiveSet

// declaredTypes/knownNames sono derivate da active per non riscandire le slice a ogni Instances.
var (
	declaredTypes map[string]bool
	knownNames    map[string]bool
	registered    map[string]bool
	undeclared    []string // task type registrati senza alcuna voce in `tasks:`
)

// Apply esegue register() con l'ActiveSet disponibile a Instances: le RegisterRunner/Register al suo
// interno forniscono a fx solo le istanze dei task effettivamente usati, con le loro properties.
// Chiamata una sola volta da batch.Module.
func Apply(register func(), a ActiveSet) {
	active = &a
	declaredTypes = make(map[string]bool, len(a.Tasks))
	knownNames = make(map[string]bool, len(a.Tasks))
	registered = make(map[string]bool)
	undeclared = nil
	for _, c := range a.Tasks {
		declaredTypes[strings.ToLower(c.Type)] = true
		knownNames[strings.ToLower(c.TaskName())] = true
	}
	defer func() { active, declaredTypes, knownNames, registered, undeclared = nil, nil, nil, nil, nil }()

	register()
	check(a)
}

// InApply indica se siamo dentro la finestra sincrona aperta da Apply.
func InApply() bool { return active != nil }

// Instances ritorna le istanze ATTIVE del task type indicato: le voci di `tasks:` con quel type che
// sono referenziate da un job o da un worker. Un task type senza alcuna voce dichiarata è un errore
// di configurazione, raccolto e segnalato a fine Apply.
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
		name := c.TaskName()
		if !isReferenced(name) {
			log.Info().Str("task", name).Str("type", taskType).
				Msg("batch: task dichiarato ma non referenziato da alcun job/worker: costruzione saltata (dipendenze non istanziate)")
			continue
		}
		out = append(out, Config{Name: name, Type: c.Type, Properties: c.Properties})
	}
	return out
}

// isReferenced indica se il task name è citato da un job o da un worker, per nome o per deduzione
// dal job type. Un ActiveSet che non nomina nulla è "config che non permette di dedurlo": nessun
// filtro, tutto attivo.
func isReferenced(name string) bool {
	if !active.references() {
		return true
	}
	for _, r := range active.Referenced {
		if strings.EqualFold(r, name) {
			return true
		}
	}
	for _, r := range active.Implied {
		if strings.EqualFold(r, name) {
			return true
		}
	}
	return false
}

// check verifica la coerenza fra `tasks:`, i task type registrati e i riferimenti di jobs:/workers:.
// I task vanno SEMPRE dichiarati, quindi un type registrato senza voce e un riferimento a un nome
// inesistente sono errori di configurazione: panic al wiring, l'app non parte (in caso contrario il
// job girerebbe a vuoto, senza mai trovare un runner).
//
// Solo i riferimenti ESPLICITI sono validati: quelli in Implied sono dedotti dal job type, e un job
// type del framework (NotificationKafka, DistribuiteTask, …) non nomina alcun task — pretenderne la
// dichiarazione sarebbe un falso positivo.
//
// Resta un Warn il caso opposto — una voce di `tasks:` il cui type nessuno ha registrato — perché lo
// stesso YAML è condiviso fra i MODE e fra binari diversi: uno scheduler che dispatcha via gRPC non
// registra i runner del worker.
func check(a ActiveSet) {
	var problems []string
	if len(undeclared) > 0 {
		problems = append(problems, fmt.Sprintf("task type registrati ma non dichiarati nella sezione `tasks:`: %s",
			strings.Join(undeclared, ", ")))
	}
	var unknown []string
	for _, r := range a.Referenced {
		if !knownNames[strings.ToLower(r)] {
			unknown = append(unknown, r)
		}
	}
	if len(unknown) > 0 {
		problems = append(problems, fmt.Sprintf("task referenziati da jobs:/workers: ma non dichiarati in `tasks:`: %s",
			strings.Join(unknown, ", ")))
	}
	if len(problems) > 0 {
		panic("batch: " + strings.Join(problems, "; "))
	}

	for _, c := range a.Tasks {
		if !registered[strings.ToLower(c.Type)] {
			log.Warn().Str("task", c.TaskName()).Str("type", c.Type).
				Msg("batch: la sezione `tasks:` dichiara un type che nessun runner ha registrato in questo processo (typo, o runner in un altro binario)")
		}
	}
}
