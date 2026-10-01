// Package runner porta il contratto ITaskRunner e la SOLA forma di registrazione di un task
// runner, condivisa da tutte le famiglie di job (simplejob, distributedjob con dispatch
// in-process o gRPC, worker pool) e da ogni trasporto futuro.
//
// La registrazione è UNA: Register[T](taskType) — e RegisterFile[T](taskType) per l'altro
// contratto, quello dei runner su file (S3). Non esiste una seconda forma.
//
// Le vecchie Provide/ProvideFile — un costruttore fx fornito direttamente al value group — sono
// state RIMOSSE, per la stessa ragione per cui go-core-kafka ha tolto ProvideHandler /
// ProvideTransformer: non passando dalla sezione `tasks:` erano l'unica forma che
//
//   - non riceveva le properties applicative dell'istanza né il suo `max-retry`;
//   - non veniva filtrata dai riferimenti di jobs:/workers: (il runner si costruiva, con le sue
//     dipendenze, anche quando nessuno lo eseguiva);
//   - poteva essere chiamata in un init(), cioè FUORI dalla finestra di batch.Module — quindi
//     fuori dal fail-fast che verifica la coerenza fra `tasks:`, `jobs:` e i type registrati.
//
// Tenere in vita due forme con semantiche di gating diverse significava tenere in vita proprio
// l'asimmetria che la registrazione unica toglie. Un runner che ha bisogno di logica di
// costruzione la mette nei campi `inject:`/`prop:` della sua struct, che è ciò che
// core.ProvideStruct sa leggere.
package runner

import (
	"fmt"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskreg"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// ITaskRunner is the single runner contract, shared with simplejob via store.ITaskRunner.
// A runner is interchangeable between distributedjob and simplejob without code changes.
//
// The framework applies the lifecycle from the return value (see store.ApplyResult):
//
//	return nil                                   → MarkDone
//	return store.Retry(d) / RetryWithCause(d, e) → MarkPending (transient, retry later)
//	return err                                   → MarkFailed
//	return store.ErrHandled                      → left untouched (runner finalized it,
//	                                               e.g. MarkDone + child inserts in a TX,
//	                                               using an fx-injected IWorkItemStore)
type ITaskRunner = store.ITaskRunner

// Register registra il tipo struct T come task runner per il task type indicato. T deve implementare
// ITaskRunner (via receiver a puntatore) e dichiarare i suoi campi con i tag di go-core-app:
//
//	`inject:""` / `inject:"nome"` / `from:"gruppo"`  → dipendenza iniettata da fx
//	`prop:"chiave"`                                   → property applicativa del task (sezione `tasks:`)
//	nessun tag                                        → campo di lavorazione, ignorato dal grafo
//
// Viene fornito a fx un runner per ogni ISTANZA attiva del task type, cioè per ogni voce della
// sezione `tasks:` con quel type che un job o un worker referenzia. Ogni istanza riceve le proprie
// properties: due job possono quindi eseguire lo stesso task type con configurazioni diverse.
//
// La dichiarazione in `tasks:` è OBBLIGATORIA: un task type registrato e non dichiarato fa fallire
// l'avvio. Un task dichiarato che nessuno referenzia non viene istanziato: le sue dipendenze non
// entrano nel grafo. L'istanza è condivisa fra le esecuzioni, quindi i campi di lavorazione NON sono
// per-esecuzione.
//
// Va chiamata dentro la funzione di registrazione passata a batch.Module: è lì che la config è nota.
//
// Quali istanze diventano runner lo decide la config: solo quelle eseguite in QUESTO processo, cioè
// nominate da un job che le esegue in linea (SingleTask, DistribuiteTask* col dispatch in-process)
// o servite da un worker pool di qui. modes è un filtro in più, nella stessa forma di
// corekafka.RegisterHandler: limita QUESTO task ai core.Mode indicati (vuoto = nessun limite) e può
// solo spegnere. Spegnere un task che la config esegue qui è una contraddizione e ferma l'avvio
// (vedi activeInstances); un task che la config non esegue qui non viene costruito comunque.
//
//	func Register() { runner.Register[myRunner]("IMPORT", engine.Worker) }
//
//	type myRunner struct {
//	    Svc    myPkg.IService `inject:""`
//	    Folder string         `prop:"folder" validate:"required"`
//	}
//	func (r *myRunner) Run(ctx context.Context, item *store.WorkItem) error { ... }
func Register[T any, PT interface {
	*T
	ITaskRunner
}](taskType string, modes ...string) {
	for _, tc := range activeInstances(taskType, modes) {
		core.ProvideStruct(func(p *T) *taskrunner.TaskRunner {
			return taskrunner.New(tc.Name, PT(p)).WithMaxRetry(tc.ResolveMaxRetry())
		},
			owner(tc.Name, taskType), tc.Properties, taskrunner.Group)
	}
}

// activeInstances ritorna le istanze del task type che questo processo deve costruire: quelle che la
// config fa eseguire qui (taskreg.Instances) E che i modes del register ammettono. I modes sono un
// filtro in più che può solo spegnere — non accende ciò che la config non esegue qui.
//
// taskreg.Instances è chiamata SEMPRE, anche quando il mode esclude il task, e il gate viene DOPO: è
// Instances ad alimentare la contabilità di task.check — marca il type come registrato e accumula i
// type non dichiarati in `tasks:`. Gate-are prima farebbe apparire "type registrato ma non dichiarato"
// un type registrato e solo non attivo in questo MODE. È lo stesso ordine che corekafka applica in
// provideIfActive: prima la config, poi il mode.
//
// Un'istanza che Instances ritorna è eseguita in questo processo da un job o da un worker pool,
// quindi spegnerla coi modes è una contraddizione: ogni item fallirebbe con "no runner registered
// for task name". L'istanza non viene costruita, e l'avvio si ferma qui con un errore che nomina
// task e modes invece di presentarsi item per item.
func activeInstances(taskType string, modes []string) []task.Config {
	all := taskreg.Instances(taskType)
	if core.IsMode(modes...) || len(all) == 0 {
		return all
	}
	names := make([]string, len(all))
	for i, tc := range all {
		names[i] = tc.Name
	}
	panic(fmt.Sprintf("batch: i task %s (type %q) sono eseguiti in questo processo da un job o da un worker pool, "+
		"ma runner.Register li limita ai MODE %v e il MODE corrente è %q: togliere il task dal job/worker di "+
		"questo MODE, oppure aggiungere il MODE al register", strings.Join(names, ", "), taskType, modes, core.Mode))
}

// owner è l'etichetta con cui core.ProvideStruct contestualizza i suoi errori (dipendenza mancante,
// property non valida): senza, fx riporterebbe solo `reflect.makeFuncStub`.
func owner(taskName, taskType string) string {
	return fmt.Sprintf("batch: task %q (type %q)", taskName, taskType)
}

// IFileRunner è il contratto dei runner su file (es. S3): riceve la chiave del file e un io.Reader
// col contenuto. Alias del tipo di internal/taskrunner, che è dove vive il legame runner→task name.
type IFileRunner = taskrunner.IFileRunner

// RegisterFile è l'analogo di Register per i runner su file (es. S3): T deve implementare IFileRunner.
// Vale lo stesso contratto sui tag, la stessa istanziazione per voce della sezione `tasks:` (che va
// dichiarata anche qui), lo stesso tetto `max-retry:` e la stessa semantica dei modes.
//
//	func Register() { runner.RegisterFile[myS3Runner]("S3_IMPORT") }
//
//	type myS3Runner struct {
//	    Svc mysvc.IService `inject:""`
//	}
//	func (r *myS3Runner) Run(ctx context.Context, key string, content io.Reader) error { ... }
func RegisterFile[T any, PT interface {
	*T
	IFileRunner
}](taskType string, modes ...string) {
	for _, tc := range activeInstances(taskType, modes) {
		core.ProvideStruct(func(p *T) *taskrunner.FileTaskRunner {
			return taskrunner.NewFile(tc.Name, PT(p)).WithMaxRetry(tc.ResolveMaxRetry())
		},
			owner(tc.Name, taskType), tc.Properties, taskrunner.FileGroup)
	}
}
