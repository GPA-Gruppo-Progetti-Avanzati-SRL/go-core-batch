package batch

import (
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/feedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpcdispatcher"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpchandler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/localdispatcher"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/purgejob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/simplejob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskreg"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	corelock "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-locker"
)

// ModuleFunc è la firma dei Module() che l'app passa a batch per riferimento diretto: lo store
// (WithStore) e i backend che portano dipendenze pesanti (WithModule). Sono modes-only
// `func(modes ...string)`: il config non è un parametro, lo fornisce batch con core.Supply della
// Config unificata.
//
// È questo il pattern che preserva la modularità COMPILE-TIME: batch importa i soli componenti che
// non aggiungono dipendenze (job type, dispatcher, worker pool — e li wira da sé, leggendo i
// `jobs:`), mentre i backend li importa l'app, che trascina in go.mod solo ciò che passa. Un'app
// mongo-only non si porta dietro uptrace/bun, una senza Kafka non si porta dietro go-core-kafka,
// una senza S3 non si porta dietro l'SDK AWS.
type ModuleFunc func(modes ...string)

// options raccoglie le scelte non derivabili dalla Config: i modes con cui gate-are le due famiglie
// (scheduler vs worker), lo store e il lock (sempre attivi) e i backend pesanti iniettati dall'app.
type options struct {
	schedulerModes []string
	workerModes    []string
	store          ModuleFunc          // obbligatorio, sempre attivo
	locker         corelock.ModuleFunc // obbligatorio, sempre attivo
	modules        []ModuleFunc        // backend pesanti, gate-ati sui scheduler modes
}

// Option configura Module.
type Option func(*options)

// WithSchedulerModes limita lo Scheduler, i job type, il dispatcher e i backend di WithModule ai
// core.Mode indicati. Vuoto = sempre attivi.
func WithSchedulerModes(modes ...string) Option {
	return func(o *options) { o.schedulerModes = modes }
}

// WithWorkerModes limita il worker pool gRPC (wirato da batch se `workers:` e `grpc.server` sono
// valorizzati) ai core.Mode indicati. Vuoto = sempre attivo. Tipicamente diverso dai scheduler modes
// (es. Worker vs Scheduler).
func WithWorkerModes(modes ...string) Option {
	return func(o *options) { o.workerModes = modes }
}

// WithStore inietta il backend store (store.IData + store.IWorkItemStore). È OBBLIGATORIO ed è
// wirato SEMPRE (nessun mode gate): il WorkItem lifecycle serve sia allo scheduler che al worker.
// Passa storemongo.Module o storesql.Module per riferimento.
//
//	batch.WithStore(storemongo.Module)   // l'app importa SOLO storemongo → niente bun
func WithStore(m ModuleFunc) Option {
	return func(o *options) { o.store = m }
}

// WithLocker inietta il backend del lock distribuito. È OBBLIGATORIA: senza un lock condiviso fra
// le repliche N istanze eseguirebbero lo stesso tick cron contemporaneamente. (Il lock è
// dispatch-dedup, non correttezza — quella la garantisce il claiming sul database — ma è ciò che
// evita di dispatchare N volte lo stesso lavoro.)
//
// A wirare go-core-locker è batch.Module: l'applicazione NON chiama più corelock.Module, e passa
// solo il backend, per riferimento diretto come ogni altro Module. Il suo go.mod elencherà
// soltanto quello — un'app mongo-only non si porta dietro bun né Redis.
//
//	batch.WithLocker(lockmongo.Module)    // go-core-locker/mongostore
//	batch.WithLocker(locksql.Module)      // go-core-locker/sqlstore
//	batch.WithLocker(lockredis.Module)    // go-core-locker/redisstore
//	batch.WithLocker(lockmem.Module)      // in-process: legittimo a REPLICA SINGOLA, e solo lì
//
// La config del lock è cfg.Lock (sezione `lock:`), e il Locker prodotto è fornito a ROOT: resta
// quindi iniettabile dall'applicazione per le proprie sezioni critiche, esattamente come prima.
// Per la stessa ragione un'app che wira il batch non deve chiamare corelock.Module da sé: sarebbe
// un secondo provider dello stesso tipo.
func WithLocker(m corelock.ModuleFunc) Option {
	return func(o *options) { o.locker = m }
}

// WithModule aggiunge i backend che portano dipendenze pesanti, gate-ati sui scheduler modes. Sono
// i soli componenti dello scheduler che l'app sceglie: job type, dispatcher e worker pool li wira
// batch dai `jobs:` (vedi Module). Chiamabile più volte (accumula).
//
//	batch.WithModule(
//	    djmongo.Module,   // query store del job DistribuiteTaskByQuery (o djsql.Module)
//	    s3feed.Module,    // job DistribuiteTaskByS3File (SDK AWS)
//	    kafkajob.Module,  // job NotificationKafka (go-core-kafka; il producer lo wira l'app)
//	)
func WithModule(m ...ModuleFunc) Option {
	return func(o *options) { o.modules = append(o.modules, m...) }
}

// topology è ciò che batch deduce dalla Config: quali job type sono in uso (e quindi quali
// componenti wirare) e dove si eseguono i task — cioè quali runner servono in questo processo.
type topology struct {
	jobTypes map[string]bool // job type dei job attivi (non disabled)
	// grpcDispatch: i DistribuiteTask* consegnano a un worker remoto (grpc.client.url valorizzato)
	// invece di eseguire in-process col localdispatcher.
	grpcDispatch bool
	// workerPool: questo deployment serve i `workers:` col worker pool gRPC (grpc.server.port).
	workerPool bool
}

func newTopology(cfg *Config) topology {
	t := topology{
		jobTypes:     make(map[string]bool),
		grpcDispatch: cfg.Grpc.Client.Url != "",
		workerPool:   len(cfg.WorkersConfig) > 0 && cfg.Grpc.Server.Port != 0,
	}
	for _, j := range cfg.JobsConfig {
		if !j.Disabled {
			t.jobTypes[j.Type] = true
		}
	}
	return t
}

func (t topology) distributed() bool {
	return t.jobTypes[distributedjob.JobType] || t.jobTypes[distributedjob.JobTypeByQuery] ||
		t.jobTypes[distributedjob.JobTypeByS3File]
}

// executesLocally dice se un job di quel type esegue il proprio `task` in QUESTO processo:
// SingleTask sempre, i DistribuiteTask* solo col dispatch in-process. FeedTask accoda e non esegue,
// PurgeWorkItems e NotificationKafka un task non ce l'hanno.
func (t topology) executesLocally(jobType string) bool {
	switch jobType {
	case simplejob.JobType:
		return true
	case distributedjob.JobType, distributedjob.JobTypeByQuery, distributedjob.JobTypeByS3File:
		return !t.grpcDispatch
	}
	return false
}

// wire registra i componenti che la Config usa, e solo quelli. Sono i package che non aggiungono
// dipendenze (misurato con go list -deps: i job type e il localdispatcher nessuna, grpcdispatcher e
// worker pool otelgrpc + protobuf, con grpc-go già presente via go-core-app), quindi importarli qui
// non costa nulla all'app; i backend pesanti restano di WithModule.
func (t topology) wire(sched, work []string) {
	if t.jobTypes[simplejob.JobType] {
		simplejob.Module(sched...)
	}
	if t.jobTypes[feedjob.JobType] {
		feedjob.Module(sched...)
	}
	if t.jobTypes[purgejob.JobType] {
		purgejob.Module(sched...)
	}
	if t.distributed() {
		if t.grpcDispatch {
			grpcdispatcher.Module(sched...)
		} else {
			localdispatcher.Module(sched...)
		}
		if t.jobTypes[distributedjob.JobType] {
			scheduler.ProvideJob(distributedjob.Register, sched...)
		}
		if t.jobTypes[distributedjob.JobTypeByQuery] {
			// Il query store (IQueryStore) è un backend dell'app: djmongo/djsql via WithModule.
			scheduler.ProvideJob(distributedjob.RegisterByQuery, sched...)
		}
		// DistribuiteTaskByS3File lo registra s3feed, che porta l'SDK AWS e resta di WithModule.
	}
	if t.workerPool {
		grpchandler.Module(work...)
	}
}

// activeSet costruisce la fotografia della config che il registro dei task usa durante register():
//
//   - Referenced: i task NOMINATI da `jobs:` (property `task`) e da `workers:` — validati: un nome
//     che non esiste in `tasks:` è un typo e ferma l'avvio, in qualunque processo;
//   - Executed: i task ESEGUITI in questo processo — il `task` dei job che eseguono in linea
//     (executesLocally) se siamo in uno scheduler mode, i `workers[].tasks` se siamo in un worker
//     mode e il worker pool è wirato. Solo questi diventano runner: un task nominato da un FeedTask,
//     o consegnato via gRPC a un worker remoto, non porta le sue dipendenze nel grafo di qui.
func activeSet(cfg *Config, t topology, sched, work []string) taskreg.ActiveSet {
	var referenced, executed []string
	seenRef, seenExe := map[string]bool{}, map[string]bool{}
	add := func(dst *[]string, seen map[string]bool, s string) {
		if s == "" || seen[strings.ToLower(s)] {
			return
		}
		seen[strings.ToLower(s)] = true
		*dst = append(*dst, s)
	}
	inSched, inWork := core.IsMode(sched...), core.IsMode(work...)
	for _, j := range cfg.JobsConfig {
		if j.Disabled {
			continue
		}
		named := j.Properties.GetString(scheduler.PropTask, "")
		add(&referenced, seenRef, named)
		if inSched && t.executesLocally(j.Type) {
			add(&executed, seenExe, named)
		}
	}
	for _, w := range cfg.WorkersConfig {
		for _, name := range w.Tasks {
			add(&referenced, seenRef, name)
			if inWork && t.workerPool {
				add(&executed, seenExe, name)
			}
		}
	}
	return taskreg.ActiveSet{Tasks: cfg.TasksConfig, Referenced: referenced, Executed: executed}
}

// Module wira il sottosistema batch a partire da una singola Config. L'app passa i suoi task
// (register), lo store e il lock (obbligatori) e i soli backend che portano dipendenze pesanti
// (WithModule): il resto lo DEDUCE dalla Config.
//
// Cosa si wira lo dicono i `jobs:`. I job type sono della libreria, e quelli in uso decidono i
// componenti (topology.wire): SingleTask, FeedTask e PurgeWorkItems se compaiono; per i
// DistribuiteTask* il dispatcher in-process, o quello gRPC se `grpc.client.url` è valorizzato; il
// worker pool gRPC se ci sono `workers:` e `grpc.server.port`. Un job type del cui componente
// manca il backend (DistribuiteTaskByS3File senza s3feed, NotificationKafka senza kafkajob) ferma
// l'avvio con "type non registrato".
//
// Quali runner si costruiscono lo dice dove si eseguono i task (activeSet): solo quelli che un job
// o un worker di QUESTO processo esegue. Un task nominato da un FeedTask, o consegnato via gRPC a un
// worker remoto, non entra nel grafo con le sue dipendenze. I modes di runner.Register restano un
// filtro in più, che può solo spegnere (vedi runner.Register).
//
// Config: batch fornisce a fx i sotto-config della Config unificata via core.Supply, SOLO se
// valorizzati — un componente attivo che ne richieda uno non impostato fa fallire fx con un chiaro
// "missing dependency" invece di girare con valori vuoti. Il lock distribuito lo wira batch da
// cfg.Lock e dal backend di WithLocker, e lo fornisce a root: l'app non chiama corelock.Module.
//
// Gating: Scheduler, job type, dispatcher e backend di WithModule girano sui scheduler modes, il
// worker pool sui worker modes. Store e lock sono wirati sempre (servono a entrambi i lati).
//
// Ordine di registrazione: indifferente. I job type confluiscono nel value group batch_jobs, che
// newScheduler consuma per intero.
//
// Registrazione dei runner: si passa la funzione register, come in corekafka.Module — dentro, le
// runner.Register[T] (e runner.RegisterFile[T] per i runner su file) vedono la config e istanziano
// un runner per ogni task eseguito qui, con le sue properties (sezione `tasks:`, obbligatoria).
// register è nil solo per un'app che non registra task runner; registrare fuori da questa finestra
// fa panicare, perché lì la sezione `tasks:` non è nota.
//
// Resta a carico dell'app la fornitura del driver DB (coremongo.Module / coresql.Module).
func Module(cfg *Config, register func(), opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.store == nil {
		panic("batch.Module: WithStore è obbligatorio (store.IData/IWorkItemStore serve a scheduler e worker)")
	}
	if o.locker == nil {
		panic("batch.Module: WithLocker è obbligatorio — senza un lock condiviso fra le repliche N " +
			"istanze eseguono lo stesso tick cron insieme. Scegliere fra batch.WithLocker(mongostore.Module), " +
			"(sqlstore.Module), (redisstore.Module) di go-core-locker, o (memstore.Module) per una replica sola")
	}
	sched := o.schedulerModes
	work := o.workerModes
	topo := newTopology(cfg)
	// Un worker pool senza server gRPC non riceverebbe mai nulla: i `workers:` sono serviti solo dal
	// worker pool gRPC. Fermato solo dove i worker girano, perché lo stesso YAML alimenta anche lo
	// scheduler, che i `workers:` non li legge.
	if len(cfg.WorkersConfig) > 0 && cfg.Grpc.Server.Port == 0 && core.IsMode(work...) {
		panic("batch.Module: la sezione `workers:` richiede `grpc.server.port` — il worker pool riceve i task via gRPC")
	}

	// Registrazione dei task runner con la config già nota (gemello di corekafka.Module): dentro
	// register() le Register/RegisterFile vedono la sezione `tasks:` e forniscono a fx una istanza
	// per ogni task eseguito in questo processo, con le sue properties. Fatto fuori
	// dallo scope core.Module("batch") perché i runner sono sempre stati forniti a root e il value
	// group aggrega comunque root + modulo. register nil solo se l'app non registra task runner:
	// registrare fuori da questa finestra (es. in un init()) è un errore, perché lì la sezione
	// `tasks:` non è nota.
	//
	// Gate-ata come tutto il resto: in un mode che non è né scheduler né worker (es. API) nessun
	// runner verrebbe costruito, quindi non si registra e — soprattutto — non si valida la
	// coerenza di `tasks:`/`jobs:`/`workers:`. Il fail-fast sulla config batch deve far cadere i
	// mode che il batch lo eseguono davvero, non un processo API che del sottosistema usa al più
	// lo store (che resta wirato sempre, vedi sotto).
	if register != nil && batchActive(sched, work) {
		taskreg.Apply(register, activeSet(cfg, topo, sched, work))
	}

	// store.IData + store.IWorkItemStore: consumati sia lato scheduler che lato worker, quindi
	// sempre attivi (nessun mode gate → chiamata senza modes). Registrato a ROOT, fuori dal
	// ModuleClosed: è il SEAM PUBBLICO di batch, l'unico simbolo del sottosistema che l'app
	// consuma davvero (il data layer accoda WorkItem dal lato API). A root resta esportato per
	// l'app e comunque visibile dall'interno del modulo, che ne è discendente.
	o.store()

	// Lock distribuito: wirato SEMPRE e a ROOT come lo store, e per la stessa ragione. Il Locker
	// non è un ingranaggio privato del sottosistema — lo scheduler ne è il consumatore principale,
	// ma l'applicazione lo inietta per le proprie sezioni critiche — quindi non può stare dentro
	// il ModuleClosed, che lo renderebbe invisibile fuori da batch. Senza mode gate perché
	// core.ProvideAs è lazy: in un mode che non lo usa non viene costruito comunque.
	corelock.Module(&cfg.Lock, corelock.WithBackend(o.locker))

	// Livello di dettaglio dei task_logs: fornito a ROOT come lo store, perché è lo store a
	// consumarlo. Validato qui e non dentro l'implementazione: un valore non previsto è un
	// errore di configurazione e deve fermare l'avvio, non degradare in silenzio su "all".
	livello, errLivello := store.ParseTaskLogLevel(cfg.TaskLog)
	if errLivello != nil {
		panic("batch.Module: " + errLivello.Error())
	}
	core.Supply(livello)

	// Tutte le altre registrazioni del sottosistema confluiscono in un core.ModuleClosed("batch"):
	// batch consuma i seam dell'app (gli ITaskRunner) e non le espone nulla in cambio, quindi
	// config dei backend, dispatcher, feed, query store, worker pool e *Scheduler sono privati al
	// modulo (lo store e il Locker no: sono registrati a root, vedi sopra). I runner restano
	// forniti a root: il value group
	// batch_runners li porta dentro (root → discendenti), e batch_jobs aggrega come prima. Il
	// mode-gating resta per-registrazione dentro ogni core.Provide/Supply.
	core.ModuleClosed("batch", func() {
		// Config dei backend: suppliti a fx SOLO se valorizzati. Un config non impostato non viene
		// supplito, così se un componente attivo lo richiede fx fallisce subito con un chiaro
		// "missing dependency" invece di far girare il backend con valori vuoti (fallimento tardivo).
		if cfg.Grpc.Client.Url != "" {
			core.Supply(&cfg.Grpc.Client, sched...)
		}
		if len(cfg.S3.Services) > 0 {
			core.Supply(cfg.S3, sched...)
		}
		if cfg.Grpc.Server.Port != 0 {
			core.Supply(&cfg.Grpc.Server, work...)
		}
		if len(cfg.WorkersConfig) > 0 {
			core.Supply(cfg.WorkersConfig, work...)
		}

		// Componenti che la config usa (job type, dispatcher, worker pool), poi i backend pesanti
		// passati dall'app: gate-ati sched, tranne il worker pool (work).
		topo.wire(sched, work)
		for _, m := range o.modules {
			m(sched...)
		}

		// Scheduler: i job confluiscono nel value group batch_jobs, quindi l'ordine di
		// registrazione è indifferente (vedi nota sull'ordine sopra).
		scheduler.Module(cfg.JobsConfig, sched...)
	})
}

// batchActive indica se in questo processo il sottosistema batch ha qualcosa da costruire: il mode
// corrente è tra gli scheduler modes o tra i worker modes. Una famiglia con modes vuoti è "sempre
// attiva" (semantica di core.IsMode), quindi rende attivo il batch in ogni mode — così un'app che
// non gate-a nulla si comporta come prima.
func batchActive(sched, work []string) bool {
	return core.IsMode(sched...) || core.IsMode(work...)
}
