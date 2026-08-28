package batch

import (
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// ModuleFunc è la firma comune di TUTTI i Module() componibili di go-core-batch (store,
// dispatcher, query store, feed, job Kafka, worker pool). Ogni package espone ormai un
// Module() modes-only `func(modes ...string)`: il config non è più un parametro, ma viene
// iniettato da fx. È batch.Module a fornirlo (core.Supply della Config unificata). Così ogni
// Module si passa per RIFERIMENTO DIRETTO — niente closure.
//
// È questo il pattern che preserva la modularità COMPILE-TIME: il package `batch` NON importa
// nessun package di backend (importa solo i loro Config, che sono struct leggere), quindi è
// l'app a importare — e a trascinare in go.mod — solo i backend che effettivamente passa a
// WithModule/WithWorkerModule. Un'app mongo-only non si porta dietro uptrace/bun, una senza
// Kafka non si porta dietro franz-go. (grpc-go resta nelle deps ma arriva da go-core-app/OTLP,
// non dai backend.)
type ModuleFunc func(modes ...string)

// options raccoglie le scelte di topologia non derivabili dalla Config: i modes con cui gate-are
// le due famiglie (scheduler vs worker), lo store (sempre attivo) e i ModuleFunc dei componenti
// iniettati dall'app, divisi per famiglia di modes.
type options struct {
	schedulerModes []string
	workerModes    []string
	store          ModuleFunc   // obbligatorio, sempre attivo
	locker         ModuleFunc   // obbligatorio, gate-ato sui scheduler modes
	modules        []ModuleFunc // gate-ati sui scheduler modes
	workerModules  []ModuleFunc // gate-ati sui worker modes
}

// Option configura Module.
type Option func(*options)

// WithSchedulerModes limita i componenti passati a WithModule (dispatcher, feed, job Kafka,
// query store) e lo Scheduler ai core.Mode indicati. Vuoto = sempre attivi.
func WithSchedulerModes(modes ...string) Option {
	return func(o *options) { o.schedulerModes = modes }
}

// WithWorkerModes limita i componenti passati a WithWorkerModule (worker pool gRPC) ai core.Mode
// indicati. Vuoto = sempre attivo. Tipicamente diverso dai scheduler modes (es. Worker vs Scheduler).
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

// WithLocker inietta il backend del lock distribuito dello scheduler (un lock.Locker).
// È OBBLIGATORIO (come WithStore): rende esplicita la scelta infrastrutturale. Il lock è
// un'ottimizzazione di dispatch-dedup tra repliche, NON la garanzia di correttezza (quella
// è il DB claiming nei runner). Il backend è iniettato per riferimento diretto e gate-ato
// sui scheduler modes; il suo eventuale config è gestito dalla sua lib (non da batch).
//
//	batch.WithLocker(redislocker.Module)   // da go-core-redis/locker (+ redis.Module per il client)
//	batch.WithLocker(mongolocker.Module)   // da go-core-mongo/locker — nessun Redis richiesto
//	batch.WithLocker(sqllocker.Module)     // da go-core-sql/locker
func WithLocker(m ModuleFunc) Option {
	return func(o *options) { o.locker = m }
}

// WithModule aggiunge uno o più componenti lato scheduler, gate-ati sui scheduler modes. I
// Module() si passano per riferimento diretto (sono tutti modes-only); il loro config lo fornisce
// batch via core.Supply della Config unificata. Chiamabile più volte (accumula).
//
//	batch.WithModule(
//	    grpcdispatcher.Module,          // dispatch via gRPC (in-process: localdispatcher.Module)
//	    djmongo.Module, queryfeed.Module, // feed by-query (query store + feed)
//	    kafkajob.Module,                 // job NotificationKafka
//	)
func WithModule(m ...ModuleFunc) Option {
	return func(o *options) { o.modules = append(o.modules, m...) }
}

// WithWorkerModule aggiunge uno o più componenti lato worker, gate-ati sui worker modes.
// Tipicamente il solo grpchandler.Module (worker pool gRPC).
//
//	batch.WithWorkerModule(grpchandler.Module)
func WithWorkerModule(m ...ModuleFunc) Option {
	return func(o *options) { o.workerModules = append(o.workerModules, m...) }
}

// ActiveSet costruisce la fotografia della config che il registro dei task usa durante register():
// le istanze dichiarate in `tasks:` e i task name referenziati dai job e dai worker pool. Un task
// non referenziato non viene istanziato, quindi le sue dipendenze non entrano nel grafo fx.
//
// I riferimenti sono di due specie, perché solo una delle due è un typo se non trova nulla:
//
//   - ESPLICITI — la property `task` di un distributedjob, il `taskName` di un simplejob, le
//     `tasks` di un worker pool: nomi scritti a mano, che devono esistere in `tasks:`.
//   - DEDOTTI — il job type, usato quando nessuna property nomina il task (un simplejob senza
//     `taskName` gira il task omonimo). Qui non si può pretendere l'esistenza: nello stesso campo
//     stanno i job type del framework (NotificationKafka, DistribuiteTask, DistribuiteTaskByQuery,
//     …), che non nominano alcun task. E batch non può nemmeno elencarli per escluderli, visto che
//     non importa i package dei job (è il vincolo di modularità compile-time di ModuleFunc).
func ActiveSet(cfg *Config) task.ActiveSet {
	seen := make(map[string]bool)
	var referenced, implied []string
	add := func(dst *[]string, s string) {
		if s == "" || seen[strings.ToLower(s)] {
			return
		}
		seen[strings.ToLower(s)] = true
		*dst = append(*dst, s)
	}
	for _, j := range cfg.JobsConfig {
		if j.Disabled {
			continue
		}
		named := j.Properties.GetString("task", "") // distributedjob e simplejob
		if named != "" {
			add(&referenced, named)
			continue
		}
		add(&implied, j.Type) // simplejob senza taskName, oppure un job type del framework
	}
	for _, w := range cfg.WorkersConfig {
		for _, t := range w.Tasks {
			add(&referenced, t)
		}
	}
	return task.ActiveSet{Tasks: cfg.TasksConfig, Referenced: referenced, Implied: implied}
}

// Module wira il sottosistema batch a partire da una singola Config, sostituendo la sfilza di
// Module() da chiamare a mano in init(). I backend (store, dispatcher, feed, job Kafka, worker
// pool) sono INIETTATI dall'app come ModuleFunc per riferimento diretto: così `batch` non importa
// nessun package di backend e ogni app trascina in go.mod solo le dipendenze di ciò che passa.
//
// Config: batch fornisce a fx i sotto-config della Config unificata via core.Supply, così i
// Module() dei backend li trovano già iniettati (stesso pattern già usato per la RedisConfig; i
// tipi Config sono leggeri, supplirli non introduce dipendenze pesanti). I config dei backend
// (grpc client/server, kafka, s3, worker) sono suppliti SOLO se valorizzati: un config non
// impostato non viene supplito e, se un componente attivo lo richiede, fx fallisce subito con un
// chiaro "missing dependency" invece di far girare il backend con valori vuoti. Il lock distribuito
// non è più un'eccezione: è un backend iniettato (WithLocker), come store e gli altri.
//
// Gating: i componenti di WithModule e lo Scheduler girano sui scheduler modes; quelli di
// WithWorkerModule sui worker modes. Lo store fa eccezione: è wirato sempre (serve a entrambi i lati).
// È il core.Mode a runtime a decidere cosa viene effettivamente costruito.
//
// Ordine di registrazione: indifferente. I job type confluiscono nel value group batch_jobs
// (scheduler.ProvideJob) e newScheduler li consuma dal gruppo, quindi fx risolve tutti i
// contributori prima di costruire lo Scheduler a prescindere dall'ordine di registrazione.
// (In precedenza lo Scheduler doveva essere registrato per ultimo perché newScheduler leggeva
// la mappa globale scheduler.Jobs al momento della costruzione.)
//
// Registrazione dei runner: si passa la funzione register, come in corekafka.Module — dentro, le
// runner.Register / simplejob.RegisterRunner vedono la config e istanziano un runner per ogni task
// attivo, con le sue properties (sezione `tasks:`, obbligatoria). register è nil solo per un'app che
// non registra task runner; registrare in un init() non è più supportato (panic: lì la config non è
// nota). I costruttori scritti a mano — runner.Provide — restano invece registrabili ovunque.
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
		panic("batch.Module: WithLocker è obbligatorio (lock distribuito dello scheduler: redis/mongo/sql)")
	}
	sched := o.schedulerModes
	work := o.workerModes

	// Registrazione dei task runner con la config già nota (gemello di corekafka.Module): dentro
	// register() le Register/RegisterRunner vedono la sezione `tasks:` e forniscono a fx una istanza
	// per ogni task effettivamente referenziato da jobs:/workers:, con le sue properties. Fatto fuori
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
		task.Apply(register, ActiveSet(cfg))
	}

	// store.IData + store.IWorkItemStore: consumati sia lato scheduler che lato worker, quindi
	// sempre attivi (nessun mode gate → chiamata senza modes). Registrato a ROOT, fuori dal
	// ModuleClosed: è il SEAM PUBBLICO di batch, l'unico simbolo del sottosistema che l'app
	// consuma davvero (il data layer accoda WorkItem dal lato API). A root resta esportato per
	// l'app e comunque visibile dall'interno del modulo, che ne è discendente.
	o.store()

	// Tutte le altre registrazioni del sottosistema confluiscono in un core.ModuleClosed("batch"):
	// batch consuma i seam dell'app (gli ITaskRunner) e non le espone nulla in cambio, quindi
	// config dei backend, locker, dispatcher, feed, query store, worker pool, producer Kafka
	// interno e *Scheduler sono privati al modulo. I runner restano forniti a root: il value group
	// batch_runners li porta dentro (root → discendenti), e batch_jobs aggrega come prima. Il
	// mode-gating resta per-registrazione dentro ogni core.Provide/Supply.
	core.ModuleClosed("batch", func() {
		// Lock distribuito: backend iniettato (redis/mongo/sql), gate-ato sui scheduler modes.
		// Il suo eventuale config è fornito dalla sua lib (es. redis.Module dell'app), non da batch.
		o.locker(sched...)

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

		// Componenti lato scheduler (dispatcher, feed, job Kafka, query store): gate-ati sched.
		for _, m := range o.modules {
			m(sched...)
		}

		// Componenti lato worker (worker pool gRPC): gate-ati work.
		for _, m := range o.workerModules {
			m(work...)
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
