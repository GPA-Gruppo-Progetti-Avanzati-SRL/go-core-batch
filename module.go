package batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
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
// Restano a carico dell'app: la registrazione dei task runner (runner.Provide / grpchandler.Provide)
// e la fornitura del driver DB (coremongo.NewService / coresql.NewService).
func Module(cfg *Config, opts ...Option) {
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

	// Tutte le registrazioni del sottosistema batch confluiscono in un fx.Module("batch")
	// per il namespacing del grafo/log fx. I provide NON sono privati: restano visibili
	// all'app e i value group (batch_runners forniti dall'app a root, batch_jobs) aggregano
	// come prima. Il mode-gating resta per-registrazione dentro ogni core.Provide/Supply.
	core.Module("batch", func() {
		// store.IData + store.IWorkItemStore: consumati sia lato scheduler che lato worker, quindi
		// sempre attivi (nessun mode gate → chiamata senza modes).
		o.store()

		// Lock distribuito: backend iniettato (redis/mongo/sql), gate-ato sui scheduler modes.
		// Il suo eventuale config è fornito dalla sua lib (es. redis.Module dell'app), non da batch.
		o.locker(sched...)

		// Config dei backend: suppliti a fx SOLO se valorizzati. Un config non impostato non viene
		// supplito, così se un componente attivo lo richiede fx fallisce subito con un chiaro
		// "missing dependency" invece di far girare il backend con valori vuoti (fallimento tardivo).
		if cfg.Grpc.Client.Url != "" {
			core.Supply(&cfg.Grpc.Client, sched...)
		}
		if cfg.KafkaConfig.BootstrapServer != "" {
			core.Supply(&cfg.KafkaConfig, sched...)
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
