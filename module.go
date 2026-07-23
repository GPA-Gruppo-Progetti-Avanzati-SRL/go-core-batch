package batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/redis"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/grpcdispatcher"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/localdispatcher"
	djmongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/mongostore"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/queryfeed"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/s3feed"
	djsql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob/sqlstore"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/kafkajob"
	storemongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store/mongostore"
	storesql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store/sqlstore"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker/grpchandler"
)

type storeBackend int

const (
	storeMongo storeBackend = iota // default
	storeSQL
)

type dispatchMode int

const (
	dispatchLocal dispatchMode = iota // default
	dispatchGRPC
)

// options raccoglie le scelte di topologia non derivabili dalla Config: i modes con cui
// gate-are le due famiglie di componenti (scheduler vs worker), il backend store, la
// strategia di dispatch e quali feed/job opzionali attivare.
type options struct {
	schedulerModes []string
	workerModes    []string
	store          storeBackend
	dispatch       dispatchMode
	queryFeed      bool
	s3Feed         bool
	kafkaJob       bool
	grpcWorker     bool
}

// Option configura Module.
type Option func(*options)

// WithSchedulerModes limita i componenti lato scheduler (dispatcher, feed, job Kafka,
// store query, Scheduler) ai core.Mode indicati. Vuoto = sempre attivi.
func WithSchedulerModes(modes ...string) Option {
	return func(o *options) { o.schedulerModes = modes }
}

// WithWorkerModes limita il worker pool gRPC (grpchandler) ai core.Mode indicati.
// Vuoto = sempre attivo. Tipicamente diverso dai modes scheduler (es. Worker vs Scheduler).
func WithWorkerModes(modes ...string) Option {
	return func(o *options) { o.workerModes = modes }
}

// WithMongoStore usa il backend MongoDB per store.IData/IWorkItemStore e per la
// distributedjob.IQueryStore. È il default. Richiede via fx un *mongolks.LinkedService.
func WithMongoStore() Option {
	return func(o *options) { o.store = storeMongo }
}

// WithSqlStore usa il backend SQL (bun) per store.IData/IWorkItemStore e per la
// distributedjob.IQueryStore. Richiede via fx un *bun.DB.
func WithSqlStore() Option {
	return func(o *options) { o.store = storeSQL }
}

// WithLocalDispatch esegue i task in-process (localdispatcher). È il default: nessuna
// infrastruttura gRPC, adatto al deployment single-instance.
func WithLocalDispatch() Option {
	return func(o *options) { o.dispatch = dispatchLocal }
}

// WithGrpcDispatch inoltra i task ai worker remoti via gRPC (grpcdispatcher, usa Grpc.Client).
// Il processo worker va cablato con WithGrpcWorker (tipicamente in un mode diverso).
func WithGrpcDispatch() Option {
	return func(o *options) { o.dispatch = dispatchGRPC }
}

// WithQueryFeed attiva il feed by-query (job DistribuiteTaskByQuery): wira la
// distributedjob.IQueryStore sul backend scelto + il queryfeed.
func WithQueryFeed() Option {
	return func(o *options) { o.queryFeed = true }
}

// WithS3Feed attiva il feed da file S3 (job DistribuiteTaskByS3File, usa S3.Services).
func WithS3Feed() Option {
	return func(o *options) { o.s3Feed = true }
}

// WithKafkaJob attiva il job NotificationKafka (usa KafkaConfig): legge i WorkItem pending
// e li invia su Kafka (pattern outbox).
func WithKafkaJob() Option {
	return func(o *options) { o.kafkaJob = true }
}

// WithGrpcWorker attiva il worker pool gRPC (grpchandler, lato worker): usa Grpc.Server e
// WorkersConfig. Da abbinare a WithGrpcDispatch sul lato scheduler.
func WithGrpcWorker() Option {
	return func(o *options) { o.grpcWorker = true }
}

// Module wira il sottosistema batch a partire da una singola Config, sostituendo la sfilza
// di Module() da chiamare a mano in init(). Ogni componente selezionato è cablato
// esplicitamente e gate-ato SOLO tramite i suoi modes (nessun condizionale sul valore del
// config): è il core.Mode a runtime a decidere cosa viene effettivamente costruito.
//
// Topologia via opzioni: store MongoDB (default) o SQL (WithSqlStore); dispatch in-process
// (default) o gRPC (WithGrpcDispatch); feed opt-in WithQueryFeed / WithS3Feed; job Kafka
// opt-in WithKafkaJob; worker pool gRPC opt-in WithGrpcWorker.
//
// Ordine di registrazione (garantito da Module): store → redis → dispatcher →
// (query store + queryfeed) → s3feed → kafkajob → grpchandler → Scheduler PER ULTIMO.
// L'ordine non è cosmetico: newScheduler legge la mappa globale scheduler.Jobs al momento
// della costruzione e gli fx.Invoke girano nell'ordine di registrazione. Registrare lo
// Scheduler prima di dispatcher/feed/kafkajob lo costruirebbe con la mappa job type ancora
// vuota ("Job Type ... not found").
//
// Restano a carico dell'app: la registrazione dei task runner (runner.Provide /
// grpchandler.Provide) e la fornitura del driver DB (coremongo.NewService / coresql.NewService).
func Module(cfg *Config, opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	sched := o.schedulerModes
	work := o.workerModes

	// store.IData + store.IWorkItemStore: consumati sia lato scheduler che lato worker,
	// quindi sempre attivi (nessun mode gate).
	switch o.store {
	case storeSQL:
		storesql.Module()
	default:
		storemongo.Module()
	}

	// *redis.Client per il lock dello Scheduler (dipendenza obbligatoria di newScheduler).
	core.Supply(&cfg.RedisConfig, sched...)
	core.Provide(redis.NewService, sched...)

	// Dispatcher: in-process oppure gRPC verso worker remoti.
	switch o.dispatch {
	case dispatchGRPC:
		grpcdispatcher.Module(&cfg.Grpc.Client, sched...)
	default:
		localdispatcher.Module(sched...)
	}

	// Feed by-query (DistribuiteTaskByQuery): IQueryStore sul backend scelto + queryfeed.
	if o.queryFeed {
		switch o.store {
		case storeSQL:
			djsql.Module(sched...)
		default:
			djmongo.Module(sched...)
		}
		queryfeed.Module(sched...)
	}

	// Feed da file S3 (DistribuiteTaskByS3File).
	if o.s3Feed {
		s3feed.Module(cfg.S3, sched...)
	}

	// Job Kafka (NotificationKafka).
	if o.kafkaJob {
		kafkajob.Module(&cfg.KafkaConfig, sched...)
	}

	// Worker pool gRPC (lato worker).
	if o.grpcWorker {
		grpchandler.Module(&cfg.Grpc.Server, cfg.WorkersConfig, work...)
	}

	// Scheduler: DEVE restare l'ultimo Module registrato (vedi nota sull'ordine sopra).
	scheduler.Module(cfg.JobsConfig, sched...)
}
