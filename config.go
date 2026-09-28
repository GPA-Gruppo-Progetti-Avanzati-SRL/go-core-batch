package batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"
	corelock "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-locker"
)

// Config è la configurazione unificata dell'intero sottosistema batch: raccoglie i
// sotto-config di tutti i pezzi cablati da Module (trasporto gRPC, feed S3, job
// schedulati, worker pool). L'app la carica come singola sezione YAML
// e la passa a Module: sono i Module dei singoli package a fare il core.Supply interno,
// quindi l'app non deve più supplire nulla a fx.
//
// Il LOCK distribuito è qui: il motore è quello di go-core-locker, ma a wirarlo è batch.Module —
// lo scheduler ne è il consumatore principale, e chiedere all'applicazione una seconda chiamata
// (corelock.Module) per un servizio che le serve solo perché usa il batch era una riga di wiring
// che non decideva nulla. Il BACKEND resta una scelta dell'app, iniettato come ogni altro:
// batch.WithLocker(lockmongo.Module). Il Locker prodotto è fornito a ROOT, quindi resta
// iniettabile anche dall'applicazione per le proprie sezioni critiche.
//
// Kafka invece non è qui. Il job NotificationKafka produce col producer di go-core-kafka, quindi la
// sua configurazione è quella di go-core-kafka (sezione `server`, con `producer` dentro) e la wira
// l'app con corekafka.ProducerModule: una sola config Kafka per applicazione, invece di una seconda
// dentro il batch che descriveva lo stesso broker con altre chiavi.
type Config struct {
	Grpc grpc.Config `yaml:"grpc" mapstructure:"grpc" json:"grpc"`
	// Lock è la configurazione del lock distribuito (go-core-locker): TTL dei lease, attesa fra
	// i tentativi, key-prefix e il nome di collection/tabella. Può mancare del tutto — i default
	// li mette corelock.WithDefaults — ma il `key-prefix` va scritto quando più deployment
	// condividono lo stesso backend: senza, due applicazioni si contendono il lock sui nomi dei
	// propri job, che spesso coincidono, e il sintomo è soltanto un tick che non parte.
	Lock       corelock.Config    `yaml:"lock" mapstructure:"lock" json:"lock"`
	S3         s3.Config          `yaml:"s3" mapstructure:"s3" json:"s3"`
	JobsConfig []scheduler.Config `yaml:"jobs" mapstructure:"jobs" json:"jobs"`
	// TasksConfig è la configurazione APPLICATIVA dei task (sezione `tasks:`): ogni voce è
	// un'istanza — name + type + properties — mappata sui campi `prop:` della struct del runner.
	// Da non confondere col blocco `properties:` di un job, che è infrastrutturale.
	TasksConfig   []task.Config   `yaml:"tasks" mapstructure:"tasks" json:"tasks"`
	WorkersConfig []worker.Config `yaml:"workers" mapstructure:"workers" json:"workers"`
	// TaskLog dice quali righe di task_logs scrivere: `all` (default), `errors`, `off`.
	// Sul percorso distributedjob si scrivono TRE righe per item lavorato, e in molti deploy
	// quelle di successo non vengono mai lette: restano un costo di scrittura e una collection
	// che cresce. Un valore non previsto ferma l'avvio (vedi store.ParseTaskLogLevel).
	TaskLog string `yaml:"task-log" mapstructure:"task-log" json:"task-log"`
}
