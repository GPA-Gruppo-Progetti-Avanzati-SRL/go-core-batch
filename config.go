package batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"
)

// Config è la configurazione unificata dell'intero sottosistema batch: raccoglie i
// sotto-config di tutti i pezzi cablati da Module (trasporto gRPC, feed S3, job
// schedulati, worker pool). L'app la carica come singola sezione YAML
// e la passa a Module: sono i Module dei singoli package a fare il core.Supply interno,
// quindi l'app non deve più supplire nulla a fx.
//
// Il lock distribuito NON è più qui: il backend (redis/mongo/sql) è iniettato via
// batch.WithLocker e il suo eventuale config (es. go-core-redis) è gestito dalla sua lib.
//
// Nemmeno Kafka è più qui. Il job NotificationKafka produce col producer di go-core-kafka, quindi la
// sua configurazione è quella di go-core-kafka (sezione `server`, con `producer` dentro) e la wira
// l'app con corekafka.ProducerModule: una sola config Kafka per applicazione, invece di una seconda
// dentro il batch che descriveva lo stesso broker con altre chiavi.
type Config struct {
	Grpc       grpc.Config        `yaml:"grpc" mapstructure:"grpc" json:"grpc"`
	S3         s3.Config          `yaml:"s3" mapstructure:"s3" json:"s3"`
	JobsConfig []scheduler.Config `yaml:"jobs" mapstructure:"jobs" json:"jobs"`
	// TasksConfig è la configurazione APPLICATIVA dei task (sezione `tasks:`): ogni voce è
	// un'istanza — name + type + properties — mappata sui campi `prop:` della struct del runner.
	// Da non confondere col blocco `properties:` di un job, che è infrastrutturale.
	TasksConfig   []task.Config   `yaml:"tasks" mapstructure:"tasks" json:"tasks"`
	WorkersConfig []worker.Config `yaml:"workers" mapstructure:"workers" json:"workers"`
}
