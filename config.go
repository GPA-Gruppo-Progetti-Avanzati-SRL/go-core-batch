package batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/redis"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"
)

// Config è la configurazione unificata dell'intero sottosistema batch: raccoglie i
// sotto-config di tutti i pezzi cablati da Module (redis lock, trasporto gRPC, feed S3,
// job schedulati, worker pool, producer Kafka). L'app la carica come singola sezione YAML
// e la passa a Module: sono i Module dei singoli package a fare il core.Supply interno,
// quindi l'app non deve più supplire nulla a fx.
type Config struct {
	RedisConfig   redis.Config       `yaml:"redis" mapstructure:"redis" json:"redis"`
	Grpc          grpc.Config        `yaml:"grpc" mapstructure:"grpc" json:"grpc"`
	S3            s3.Config          `yaml:"s3" mapstructure:"s3" json:"s3"`
	JobsConfig    []scheduler.Config `yaml:"jobs" mapstructure:"jobs" json:"jobs"`
	WorkersConfig []worker.Config    `yaml:"workers" mapstructure:"workers" json:"workers"`
	KafkaConfig   kafka.Config       `yaml:"kafka" mapstructure:"kafka" json:"kafka"`
}
