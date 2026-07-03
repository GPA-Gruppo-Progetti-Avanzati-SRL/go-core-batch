package go_core_batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/redis"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
)

type Config struct {
	RedisConfig redis.Config `yaml:"redis" mapstructure:"redis" json:"redis"`
	Grpc        grpc.Config  `yaml:"grpc" mapstructure:"grpc" json:"grpc"`
	S3          s3.Config    `yaml:"s3" mapstructure:"s3" json:"s3"`
}
