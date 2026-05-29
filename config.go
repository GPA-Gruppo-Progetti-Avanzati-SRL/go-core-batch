package go_core_batch

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/redis"
)

type Config struct {
	RedisConfig redis.Config `yaml:"redis" mapstructure:"redis" json:"redis"`
	Grpc        grpc.Config  `yaml:"grpc" mapstructure:"grpc" json:"grpc"`
}
