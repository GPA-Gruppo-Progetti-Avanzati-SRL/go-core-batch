package kafka

import (
	"time"
)

const (
	AcksPropertyName                             = "acks"
	AutoOffsetResetPropertyName                  = "auto.offset.reset"
	BootstrapServersPropertyName                 = "bootstrap.servers"
	CommitModeAuto                               = "auto"
	CommitModeManual                             = "manual"
	CommitModeTransaction                        = "tx"
	ConnectionsMaxIdleMs                         = "connections.max.idle.ms"
	DeliveryTimeoutMs                            = "delivery.timeout.ms"
	EnableAutoCommitPropertyName                 = "enable.auto.commit"
	EnablePartitionEOFPropertyName               = "enable.partition.eof"
	EnableSSLCertificateVerificationPropertyName = "enable.ssl.certificate.verification"
	GoApplicationRebalanceEnablePropertyName     = "go.application.rebalance.enable"
	GroupIdPropertyName                          = "group.id"
	HeartBeatIntervalMs                          = "heartbeat.interval.ms"
	IsolationLevelPropertyName                   = "isolation.level"
	LingerMs                                     = "linger.ms"
	MaxPollIntervalMs                            = "max.poll.interval.ms"
	MessageSendMaxRetries                        = "message.send.max.retries"
	MetadataMaxAgeMs                             = "metadata.max.age.ms" // 180000
	MetadataMaxIdleMs                            = "metadata.max.idle.ms"
	RequestTimeoutMs                             = "request.timeout.ms" //60000
	Retries                                      = "retries"
	SASLMechanismPropertyName                    = "sasl.mechanism"
	SASLPasswordPropertyName                     = "sasl.password"
	SASLUsernamePropertyName                     = "sasl.username"
	SSLCaLocationPropertyName                    = "ssl.ca.location"
	SecurityProtocolPropertyName                 = "security.protocol"
	SessionTimeOutMsPropertyName                 = "session.timeout.ms"
	SocketKeepaliveEnable                        = "socket.keepalive.enable" // true
	TransactionalIdPropertyName                  = "transactional.id"
	TransactionalTimeoutMsPropertyName           = "transaction.timeout.ms"
	Debug                                        = "debug"

	KafkaNumberOfDeliveryAttemptsHeaderName = "Kafka-Delivery-Attempts"
)

type Config struct {
	Producer         ProducerConfig `yaml:"producer" mapstructure:"producer" json:"producer"`
	Consumer         ConsumerConfig `yaml:"consumer" mapstructure:"consumer" json:"consumer"`
	SecurityProtocol string         `mapstructure:"security-protocol" json:"security-protocol" yaml:"security-protocol"`
	SSL              SSLCfg         `mapstructure:"ssl" json:"ssl" yaml:"ssl"`
	SASL             SaslCfg        `mapstructure:"sasl" json:"sasl" yaml:"sasl"`
	BootstrapServer  string         `yaml:"bootstrap-server" mapstructure:"bootstrap-server" json:"bootstrap-server"`
	TopicSoglie      string         `yaml:"topic-soglie" mapstructure:"topic-soglie" json:"topic-soglie"`
	FlushTimeout     int            `yaml:"flush-timeout" mapstructure:"flush-timeout" json:"flush-timeout"`
	TopicRicariche   string         `yaml:"topic-ricariche" mapstructure:"topic-ricariche" json:"topic-ricariche"`
	GroupId          string         `yaml:"group-id" mapstructure:"group-id" json:"group-id"`
}

type SSLCfg struct {
	CaLocation string `mapstructure:"ca-location" json:"ca-location" yaml:"ca-location"`
	SkipVerify bool   `json:"skv,omitempty" yaml:"skv,omitempty" mapstructure:"skv,omitempty"`
}

type SaslCfg struct {
	Mechanisms string `mapstructure:"mechanisms" json:"mechanisms" yaml:"mechanisms"`
	Username   string `mapstructure:"username" json:"username" yaml:"username"`
	Password   string `mapstructure:"password" json:"password" yaml:"password"`
	CaLocation string `json:"ca-location" mapstructure:"ca-location" yaml:"ca-location"`
	SkipVerify bool   `json:"skv,omitempty" mapstructure:"skv,omitempty" yaml:"skv,omitempty"`
}

type ConsumerConfig struct {
	// Consumer related configs
	IsolationLevel        string `mapstructure:"isolation-level" json:"isolation-level" yaml:"isolation-level"`
	MaxPollRecords        int    `mapstructure:"max-poll-records" json:"max-poll-records" yaml:"max-poll-records"`
	AutoOffsetReset       string `mapstructure:"auto-offset-reset" json:"auto-offset-reset" yaml:"auto-offset-reset"`
	SessionTimeoutMs      int    `mapstructure:"session-timeout-ms" json:"session-timeout-ms" yaml:"session-timeout-ms"`
	FetchMinBytes         int    `mapstructure:"fetch-min-bytes" json:"fetch-min-bytes" yaml:"fetch-min-bytes"`
	FetchMaxBytes         int    `mapstructure:"fetch-max-bytes" json:"fetch-max-bytes" yaml:"fetch-max-bytes"`
	Delay                 int    `mapstructure:"delay" json:"delay" yaml:"delay"`
	MaxRetry              int    `mapstructure:"max-retry" json:"max-retry" yaml:"max-retry"`
	EnablePartitionEOF    bool   `mapstructure:"enable-partition-eof" json:"enable-partition-eof" yaml:"enable-partition-eof"`
	MetadataMaxAgeMs      int    `mapstructure:"metadata-max-age-ms,omitempty" json:"metadata-max-age-ms,omitempty" yaml:"metadata-max-age-ms,omitempty"`
	SocketKeepaliveEnable bool   `mapstructure:"socket-keepalive-enable,omitempty" json:"socket-keepalive-enable,omitempty" yaml:"socket-keepalive-enable,omitempty"`
	// RequestTimeoutMs      int    `mapstructure:"request-timeout-ms,omitempty" json:"request-timeout-ms,omitempty" yaml:"request-timeout-ms,omitempty"`
	ConnectionsMaxIdleMs int                    `mapstructure:"connections-max-idle-ms,omitempty" json:"connections-max-idle-ms,omitempty" yaml:"connections-max-idle-ms,omitempty"`
	HeartBeatIntervalMs  int                    `mapstructure:"heartbeat-interval-ms,omitempty" json:"heartbeat-interval-ms,omitempty" yaml:"heartbeat-interval-ms,omitempty"`
	ExtraConfig          map[string]interface{} `mapstructure:"extra-config,omitempty" json:"extra-config,omitempty" yaml:"extra-config,omitempty"`
}

type ProducerConfig struct {
	// Producer related configs
	Acks                  string                 `mapstructure:"acks" json:"acks" yaml:"acks"`
	MaxTimeoutMs          int                    `mapstructure:"max-timeout-ms" json:"max-timeout-ms" yaml:"max-timeout-ms"`
	DeliveryTimeout       time.Duration          `mapstructure:"delivery-timeout"`
	FlushTimeout          time.Duration          `mapstructure:"flush-timeout"`
	MessageSendMaxRetries int                    `mapstructure:"max-retries"`
	MetadataMaxAgeMs      int                    `mapstructure:"metadata-max-age-ms,omitempty" json:"metadata-max-age-ms,omitempty" yaml:"metadata-max-age-ms,omitempty"`
	SocketKeepaliveEnable bool                   `mapstructure:"socket-keepalive-enable,omitempty" json:"socket-keepalive-enable,omitempty" yaml:"socket-keepalive-enable,omitempty"`
	RequestTimeoutMs      int                    `mapstructure:"request-timeout-ms,omitempty" json:"request-timeout-ms,omitempty" yaml:"request-timeout-ms,omitempty"`
	ConnectionsMaxIdleMs  int                    `mapstructure:"connections-max-idle-ms,omitempty" json:"connections-max-idle-ms,omitempty" yaml:"connections-max-idle-ms,omitempty"`
	MetadataMaxIdleMs     int                    `mapstructure:"metadata-max-idle-ms,omitempty" json:"metadata-max-idle-ms,omitempty" yaml:"metadata-max-idle-ms,omitempty"`
	ExtraConfig           map[string]interface{} `mapstructure:"extra-config,omitempty" json:"extra-config,omitempty" yaml:"extra-config,omitempty"`
}
