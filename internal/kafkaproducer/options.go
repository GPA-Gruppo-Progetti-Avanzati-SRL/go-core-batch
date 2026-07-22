package kafkaproducer

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// kgoLogger adatta il logging interno di franz-go a zerolog: senza di esso gli errori di
// dial/TLS/SASL/transaction restano nascosti dietro un generico "context deadline exceeded".
// La verbosità segue il livello globale di zerolog (con log level -1/trace → Debug completo).
type kgoLogger struct{}

func (kgoLogger) Level() kgo.LogLevel {
	switch {
	case zerolog.GlobalLevel() <= zerolog.DebugLevel:
		return kgo.LogLevelDebug
	case zerolog.GlobalLevel() == zerolog.InfoLevel:
		return kgo.LogLevelInfo
	case zerolog.GlobalLevel() == zerolog.WarnLevel:
		return kgo.LogLevelWarn
	default:
		return kgo.LogLevelError
	}
}

func (kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var zl zerolog.Level
	switch level {
	case kgo.LogLevelError:
		zl = zerolog.ErrorLevel
	case kgo.LogLevelWarn:
		zl = zerolog.WarnLevel
	case kgo.LogLevelInfo:
		zl = zerolog.InfoLevel
	default:
		zl = zerolog.DebugLevel
	}
	e := log.WithLevel(zl).Str("component", "kafka")
	for i := 0; i+1 < len(keyvals); i += 2 {
		e = e.Interface(fmt.Sprint(keyvals[i]), keyvals[i+1])
	}
	e.Msg(msg)
}

// buildKafkaOptions traduce la *kafka.Config in opzioni franz-go. È plumbing
// interno al producer: NON fa parte dell'API pubblica del package kafka.
func buildKafkaOptions(cfgKafka *kafka.Config, extraConfig map[string]interface{}) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfgKafka.BootstrapServer, ",")...),
		kgo.WithLogger(kgoLogger{}),
	}

	// Security Protocol & SASL
	switch cfgKafka.SecurityProtocol {
	case "SSL":
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfgKafka.SSL.SkipVerify,
		}
		// Nota: Per caricare CA da file (CaLocation) servirebbe leggere il file e aggiungerlo a RootCAs
		// Per ora manteniamo la logica di skip verify se configurato
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))

	case "SASL_SSL", "SASL":
		var mechanism sasl.Mechanism
		switch strings.ToUpper(cfgKafka.SASL.Mechanisms) {
		case "PLAIN":
			mechanism = plain.Auth{
				User: cfgKafka.SASL.Username,
				Pass: cfgKafka.SASL.Password,
			}.AsMechanism()
		case "SCRAM-SHA-256":
			mechanism = scram.Auth{
				User: cfgKafka.SASL.Username,
				Pass: cfgKafka.SASL.Password,
			}.AsSha256Mechanism()
		case "SCRAM-SHA-512":
			mechanism = scram.Auth{
				User: cfgKafka.SASL.Username,
				Pass: cfgKafka.SASL.Password,
			}.AsSha512Mechanism()
		default:
			log.Warn().Str("mechanism", cfgKafka.SASL.Mechanisms).Msg("Unsupported SASL mechanism, defaulting to PLAIN")
			mechanism = plain.Auth{
				User: cfgKafka.SASL.Username,
				Pass: cfgKafka.SASL.Password,
			}.AsMechanism()
		}
		opts = append(opts, kgo.SASL(mechanism))

		if cfgKafka.SecurityProtocol == "SASL_SSL" || cfgKafka.SecurityProtocol == "SSL" {
			tlsCfg := &tls.Config{
				InsecureSkipVerify: cfgKafka.SASL.SkipVerify,
			}
			opts = append(opts, kgo.DialTLSConfig(tlsCfg))
		}
	default:
		log.Trace().Str(kafka.SecurityProtocolPropertyName, cfgKafka.SecurityProtocol).Msg("No special security protocol configured")
	}

	// Consumer Group
	if cfgKafka.GroupId != "" {
		opts = append(opts, kgo.ConsumerGroup(cfgKafka.GroupId))
	}

	// Extra Config handling (best effort mapping)
	for key, value := range extraConfig {
		k := strings.ReplaceAll(key, "_", ".")
		switch k {
		case kafka.AcksPropertyName:
			if s, ok := value.(string); ok {
				switch s {
				case "all", "-1":
					opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
				case "1":
					opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
				case "0":
					opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
				}
			}
		case kafka.TransactionalIdPropertyName:
			if s, ok := value.(string); ok {
				opts = append(opts, kgo.TransactionalID(s))
			}
		case kafka.AutoOffsetResetPropertyName:
			if s, ok := value.(string); ok {
				switch s {
				case "earliest":
					opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
				case "latest":
					opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
				}
			}
		default:
			log.Debug().Str("key", k).Interface("value", value).Msg("Extra config ignored or not directly mappable in buildKafkaOptions")
		}
	}

	return opts
}
