package kafkaproducer

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
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

// buildTLSConfig costruisce
// la tls.Config per il dial verso i broker. Se caLocation è
// valorizzato, il PEM (tipicamente il truststore con la CA privata) viene caricato in RootCAs:
// senza questo, con una CA non nei root di sistema, la verifica fallisce con
// "x509: certificate signed by unknown authority". skipVerify disabilita la verifica (insicuro).
func buildTLSConfig(caLocation string, skipVerify bool) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: skipVerify}
	if caLocation != "" {
		pem, err := os.ReadFile(caLocation)
		if err != nil {
			return nil, fmt.Errorf("lettura ca-location %q: %w", caLocation, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("nessun certificato valido in ca-location %q", caLocation)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// transactionalID ritorna il transactional.id configurato (campo di primo livello
// producer.transactional-id) e se è valorizzato. Serve al ProducerService per il fail-fast:
// senza un id il client franz-go NON è transazionale e BeginTransaction fallirebbe.
func transactionalID(p kafka.ProducerConfig) (string, bool) {
	return p.TransactionalID, p.TransactionalID != ""
}

// buildKafkaOptions traduce la *kafka.Config in opzioni franz-go. È plumbing
// interno al producer: NON fa parte dell'API pubblica del package kafka.
func buildKafkaOptions(cfgKafka *kafka.Config, extraConfig map[string]any) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfgKafka.BootstrapServer, ",")...),
		kgo.WithLogger(kgoLogger{}),
	}

	// Security Protocol & SASL
	switch cfgKafka.SecurityProtocol {
	case "SSL":
		if tlsCfg, err := buildTLSConfig(cfgKafka.SSL.CaLocation, cfgKafka.SSL.SkipVerify); err != nil {
			log.Error().Err(err).Msg("Kafka TLS (SSL): impossibile costruire la tls.Config")
		} else {
			opts = append(opts, kgo.DialTLSConfig(tlsCfg))
		}

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
			if tlsCfg, err := buildTLSConfig(cfgKafka.SASL.CaLocation, cfgKafka.SASL.SkipVerify); err != nil {
				log.Error().Err(err).Msg("Kafka TLS (SASL_SSL): impossibile costruire la tls.Config")
			} else {
				opts = append(opts, kgo.DialTLSConfig(tlsCfg))
			}
		}
	default:
		log.Trace().Str(kafka.SecurityProtocolPropertyName, cfgKafka.SecurityProtocol).Msg("No special security protocol configured")
	}

	// Consumer Group
	if cfgKafka.GroupId != "" {
		opts = append(opts, kgo.ConsumerGroup(cfgKafka.GroupId))
	}

	// Transactional producer (EOS): id come campo di primo livello producer.transactional-id.
	if cfgKafka.Producer.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(cfgKafka.Producer.TransactionalID))
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
