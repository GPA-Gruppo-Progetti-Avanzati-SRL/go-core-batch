// Package kafkaproducer contiene il ProducerService Kafka. È un package INTERNAL: importabile
// solo da codice dentro go-core-batch (es. scheduler/kafkajob), NON dalle applicazioni.
// In questo modo il producer non è iniettabile nei runner dell'app: l'unico modo di mandare
// su Kafka è creare un WorkItem di tipo "NotificationKafka" (outbox), drenato dal job.
// I tipi pubblici Config e Message restano nel package kafka.
package kafkaproducer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
)

type ProducerService struct {
	client         *kgo.Client
	producerConfig *kafka.Config
	mu             sync.Mutex
}

// NewProducerService costruisce il producer Kafka. Fail-fast: se manca transactional.id in
// producer.extra-config l'app NON parte. Il producer usa SEMPRE le transazioni EOS
// (BeginTransaction/EndTransaction) e senza un transactional.id il client franz-go non è
// transazionale, quindi BeginTransaction fallirebbe ad ogni tick (retry-loop permanente):
// meglio bloccare subito con un errore chiaro. Il costruttore è lazy (fx lo invoca solo se il
// kafkajob è wirato), quindi il vincolo scatta solo quando il producer è effettivamente usato.
func NewProducerService(lc fx.Lifecycle, cfg *kafka.Config) (*ProducerService, error) {
	if _, ok := transactionalID(cfg.Producer); !ok {
		return nil, fmt.Errorf("kafka producer: transactional.id mancante in producer.extra-config, " +
			"richiesto per le transazioni EOS del kafkajob")
	}
	ks := &ProducerService{producerConfig: cfg}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {

			log.Info().Msg("Avvio Kafka Producer (Franz-go)")
			if ks.client != nil {
				log.Warn().Msg("Kafka Producer already running")
				return nil
			}
			errProd := ks.initProducer(ctx)

			if errProd != nil {
				log.Error().Err(errProd).Msg("Error initializing producer")
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info().Msg("Stopping Kafka Producer")
			if ks.client != nil {
				ks.client.Close()
			}
			return nil
		},
	})
	return ks, nil
}

func newKafkaClient(cfgKafka *kafka.Config) (*kgo.Client, error) {

	opts := buildKafkaOptions(cfgKafka, cfgKafka.Producer.ExtraConfig)

	// Opzioni specifiche del producer
	if cfgKafka.Producer.Acks != "" {
		switch cfgKafka.Producer.Acks {
		case "all", "-1":
			opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		case "1":
			opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
		case "0":
			opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
		}
	}

	if cfgKafka.Producer.DeliveryTimeout != 0 {
		// Franz-go non ha un'opzione diretta "delivery.timeout.ms" come librdkafka,
		// ma si può usare RequestTimeout o gestire a livello di Produce.
		// kgo gestisce i timeout internamente.
	}

	if cfgKafka.Producer.MessageSendMaxRetries > 0 {
		opts = append(opts, kgo.RecordRetries(cfgKafka.Producer.MessageSendMaxRetries))
	}

	// Tracing
	kotelClient := kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(kotel.TracerProvider(otel.GetTracerProvider()))))
	opts = append(opts, kgo.WithHooks(kotelClient.Hooks()...))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (ks *ProducerService) ProduceMessages(ctx context.Context, messages []*kafka.Message, topic string) *core.ApplicationError {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ks.client == nil {
		if errProd := ks.initProducer(ctx); errProd != nil {
			return core.TechnicalErrorWithError(errProd)
		}
	}

	// Il producer è sempre transazionale (transactional.id validato nel costruttore).
	if errBegin := ks.client.BeginTransaction(); errBegin != nil {
		log.Error().Err(errBegin).Msg("Failed to begin transaction ... Destroying Producer")
		ks.client.Close()
		ks.client = nil
		return core.TechnicalErrorWithError(errBegin)
	}

	transactionInProgress := true
	defer func() {
		if transactionInProgress && ctx.Err() != nil {
			log.Warn().Msg("Timeout o cancellazione rilevata, interrompo transazione kafka...")
			if abortErr := ks.client.EndTransaction(context.Background(), kgo.TryAbort); abortErr != nil {
				log.Error().Err(abortErr).Msgf("Errore durante l'interruzione della transazione kafka : %v\n", abortErr)
			} else {
				log.Info().Msg("Transazione  kafka interrotta con successo")
			}
		}
	}()

	var wg sync.WaitGroup
	var produceErr error
	var errMu sync.Mutex

	for _, message := range messages {
		key, errSerKey := json.Marshal(message.MessageKey)
		if errSerKey != nil {
			log.Error().Err(errSerKey).Msgf("Impossibile serializzare la chiave : %s", errSerKey.Error())
			ks.abort(ctx)
			return core.TechnicalErrorWithError(errSerKey)
		}

		value, errSerMessage := json.Marshal(message.MessageValue)
		if errSerMessage != nil {
			log.Error().Err(errSerMessage).Msgf("Impossibile serializzare il messaggio : %s", errSerMessage.Error())
			ks.abort(ctx)
			return core.TechnicalErrorWithError(errSerMessage)
		}

		var kafkaHeaders []kgo.RecordHeader
		if message.MessageHeader != nil {
			for k, v := range message.MessageHeader {
				kafkaHeaders = append(kafkaHeaders, kgo.RecordHeader{
					Key:   k,
					Value: []byte(v),
				})
			}
		}

		record := &kgo.Record{
			Topic:   topic,
			Value:   value,
			Key:     key,
			Headers: kafkaHeaders,
		}

		wg.Add(1)
		ks.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				errMu.Lock()
				if produceErr == nil {
					produceErr = err
				}
				errMu.Unlock()
			}
		})
	}

	wg.Wait()

	if produceErr != nil {
		log.Error().Err(produceErr).Msg("Error during produce")
		ks.abort(ctx)
		return core.TechnicalErrorWithError(produceErr)
	}

	// In franz-go non serve il Flush esplicito se usiamo le transazioni,
	// EndTransaction lo fa internamente.
	errCommit := ks.client.EndTransaction(ctx, kgo.TryCommit)

	if errCommit != nil {
		log.Error().Err(errCommit).Msg("Failed to commit transaction")
		// In caso di errore fatale, resettiamo il client
		if errors.Is(errCommit, kgo.ErrClientClosed) {
			ks.client = nil
		}
		return core.TechnicalErrorWithError(errCommit)
	}

	transactionInProgress = false
	return nil
}

// abort interrompe la transazione corrente (TryAbort), loggando un eventuale errore.
// Consolida i vari punti di rollback di ProduceMessages in un unico helper.
func (ks *ProducerService) abort(ctx context.Context) {
	if errAbort := ks.client.EndTransaction(ctx, kgo.TryAbort); errAbort != nil {
		log.Error().Err(errAbort).Msgf("Errore durante l'interruzione della transazione kafka : %v", errAbort)
	}
}

func (ks *ProducerService) initProducer(ctx context.Context) error {

	log.Info().Msg("Producer not initialized... Initializing producer")
	var errProd error
	if ks.client, errProd = newKafkaClient(ks.producerConfig); errProd != nil {
		log.Error().Err(errProd).Msg("Failed to create Kafka producer")
		return core.TechnicalErrorWithError(errProd)
	}
	return nil
}
