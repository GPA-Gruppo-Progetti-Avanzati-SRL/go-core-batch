package kafkajob

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("NotificationKafkaJob")

func makeNotificationJobFactory(producer *kafka.ProducerService, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return gocron.NewTask(notificationJobRun, name, producer, items, config.Properties)
	}
}

func notificationJobRun(name string, producer *kafka.ProducerService, items store.IWorkItemStore, p map[string]string) error {
	jobId := jobID(name)
	log.Trace().Msgf("S - %s - Eseguo job ...", jobId)

	timeout := 10 * time.Second
	if v, ok := p["timeout"]; ok {
		if n, err := strconv.Atoi(v); err != nil {
			log.Error().Msgf("S - %s - timeout invalido: %s", jobId, v)
		} else {
			timeout = time.Duration(n) * time.Second
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	spanCtx, span := tracer.Start(ctx, name)
	span.SetAttributes(attribute.String("jobName", name), attribute.String("jobId", jobId))
	defer span.End()

	destination, ok := p["destination"]
	if !ok {
		return core.TechnicalErrorWithCodeAndMessage("PROPERTIES", "destination not found in properties")
	}
	object, ok := p["object"]
	if !ok {
		return core.TechnicalErrorWithCodeAndMessage("PROPERTIES", "object not found in properties")
	}
	topic, ok := p["topic"]
	if !ok {
		return core.TechnicalErrorWithCodeAndMessage("PROPERTIES", "topic not found in properties")
	}
	span.SetAttributes(
		attribute.String("destination", destination),
		attribute.String("object", object),
	)

	pending, err := items.FindPending(spanCtx, JobType, destination, object)
	if err != nil {
		span.RecordError(err)
		return core.TechnicalErrorWithError(err)
	}
	if len(pending) == 0 {
		log.Trace().Msgf("S - %s - No pending work items", jobId)
		return nil
	}
	log.Info().Msgf("S - %s - %d work items to send", jobId, len(pending))

	ids, kafkaMsgs := prepareMessages(pending)
	if errProduce := producer.ProduceMessages(spanCtx, kafkaMsgs, topic); errProduce != nil {
		span.RecordError(errProduce)
		log.Error().Msgf("S - %s - error producing to kafka: %s", jobId, errProduce)
		return errProduce
	}

	// At-least-once delivery: if MarkDone fails, items will be re-sent on the next tick
	// (Kafka produce is NOT rolled back).
	if len(ids) > 0 {
		if errMark := items.MarkDone(spanCtx, ids); errMark != nil {
			span.RecordError(errMark)
			log.Error().Err(errMark).Msgf("S - %s - ATTENZIONE: Kafka produce OK ma MarkDone fallito — %d items saranno ri-inviati al prossimo tick", jobId, len(ids))
			return errMark
		}
	}
	return nil
}

func prepareMessages(items []*store.WorkItem) ([]string, []*kafka.Message) {
	var ids []string
	var out []*kafka.Message

	for _, item := range items {
		d, ok := item.Payload.(bson.D)
		if !ok {
			log.Error().Msgf("Payload non atteso per work item %s: %T", item.Id, item.Payload)
			continue
		}
		kMap := make(map[string]interface{})
		if err := decodeBsonD(d, &kMap); err != nil {
			log.Error().Err(err).Msgf("Impossibile decodificare payload work item %s", item.Id)
			continue
		}
		messageKey, ok := kMap["messageKey"]
		if !ok {
			log.Error().Msgf("messageKey mancante in work item %s", item.Id)
			continue
		}
		messageValue, ok := kMap["messageValue"]
		if !ok {
			log.Error().Msgf("messageValue mancante in work item %s", item.Id)
			continue
		}
		km := &kafka.Message{MessageKey: messageKey, MessageValue: messageValue}
		if headersRaw, ok := kMap["messageHeaders"]; ok {
			headers, err := toStringMap(headersRaw)
			if err != nil {
				log.Error().Msgf("Impossibile mappare headers in work item %s", item.Id)
				continue
			}
			km.MessageHeader = headers
		}
		out = append(out, km)
		ids = append(ids, item.Id)
	}
	log.Debug().Msgf("S - preparati %d messaggi su %d work items", len(ids), len(items))
	return ids, out
}

func decodeBsonD(d bson.D, dest *map[string]interface{}) error {
	temp, err := bson.MarshalExtJSON(d, true, true)
	if err != nil {
		return err
	}
	return bson.UnmarshalExtJSON(temp, true, dest)
}

func toStringMap(input interface{}) (map[string]string, error) {
	m, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[string]interface{}, got %T", input)
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("value for key %q is not a string", k)
		}
		out[k] = s
	}
	return out, nil
}

func jobID(name string) string {
	return fmt.Sprintf("%s-%s", name, time.Now().Format("20060102150405"))
}
