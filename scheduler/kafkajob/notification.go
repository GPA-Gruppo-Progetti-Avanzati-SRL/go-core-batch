package kafkajob

import (
	"context"
	"errors"
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

const defaultKafkaOrphanTimeout = 10 * time.Minute
const defaultKafkaLimit = 100

func makeNotificationJobFactory(producer *kafka.ProducerService, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return gocron.NewTask(func() error {
			return notificationJobRun(name, producer, items, config)
		})
	}
}

func notificationJobRun(name string, producer *kafka.ProducerService, items store.IWorkItemStore, config scheduler.Config) error {
	p := config.Properties
	jobId := jobID(name)

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

	limit := defaultKafkaLimit
	if v := p["limit"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}

	timeout := 30 * time.Second
	if v := p["timeout"]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			timeout = time.Duration(n) * time.Second
		}
	}

	orphanTimeout := config.LockTimeout
	if orphanTimeout == 0 {
		orphanTimeout = defaultKafkaOrphanTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	spanCtx, span := tracer.Start(ctx, name)
	span.SetAttributes(
		attribute.String("jobName", name),
		attribute.String("jobId", jobId),
		attribute.String("destination", destination),
		attribute.String("object", object),
	)
	defer span.End()

	// 1. Re-claim orphans
	orphans, appErr := items.RecoverOrphans(spanCtx, JobType, destination, object, orphanTimeout, limit)
	if appErr != nil {
		log.Warn().Err(appErr).Msgf("[%s] orphan recovery failed", jobId)
		orphans = nil
	} else if len(orphans) > 0 {
		log.Info().Msgf("[%s] re-claimed %d orphaned item(s)", jobId, len(orphans))
	}

	// 2. Claim fresh PENDING items
	remaining := limit - len(orphans)
	var fresh []*store.WorkItem
	if remaining > 0 {
		fresh, appErr = items.ClaimPending(spanCtx, JobType, destination, object, remaining)
		if appErr != nil {
			span.RecordError(appErr)
			log.Error().Err(appErr).Msgf("[%s] ClaimPending failed", jobId)
			return appErr
		}
	}

	all := append(orphans, fresh...)
	if len(all) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobId)
		return nil
	}
	log.Info().Msgf("[%s] processing %d item(s) (%d orphaned, %d fresh)", jobId, len(all), len(orphans), len(fresh))

	ids, kafkaMsgs := prepareMessages(all)
	if len(kafkaMsgs) == 0 {
		// all items had invalid payload — mark them failed individually
		for _, item := range all {
			items.MarkFailed(spanCtx, item.Id, "invalid payload")
		}
		return nil
	}

	if errProduce := producer.ProduceMessages(spanCtx, kafkaMsgs, topic); errProduce != nil {
		span.RecordError(errProduce)
		log.Error().Err(errProduce).Msgf("[%s] Kafka produce failed — resetting %d items to PENDING", jobId, len(ids))
		// Transient Kafka failure — reset claimed items so they are retried next tick.
		var retryErr *store.RetryError
		var after time.Duration
		if errors.As(errProduce, &retryErr) {
			after = retryErr.After
		}
		for _, id := range ids {
			items.MarkPending(spanCtx, id, after)
		}
		return errProduce
	}

	// At-least-once: if MarkDone fails, items will be re-sent on the next tick.
	if errMark := items.MarkDone(spanCtx, ids); errMark != nil {
		span.RecordError(errMark)
		log.Error().Err(errMark).Msgf("[%s] ATTENZIONE: Kafka produce OK ma MarkDone fallito — %d items saranno ri-inviati", jobId, len(ids))
		return errMark
	}

	log.Info().Msgf("[%s] sent %d message(s) to topic %s", jobId, len(ids), topic)
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
