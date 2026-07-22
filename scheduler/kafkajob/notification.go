package kafkajob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/kafkaproducer"
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

func makeNotificationJobFactory(producer *kafkaproducer.ProducerService, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return gocron.NewTask(func() error {
			return notificationJobRun(name, producer, items, config)
		})
	}
}

func notificationJobRun(name string, producer *kafkaproducer.ProducerService, items store.IWorkItemStore, config scheduler.Config) error {
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

	// 1+2. Recupero orfani + claim dei PENDING freschi — loop comune (store.ClaimBatch).
	all, norph, nfresh, appErr := store.ClaimBatch(spanCtx, items, jobId, JobType, destination, object, orphanTimeout, limit)
	if appErr != nil {
		span.RecordError(appErr)
		log.Error().Err(appErr).Msgf("[%s] ClaimPending failed", jobId)
		return appErr
	}
	if len(all) == 0 {
		log.Trace().Msgf("[%s] no pending items", jobId)
		return nil
	}
	log.Info().Msgf("[%s] processing %d item(s) (%d orphaned, %d fresh)", jobId, len(all), norph, nfresh)

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
		native, ok := normalizePayload(item.Payload)
		if !ok {
			log.Error().Msgf("Payload di tipo non gestito per work item %s: %T", item.Id, item.Payload)
			continue
		}
		messageKey, ok := native["messageKey"]
		if !ok {
			log.Error().Msgf("messageKey mancante in work item %s", item.Id)
			continue
		}
		messageValue, ok := native["messageValue"]
		if !ok {
			log.Error().Msgf("messageValue mancante in work item %s", item.Id)
			continue
		}
		km := &kafka.Message{MessageKey: messageKey, MessageValue: messageValue}
		if headersRaw, ok := native["messageHeaders"]; ok {
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

// bsonToNative converte ricorsivamente i tipi bson (D/M/A) in tipi JSON-native
// (map[string]interface{}, []interface{}), lasciando invariati gli scalari. Il Payload del WI,
// riletto da Mongo, arriva come bson.D: senza questa conversione json.Marshal(bson.D)
// produrrebbe un array [{Key,Value},...] invece di un oggetto, e gli header (bson.D) non
// sarebbero mappabili da toStringMap.
func bsonToNative(v interface{}) interface{} {
	switch t := v.(type) {
	case bson.D:
		m := make(map[string]interface{}, len(t))
		for _, e := range t {
			m[e.Key] = bsonToNative(e.Value)
		}
		return m
	case bson.M:
		m := make(map[string]interface{}, len(t))
		for k, val := range t {
			m[k] = bsonToNative(val)
		}
		return m
	case map[string]interface{}:
		m := make(map[string]interface{}, len(t))
		for k, val := range t {
			m[k] = bsonToNative(val)
		}
		return m
	case bson.A:
		a := make([]interface{}, len(t))
		for i, e := range t {
			a[i] = bsonToNative(e)
		}
		return a
	case []interface{}:
		a := make([]interface{}, len(t))
		for i, e := range t {
			a[i] = bsonToNative(e)
		}
		return a
	default:
		return v
	}
}

// normalizePayload porta il Payload del WI a map[string]interface{} indipendentemente dal
// backend: Mongo lo rilegge come bson.D, SQL (colonna jsonb) come map[string]interface{} o
// []byte; se salvato come stringa JSON viene deserializzato. Ritorna (nil,false) se non gestibile.
func normalizePayload(p interface{}) (map[string]interface{}, bool) {
	switch v := p.(type) {
	case bson.D, bson.M, map[string]interface{}:
		m, ok := bsonToNative(v).(map[string]interface{})
		return m, ok
	case []byte:
		var m map[string]interface{}
		if json.Unmarshal(v, &m) != nil {
			return nil, false
		}
		return m, true
	case string:
		var m map[string]interface{}
		if json.Unmarshal([]byte(v), &m) != nil {
			return nil, false
		}
		return m, true
	default:
		return nil, false
	}
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
