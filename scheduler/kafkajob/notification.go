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

const defaultKafkaLimit = 100

func makeNotificationJobFactory(producer *kafkaproducer.ProducerService, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return scheduler.LabeledTask(name, config.Type, func() error {
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

	// Convenzione unica (scheduler.Config.ResolveTimeouts): LockTimeout governa sia il timeout
	// del context di run sia l'età di orphan. Prima il run usava il knob ad-hoc properties["timeout"].
	runTimeout, orphanTimeout := config.ResolveTimeouts()

	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
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

	valid, kafkaMsgs := prepareMessages(all)
	if len(kafkaMsgs) == 0 {
		// all items had invalid payload — mark them failed individually (fenced dal token)
		for _, item := range all {
			items.MarkFailed(spanCtx, item.Id, item.LockToken, "invalid payload")
		}
		return nil
	}

	if errProduce := producer.ProduceMessages(spanCtx, kafkaMsgs, topic); errProduce != nil {
		span.RecordError(errProduce)
		log.Error().Err(errProduce).Msgf("[%s] Kafka produce failed — resetting %d items to PENDING", jobId, len(valid))
		// Transient Kafka failure — reset claimed items so they are retried next tick.
		var after time.Duration
		if retryErr, ok := errors.AsType[*store.RetryError](errProduce); ok {
			after = retryErr.After
		}
		for _, item := range valid {
			items.MarkPending(spanCtx, item.Id, item.LockToken, after)
		}
		return errProduce
	}

	// At-least-once: se un MarkDone non matcha (token stale) l'item verrà ri-inviato al tick
	// successivo. MarkDone è batch + fenced: gli item di un tick hanno al più 2 token (gruppo
	// orfani + gruppo fresh), quindi li raggruppiamo per token e facciamo ≤2 update invece di N.
	byToken := make(map[string][]string, 2)
	for _, item := range valid {
		byToken[item.LockToken] = append(byToken[item.LockToken], item.Id)
	}
	for token, doneIds := range byToken {
		if errMark := items.MarkDone(spanCtx, doneIds, token); errMark != nil {
			span.RecordError(errMark)
			log.Error().Err(errMark).Msgf("[%s] MarkDone fallito per %d item", jobId, len(doneIds))
		}
	}

	log.Info().Msgf("[%s] sent %d message(s) to topic %s", jobId, len(valid), topic)
	return nil
}

// prepareMessages ritorna gli item con payload valido (allineati ai messaggi Kafka prodotti):
// restituire gli item — non solo gli id — permette ai Mark* di usare il fencing token di ciascuno.
func prepareMessages(items []*store.WorkItem) ([]*store.WorkItem, []*kafka.Message) {
	var valid []*store.WorkItem
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
		valid = append(valid, item)
	}
	log.Debug().Msgf("S - preparati %d messaggi su %d work items", len(valid), len(items))
	return valid, out
}

// bsonToNative converte ricorsivamente i tipi bson (D/M/A) in tipi JSON-native
// (map[string]interface{}, []interface{}), lasciando invariati gli scalari. Il Payload del WI,
// riletto da Mongo, arriva come bson.D: senza questa conversione json.Marshal(bson.D)
// produrrebbe un array [{Key,Value},...] invece di un oggetto, e gli header (bson.D) non
// sarebbero mappabili da toStringMap.
func bsonToNative(v any) any {
	switch t := v.(type) {
	case bson.D:
		m := make(map[string]any, len(t))
		for _, e := range t {
			m[e.Key] = bsonToNative(e.Value)
		}
		return m
	case bson.M:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = bsonToNative(val)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = bsonToNative(val)
		}
		return m
	case bson.A:
		a := make([]any, len(t))
		for i, e := range t {
			a[i] = bsonToNative(e)
		}
		return a
	case []any:
		a := make([]any, len(t))
		for i, e := range t {
			a[i] = bsonToNative(e)
		}
		return a
	default:
		return v
	}
}

// normalizePayload porta il Payload del WI a map[string]any indipendentemente dal
// backend: Mongo lo rilegge come bson.D, SQL (colonna jsonb) come map[string]any o
// []byte; se salvato come stringa JSON viene deserializzato. Ritorna (nil,false) se non gestibile.
func normalizePayload(p any) (map[string]any, bool) {
	switch v := p.(type) {
	case bson.D, bson.M, map[string]any:
		m, ok := bsonToNative(v).(map[string]any)
		return m, ok
	case []byte:
		var m map[string]any
		if json.Unmarshal(v, &m) != nil {
			return nil, false
		}
		return m, true
	case string:
		var m map[string]any
		if json.Unmarshal([]byte(v), &m) != nil {
			return nil, false
		}
		return m, true
	default:
		return nil, false
	}
}

func toStringMap(input any) (map[string]string, error) {
	m, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any, got %T", input)
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
