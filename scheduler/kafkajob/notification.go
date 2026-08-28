package kafkajob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("NotificationKafkaJob")

const defaultKafkaLimit = 100

func makeNotificationJobFactory(prod producer.IProducer, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		return scheduler.LabeledTask(name, config.Type, func() error {
			return notificationJobRun(name, prod, items, config)
		})
	}
}

func notificationJobRun(name string, prod producer.IProducer, items store.IWorkItemStore, config scheduler.Config) error {
	p := config.Properties
	jobId := jobID(name)

	if !p.Has("destination") {
		return errs.Tech(errs.CodeJobProperties).WithMessage("destination not found in properties")
	}
	destination := p.GetString("destination", "")
	if !p.Has("object") {
		return errs.Tech(errs.CodeJobProperties).WithMessage("object not found in properties")
	}
	object := p.GetString("object", "")
	if !p.Has("topic") {
		return errs.Tech(errs.CodeJobProperties).WithMessage("topic not found in properties")
	}
	topic := p.GetString("topic", "")

	limit := p.GetInt("limit", defaultKafkaLimit)

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

	valid, recs, invalid := prepareRecords(all)
	// Gli item con payload inutilizzabile sono marcati falliti UNO PER UNO (fenced dal token) e non
	// fanno cadere il tick: un payload malformato è un errore deterministico di quel singolo item, e
	// ritornare un errore per l'intero batch lascerebbe in IN_PROGRESS anche gli item buoni, fino al
	// recupero orfani.
	for _, item := range invalid {
		items.MarkFailed(spanCtx, item.Id, item.LockToken, "invalid payload")
	}
	if len(recs) == 0 {
		return nil
	}

	if errProduce := prod.ProduceTo(spanCtx, topic, recs); errProduce != nil {
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

// prepareRecords converte gli item in record Kafka, separando quelli con payload inutilizzabile.
// Ritorna gli item validi ALLINEATI ai record prodotti (servono i loro fencing token per i Mark*) e
// quelli da marcare falliti.
//
// La serializzazione di chiave e valore avviene QUI, per item, e non dentro il producer: un
// json.Marshal che fallisce è un difetto deterministico di quel payload — esattamente come un campo
// mancante — e va trattato come tale. Nel producer sarebbe stato un errore del batch, con gli item
// buoni fermi in IN_PROGRESS.
//
// La chiave è JSON-encoded, non la stringa nuda: è il formato storico di questo job, e cambiarlo
// cambierebbe il partizionamento di tutti i topic già in esercizio.
func prepareRecords(items []*store.WorkItem) (valid []*store.WorkItem, recs []*message.ProducerRecord, invalid []*store.WorkItem) {
	for _, item := range items {
		rec, err := toRecord(item)
		if err != nil {
			log.Error().Err(err).Msgf("Payload non utilizzabile per work item %s", item.Id)
			invalid = append(invalid, item)
			continue
		}
		recs = append(recs, rec)
		valid = append(valid, item)
	}
	log.Debug().Msgf("S - preparati %d record su %d work items", len(recs), len(items))
	return valid, recs, invalid
}

// toRecord traduce il payload di un WorkItem nel record da produrre. Il topic NON è impostato qui: lo
// mette ProduceTo dalla property del job, così il topic resta una decisione del job e non si ripete su
// ogni record.
func toRecord(item *store.WorkItem) (*message.ProducerRecord, error) {
	native, ok := normalizePayload(item.Payload)
	if !ok {
		return nil, fmt.Errorf("payload di tipo non gestito: %T", item.Payload)
	}
	messageKey, ok := native["messageKey"]
	if !ok {
		return nil, errors.New("messageKey mancante")
	}
	messageValue, ok := native["messageValue"]
	if !ok {
		return nil, errors.New("messageValue mancante")
	}
	key, err := json.Marshal(messageKey)
	if err != nil {
		return nil, fmt.Errorf("serializzazione di messageKey: %w", err)
	}
	value, err := json.Marshal(messageValue)
	if err != nil {
		return nil, fmt.Errorf("serializzazione di messageValue: %w", err)
	}
	rec := &message.ProducerRecord{Key: key, Value: value}
	if headersRaw, ok := native["messageHeaders"]; ok {
		headers, err := toStringMap(headersRaw)
		if err != nil {
			return nil, fmt.Errorf("mappatura di messageHeaders: %w", err)
		}
		// message.Headers è una LISTA perché Kafka ammette chiavi ripetute; il payload del WorkItem è
		// una mappa, quindi qui le chiavi sono per costruzione uniche.
		for k, v := range headers {
			rec.Headers.Add(k, v)
		}
	}
	return rec, nil
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
