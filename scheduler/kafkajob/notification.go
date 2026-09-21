package kafkajob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const defaultKafkaLimit = 100

// Properties infrastrutturali del job.
const (
	// PropDestination e PropObject sono i filtri di claim: un job per coppia.
	PropDestination = "destination"
	PropObject      = "object"
	// PropTopic è il topic su cui pubblicare.
	PropTopic = "topic"
	// PropLimit è il tetto agli item claimati per tick.
	PropLimit = "limit"
)

type parametri struct {
	destination string
	object      string
	topic       string
	limit       int
}

func makeNotificationJobFactory(prod producer.IProducer, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, s *scheduler.Services, config scheduler.Config) gocron.Task {
		p, resolveErr := risolvi(name, config)
		if resolveErr != nil {
			// Come per le altre famiglie: un job che non può funzionare si vede all'avvio, non
			// al primo tick. Prima queste tre property erano verificate DENTRO il tick.
			log.Error().Err(resolveErr).Msgf("[%s] il job fallirà a ogni tick", name)
		}
		runTimeout, orphanTimeout := config.ResolveTimeouts()
		backlog := config.Properties.GetBool(scheduler.PropBacklogMetrics, false)
		return scheduler.LabeledTask(name, config.Type, func() error {
			if resolveErr != nil {
				return resolveErr
			}
			return runTick(name, p, runTimeout, orphanTimeout, backlog, prod, items)
		})
	}
}

// notificationJobRun esegue UN tick risolvendo la config al momento. È la forma usata dai test:
// il percorso di esercizio passa dalla factory, che la config la risolve una volta sola all'avvio.
func notificationJobRun(name string, prod producer.IProducer, items store.IWorkItemStore, config scheduler.Config) error {
	p, err := risolvi(name, config)
	if err != nil {
		return err
	}
	runTimeout, orphanTimeout := config.ResolveTimeouts()
	return runTick(name, p, runTimeout, orphanTimeout,
		config.Properties.GetBool(scheduler.PropBacklogMetrics, false), prod, items)
}

// runTick è il tick: claim comune (scheduler.ClaimingTick) più la fase di publish, che è l'unica
// cosa specifica di questa famiglia.
func runTick(name string, p parametri, runTimeout, orphanTimeout time.Duration, backlog bool,
	prod producer.IProducer, items store.IWorkItemStore) error {

	return scheduler.ClaimingTick{
		JobName:       name,
		JobType:       JobType,
		TaskName:      JobType,
		Destination:   p.destination,
		ObjectType:    p.object,
		Limit:         p.limit,
		RunTimeout:    runTimeout,
		OrphanTimeout: orphanTimeout,
		Backlog:       backlog,
		Process: func(ctx context.Context, jobID string, batch []*store.WorkItem) error {
			return publishBatch(ctx, name, jobID, p.topic, batch, prod, items)
		},
	}.Run(items)
}

func risolvi(name string, config scheduler.Config) (parametri, error) {
	p := config.Properties
	var out parametri
	for _, campo := range []struct {
		prop string
		dst  *string
	}{
		{PropDestination, &out.destination},
		{PropObject, &out.object},
		{PropTopic, &out.topic},
	} {
		if !p.Has(campo.prop) {
			return out, errs.Tech(errs.CodeJobProperties).WithMessage(
				fmt.Sprintf("kafkajob: job %q senza la property %q", name, campo.prop))
		}
		*campo.dst = p.GetString(campo.prop, "")
		if *campo.dst == "" {
			return out, errs.Tech(errs.CodeJobProperties).WithMessage(
				fmt.Sprintf("kafkajob: job %q: la property %q è vuota", name, campo.prop))
		}
	}
	out.limit = p.GetInt(PropLimit, defaultKafkaLimit)
	if out.limit <= 0 {
		return out, errs.Tech(errs.CodeJobProperties).WithMessage(
			fmt.Sprintf("kafkajob: job %q: la property %q non è un intero positivo: %v", name, PropLimit, p[PropLimit]))
	}
	return out, nil
}

// publishBatch è la fase di elaborazione di questa famiglia: traduce gli item in record e li
// pubblica in blocco sul topic.
func publishBatch(ctx context.Context, name, jobId, topic string, all []*store.WorkItem,
	prod producer.IProducer, items store.IWorkItemStore) error {

	// Inizio della fase di elaborazione: è la finestra che le istogrammi misurano.
	itemsStart := time.Now()

	valid, recs, invalid := prepareRecords(all)
	// Gli item con payload inutilizzabile sono marcati falliti UNO PER UNO (fenced dal token) e non
	// fanno cadere il tick: un payload malformato è un errore deterministico di quel singolo item, e
	// ritornare un errore per l'intero batch lascerebbe in IN_PROGRESS anche gli item buoni, fino al
	// recupero orfani.
	for _, item := range invalid {
		if errMark := items.MarkFailed(ctx, item.Id, item.LockToken, "invalid payload"); errMark != nil {
			log.Error().Err(errMark).Msgf("[%s] MarkFailed fallito per l'item %s", jobId, item.Id)
		}
	}
	// Gli invalidi sono item finalizzati come falliti, quindi vanno contati in OGNI esito del tick e
	// non solo quando sono tutti invalidi: altrimenti un tick misto ne perderebbe la traccia, e
	// batch_job_items_claimed_total non tornerebbe con la somma dei processed.
	observeItems(name, len(invalid), store.OutcomeFailed, itemsStart)
	if len(recs) == 0 {
		return nil
	}

	if errProduce := prod.ProduceTo(ctx, topic, recs); errProduce != nil {
		log.Error().Err(errProduce).Msgf("[%s] Kafka produce failed — resetting %d items to PENDING", jobId, len(valid))
		// Errore transiente: gli item claimati tornano PENDING e il tick successivo li riprende.
		// Il delay è 0 — quando riprovare lo decide il cron del job, non il producer: l'errore che
		// arriva qui è un *core.ApplicationError di go-core-kafka, che non conosce (né potrebbe
		// conoscere) store.RetryError.
		for _, item := range valid {
			if errMark := items.MarkPending(ctx, item.Id, item.LockToken, 0); errMark != nil {
				log.Error().Err(errMark).Msgf("[%s] MarkPending fallito per l'item %s", jobId, item.Id)
			}
		}
		observeItems(name, len(valid), store.OutcomeRetry, itemsStart)
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
		if errMark := items.MarkDone(ctx, doneIds, token); errMark != nil {
			log.Error().Err(errMark).Msgf("[%s] MarkDone fallito per %d item", jobId, len(doneIds))
		}
	}

	observeItems(name, len(valid), store.OutcomeDone, itemsStart)

	log.Info().Msgf("[%s] sent %d message(s) to topic %s", jobId, len(valid), topic)
	return nil
}

// observeItems emette le metriche di task e di job per n item che hanno condiviso lo stesso
// esito. kafkajob produce in blocco, quindi una durata per singolo item non esiste: tutte le
// osservazioni partono dallo stesso start e registrano la latenza del batch — è l'unica lettura
// onesta possibile. Incrementa TaskStarted direttamente (e non via batchmetrics.TaskStart) per
// non far ripartire il cronometro a ogni item.
func observeItems(job string, n int, outcome store.Outcome, start time.Time) {
	for range n {
		batchmetrics.TaskStarted.WithLabelValues(JobType).Inc()
		batchmetrics.ObserveTask(JobType, outcome, start)
		batchmetrics.JobProcessed(job, JobType, outcome, start)
	}
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
