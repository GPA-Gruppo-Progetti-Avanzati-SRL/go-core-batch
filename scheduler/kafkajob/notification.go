package kafkajob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/batchmetrics"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

const defaultKafkaLimit = 100

// Properties infrastrutturali del job.
//
// Il filtro sulla destinazione e il tetto per tick sono scheduler.PropDestination /
// scheduler.PropLimit: stesse chiavi degli altri job type.
const (
	// PropObject è il filtro sul WorkItem.ObjectType. ATTENZIONE: il job FeedTask chiama
	// `objectType` lo stesso campo (feedjob.PropObjectType) — due nomi in YAML per la stessa
	// colonna, divergenza storica che unificare sarebbe un breaking change di configurazione.
	PropObject = "object"
	// PropTopic è il topic su cui pubblicare.
	PropTopic = "topic"
)

type parametri struct {
	destination string
	object      string
	topic       string
	limit       int
}

func makeNotificationJobFactory(prod producer.IProducer, items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, config scheduler.Config) gocron.Task {
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
	j := scheduler.JobProps(name, config)
	var out parametri
	var err error
	for _, campo := range []struct {
		prop   string
		perche string
		dst    *string
	}{
		{scheduler.PropDestination, "non si sa quali item reclamare", &out.destination},
		{PropObject, "non si sa quali item reclamare", &out.object},
		{PropTopic, "non si sa su quale topic pubblicare", &out.topic},
	} {
		if *campo.dst, err = j.RequiredString(campo.prop, campo.perche); err != nil {
			return out, err
		}
	}
	if out.limit, err = j.PositiveInt(scheduler.PropLimit, defaultKafkaLimit); err != nil {
		return out, err
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
	native, ok := store.PayloadMap(item.Payload)
	if !ok {
		return nil, fmt.Errorf("payload di tipo non gestito: %T", item.Payload)
	}
	messageKey, ok := native[kafka.KeyMessageKey]
	if !ok {
		return nil, errors.New("messageKey mancante")
	}
	messageValue, ok := native[kafka.KeyMessageValue]
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
	if headersRaw, ok := native[kafka.KeyMessageHeaders]; ok {
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
