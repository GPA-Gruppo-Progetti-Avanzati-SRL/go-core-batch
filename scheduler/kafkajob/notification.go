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
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

const defaultKafkaLimit = 100

// Properties infrastrutturali del job. Il tetto per tick è scheduler.PropLimit: stessa chiave
// degli altri job type.
const (
	// PropStream nomina il FLUSSO di notifiche che questo job drena, e finisce in
	// WorkItem.TaskName: è la coda che il claim filtra e, insieme a ObjectId, la chiave su cui
	// l'indice unico parziale deduplica gli accodamenti.
	//
	// Non si chiama `task` di proposito. `task` è un riferimento ESPLICITO a una voce di `tasks:`
	// (batch.ActiveSet lo mette in Referenced, e task.check pretende che esista), mentre una
	// notifica non ha un runner da configurare: pretenderne la dichiarazione sarebbe un falso
	// positivo. Con `stream` il job type resta fra gli Implied, che non sono validati.
	//
	// Prima il claim girava su TaskName = "NotificationKafka", uguale per OGNI notifica dell'app:
	// per distinguere i flussi servivano due filtri in più (`destination` e `object`, spariti con
	// questa property) e la deduplica finiva in un namespace unico — due flussi diversi sullo
	// stesso objectId collidevano, e InsertIfNotActive scartava il secondo in silenzio.
	PropStream = "stream"
	// PropTopic è il topic di DEFAULT su cui pubblicare: vale per i record che non ne portano uno
	// proprio (kafka.Message.Topic). È facoltativa, ma se manca ogni item deve nominare il suo,
	// altrimenti quel singolo item è un payload inutilizzabile.
	PropTopic = "topic"
	// PropMaxRetry è il tetto ai ritentativi di un item, con la stessa convenzione di
	// task.Config.MaxRetry: assente = illimitato, che è la condotta storica.
	//
	// Serve perché su questo percorso store.ApplyResult non passa mai — kafkajob chiama i Mark*
	// da sé — quindi WorkItem.Retry veniva incrementato (da MarkPending e da RecoverOrphans) e
	// non letto da nessuno: una notifica irrecuperabile ritentava per sempre e occupava uno slot
	// del `limit` a ogni tick, rubando capacità a quelle sane.
	PropMaxRetry = "max-retry"
)

type parametri struct {
	stream   string
	topic    string
	limit    int
	maxRetry int
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
		TaskName:      p.stream,
		Limit:         p.limit,
		RunTimeout:    runTimeout,
		OrphanTimeout: orphanTimeout,
		Backlog:       backlog,
		Process: func(ctx context.Context, jobID string, batch []*store.WorkItem) error {
			return publishBatch(ctx, name, jobID, p, batch, prod, items)
		},
	}.Run(items)
}

func risolvi(name string, config scheduler.Config) (parametri, error) {
	j := scheduler.JobProps(name, config)
	var out parametri
	var err error
	if out.stream, err = j.RequiredString(PropStream, "non si sa quale flusso di notifiche reclamare"); err != nil {
		return out, err
	}
	// Il topic è FACOLTATIVO: è il default dei record che non ne portano uno. Se manca, ogni item
	// deve nominare il proprio — e se non lo fa è quel singolo item a fallire, non l'avvio.
	out.topic = j.String(PropTopic, "")
	if out.limit, err = j.PositiveInt(scheduler.PropLimit, defaultKafkaLimit); err != nil {
		return out, err
	}
	if out.maxRetry, err = risolviMaxRetry(j, config); err != nil {
		return out, err
	}
	return out, nil
}

// risolviMaxRetry legge il tetto con la convenzione di task.Config.MaxRetry: assente o -1 =
// illimitato, >= 0 = il tetto (0 = nessun ritentativo).
//
// Un valore NON convertibile non può ricadere sul default, perché il default è l'illimitato: un
// refuso spegnerebbe in silenzio proprio il controllo che si stava cercando di accendere. GetInt
// ritorna il default sia quando la chiave è assente sia quando il valore non si converte, e i due
// casi si distinguono solo interrogandola con due default diversi.
func risolviMaxRetry(j scheduler.Props, config scheduler.Config) (int, error) {
	if !j.Has(PropMaxRetry) {
		return task.MaxRetryUnlimited, nil
	}
	if a, b := config.Properties.GetInt(PropMaxRetry, 0), config.Properties.GetInt(PropMaxRetry, 1); a != b {
		return 0, j.Invalid("la property %q non è un intero: %v (ometterla, o -1, significa illimitato)",
			PropMaxRetry, config.Properties[PropMaxRetry])
	}
	n := config.Properties.GetInt(PropMaxRetry, task.MaxRetryUnlimited)
	if n < task.MaxRetryUnlimited {
		return 0, j.Invalid("la property %q non può essere < -1: %v (-1 = illimitato)", PropMaxRetry, n)
	}
	return n, nil
}

// publishBatch è la fase di elaborazione di questa famiglia: traduce gli item in record e li
// pubblica in blocco sul topic.
func publishBatch(ctx context.Context, name, jobId string, p parametri, all []*store.WorkItem,
	prod producer.IProducer, items store.IWorkItemStore) error {

	// Inizio della fase di elaborazione: è la finestra che le istogrammi misurano.
	itemsStart := time.Now()

	// Il tetto si applica PRIMA di pubblicare, e sul batch appena claimato: è l'unico punto in cui
	// copre anche il percorso degli orfani, che incrementa `retry` senza passare da qui. Un item
	// oltre il tetto è finalizzato e non pubblicato — altrimenti resterebbe a occupare uno slot
	// del `limit` a ogni tick, per sempre.
	all, esauriti := separaEsauriti(all, p.maxRetry)
	for _, item := range esauriti {
		motivo := fmt.Sprintf("max-retry %d esaurito (retry=%d)", p.maxRetry, item.Retry)
		if errMark := items.MarkFailed(ctx, item.Id, item.LockToken, motivo); errMark != nil {
			log.Error().Err(errMark).Msgf("[%s] MarkFailed fallito per l'item %s", jobId, item.Id)
		}
	}

	valid, recs, invalid := prepareRecords(all, p.topic)
	// Gli item con payload inutilizzabile sono marcati falliti UNO PER UNO (fenced dal token) e non
	// fanno cadere il tick: un payload malformato è un errore deterministico di quel singolo item, e
	// ritornare un errore per l'intero batch lascerebbe in IN_PROGRESS anche gli item buoni, fino al
	// recupero orfani.
	for _, item := range invalid {
		if errMark := items.MarkFailed(ctx, item.Id, item.LockToken, "invalid payload"); errMark != nil {
			log.Error().Err(errMark).Msgf("[%s] MarkFailed fallito per l'item %s", jobId, item.Id)
		}
	}
	// Invalidi ed esauriti sono item finalizzati come falliti, quindi vanno contati in OGNI esito
	// del tick e non solo quando sono tutti tali: altrimenti un tick misto ne perderebbe la
	// traccia, e batch_job_items_claimed_total non tornerebbe con la somma dei processed.
	observeItems(name, len(invalid)+len(esauriti), store.OutcomeFailed, itemsStart)
	if len(recs) == 0 {
		return nil
	}

	if errProduce := prod.ProduceTo(ctx, p.topic, recs); errProduce != nil {
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

	log.Info().Msgf("[%s] sent %d message(s) (topic di default %q)", jobId, len(valid), p.topic)
	return nil
}

// separaEsauriti divide il batch fra gli item ancora entro il tetto dei ritentativi e quelli che
// l'hanno superato. maxRetry negativo (task.MaxRetryUnlimited) disattiva il tetto, che è la
// condotta di chi non scrive la property.
//
// Il confronto è `>=` come in store.ApplyResult: `max-retry: N` concede N ritentativi, cioè N+1
// esecuzioni in tutto.
func separaEsauriti(all []*store.WorkItem, maxRetry int) (vivi, esauriti []*store.WorkItem) {
	if maxRetry < 0 {
		return all, nil
	}
	for _, item := range all {
		if item.Retry >= maxRetry {
			esauriti = append(esauriti, item)
			continue
		}
		vivi = append(vivi, item)
	}
	return vivi, esauriti
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
func prepareRecords(items []*store.WorkItem, defaultTopic string) (valid []*store.WorkItem, recs []*message.ProducerRecord, invalid []*store.WorkItem) {
	for _, item := range items {
		rec, err := toRecord(item, defaultTopic)
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

// toRecord traduce il payload di un WorkItem nel record da produrre.
//
// Il topic lo porta il record solo se l'item lo nomina (kafka.KeyTopic); altrimenti resta vuoto e
// lo stampa ProduceTo dalla property del job, che sovrascrive i soli record senza topic. Con
// entrambi assenti non c'è destinazione, e l'item è inutilizzabile come lo sarebbe senza chiave:
// meglio un MarkFailed che nomina il difetto di una Produce su topic vuoto.
func toRecord(item *store.WorkItem, defaultTopic string) (*message.ProducerRecord, error) {
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
	if topicRaw, ok := native[kafka.KeyTopic]; ok {
		topic, ok := topicRaw.(string)
		if !ok {
			return nil, fmt.Errorf("topic di tipo non gestito: %T", topicRaw)
		}
		rec.Topic = topic
	}
	if rec.Topic == "" && defaultTopic == "" {
		return nil, fmt.Errorf("nessun topic: l'item non ne porta uno e il job non ha la property %q", PropTopic)
	}
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
