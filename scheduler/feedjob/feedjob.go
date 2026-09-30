// Package feedjob fornisce un job che crea UN work item per tick, descritto interamente nella
// configurazione del job. È il pezzo che mancava fra i job type del framework: ce ne sono che
// CONSUMANO work item (DistribuiteTask, NotificationKafka, simplejob) e uno che li PRODUCE da
// una query (DistribuiteTaskByQuery), ma nessuno che li produca da configurazione statica —
// e ogni applicazione che deve schedulare "questa cosa, a quest'ora" finiva per riscriverselo.
//
// Non reclama e non dispatcha: a lavorare l'item è un altro job, quello che serve il task di
// destinazione (un pickup simplejob, un DistribuiteTask, un worker pool). Tenerlo al solo feed
// è ciò che lo rende componibile con tutte e tre le famiglie senza trascinare un dispatcher.
//
// Wiring:
//
//	batch.Module(&svc.Batch, Register, …, batch.WithModule(feedjob.Module))
//
// Config:
//
//	tasks:
//	  - name: import-anagrafiche       # il task che ESEGUE, con il suo runner
//	    type: IMPORT
//	jobs:
//	  - name: feed-anagrafiche
//	    type: FeedTask
//	    cron: "0 0 3 * * *"            # il QUANDO sta solo qui
//	    singleton: true
//	    lock-timeout: 5m
//	    properties:
//	      task: import-anagrafiche     # dove accodare (deve esistere in `tasks:`)
//	      objectId: ANAGRAFICHE        # cosa accodare
//	      payload:                     # facoltativo
//	        modalita: completa
//
// La deduplica è quella di InsertIfNotActive: finché l'item creato dal tick precedente è
// PENDING o IN_PROGRESS non ne nasce un altro, quindi un'esecuzione che non finisce non fa
// accumulare coda. Richiede l'indice unico parziale sui work item (mongostore.EnsureIndexes o
// la migration SQL equivalente): senza, il duplicato non viene riconosciuto.
//
// ATTENZIONE ALLE CHIAVI DEL PAYLOAD: la config è letta con viper, che ABBASSA ricorsivamente
// le chiavi delle mappe. Un `payload: {idOrdine: X}` arriva quindi al work item come
// `idordine`, e chi lo rilegge deve farlo in modo case-insensitive. Il payload è opaco a
// questo job: viene copiato così com'è, senza conoscerne la forma.
package feedjob

import (
	"context"
	"strings"
	"time"
	"uuid"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog/log"
)

// JobType è il `type` da scrivere nella voce di `jobs:`.
const JobType = "FeedTask"

// Properties del job. Sono INFRASTRUTTURALI — le legge il framework, non il runner — come
// quelle di ogni altro job type.
//
// `task` (su quale istanza accodare) è scheduler.PropTask: stessa chiave, stesso YAML, una sola
// dichiarazione. Che il task esista lo verifica già il wiring (task.check): è un riferimento
// esplicito come quello di distributedjob e simplejob, e un nome sbagliato ferma l'avvio.
const (
	// PropObjectId è il WorkItem.ObjectId: identifica COSA accodare ed è la chiave su cui
	// l'indice unico parziale impedisce il duplicato.
	PropObjectId = "objectId"
	// PropPayload è il payload applicativo del work item, facoltativo. Copiato così com'è.
	PropPayload = "payload"
)

// Register costruisce la JobRegistration del job FeedTask. È un costruttore fx: il risultato
// confluisce nel value group batch_jobs via scheduler.ProvideJob.
func Register(items store.IWorkItemStore) scheduler.JobRegistration {
	return scheduler.JobRegistration{Type: JobType, Factory: makeFactory(items)}
}

// Module registra il job FeedTask. Se modes è vuoto registra sempre; altrimenti solo quando
// core.Mode è tra i modes indicati.
func Module(modes ...string) {
	scheduler.ProvideJob(Register, modes...)
}

func makeFactory(items store.IWorkItemStore) scheduler.JobFactory {
	return func(name string, config scheduler.Config) gocron.Task {
		return scheduler.LabeledTask(name, config.Type, func() error {
			return run(name, items, config)
		})
	}
}

func run(name string, items store.IWorkItemStore, config scheduler.Config) error {
	p := scheduler.JobProps(name, config)

	// Le due property obbligatorie si verificano PRIMA di toccare lo store: un errore di
	// configurazione deve dire quale property manca, non presentarsi come un guasto del
	// database. Vale anche quando lo store è irraggiungibile, che è il caso in cui la
	// confusione costa di più.
	taskName, err := p.RequiredString(scheduler.PropTask, "è il task su cui accodare")
	if err != nil {
		return err
	}
	objectId, err := p.RequiredString(PropObjectId, "è l'oggetto da accodare")
	if err != nil {
		return err
	}

	// Convenzione unica (scheduler.Config.ResolveTimeouts): il tick di un feed è una insert,
	// ma il timeout resta quello dichiarato dal job come per ogni altra famiglia.
	timeout, _ := config.ResolveTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	now := time.Now()
	item := &store.WorkItem{
		Id:         uuid.NewV7().String(),
		TaskName:   taskName,
		ObjectId:   objectId,
		Status:     store.StatusPending,
		CreateTime: now,
		NextRunAt:  &now,
	}
	if v, ok := valoreGrezzo(config.Properties, PropPayload); ok {
		item.Payload = v
	}

	inserted, appErr := items.InsertIfNotActive(ctx, []*store.WorkItem{item})
	if appErr != nil {
		log.Error().Err(appErr).Msgf("[%s] feed insert failed", name)
		return appErr
	}
	if inserted == 0 {
		// Non è un errore: esiste già un item PENDING o IN_PROGRESS per la stessa coppia
		// (task, objectId) — cioè l'esecuzione precedente non è finita. È esattamente ciò che
		// la deduplica deve impedire, ma va detto: è il sintomo di una lavorazione in ritardo.
		log.Warn().Msgf("[%s] item già attivo per task %q object %q: nessun work item creato",
			name, taskName, objectId)
		return nil
	}

	log.Info().Msgf("[%s] work item %s creato per task %q object %q", name, item.Id, taskName, objectId)
	return nil
}

// valoreGrezzo legge una property senza convertirla, con lo stesso confronto case-insensitive
// dei getter di properties.Properties (viper abbassa le chiavi, quindi l'indicizzazione diretta non
// basta). Serve perché il payload è OPACO: non ha un tipo da dichiarare, e i getter esportati
// sono tutti tipizzati.
func valoreGrezzo(p properties.Properties, key string) (any, bool) {
	if v, ok := p[key]; ok {
		return v, true
	}
	for k, v := range p {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
}
