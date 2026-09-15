package feedjob

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// storeFake implementa il solo InsertIfNotActive: l'interfaccia incorporata è nil di
// proposito, così se il job chiamasse altro il test andrebbe in panico invece di passare.
type storeFake struct {
	store.IWorkItemStore
	inserted []*store.WorkItem
	// created è quanti item InsertIfNotActive dichiara di aver creato: 0 simula l'item già
	// attivo, cioè l'esecuzione precedente non ancora finita.
	created *int
	err     *core.ApplicationError
}

func (s *storeFake) InsertIfNotActive(_ context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if s.err != nil {
		return 0, s.err
	}
	s.inserted = append(s.inserted, items...)
	if s.created != nil {
		return *s.created, nil
	}
	return len(items), nil
}

func config(props core.Properties) scheduler.Config {
	return scheduler.Config{Name: "feed-test", Type: JobType, LockTimeout: time.Minute, Properties: props}
}

func TestCreaIlWorkItem(t *testing.T) {
	st := &storeFake{}
	// Le chiavi arrivano come viper le lascia: minuscole per il payload scritto nello YAML.
	payload := map[string]any{"modalita": "completa", "tentativi": 3}

	err := run("feed-test", st, config(core.Properties{
		PropTask:        "import-anagrafiche",
		PropObjectId:    "ANAGRAFICHE",
		PropObjectType:  "anagrafica",
		PropDestination: "milano",
		PropPayload:     payload,
	}))
	if err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}

	if len(st.inserted) != 1 {
		t.Fatalf("creati %d work item, atteso 1", len(st.inserted))
	}
	wi := st.inserted[0]
	if wi.TaskName != "import-anagrafiche" || wi.ObjectId != "ANAGRAFICHE" {
		t.Errorf("task %q, objectId %q", wi.TaskName, wi.ObjectId)
	}
	if wi.ObjectType != "anagrafica" || wi.Destination != "milano" {
		t.Errorf("objectType %q, destination %q", wi.ObjectType, wi.Destination)
	}
	if wi.Status != store.StatusPending {
		t.Errorf("stato = %q, atteso %q", wi.Status, store.StatusPending)
	}
	// NextRunAt valorizzato è ciò che rende l'item claimabile subito: senza, il consumatore
	// lo vedrebbe comunque (null = dovuto ora), ma la coerenza con gli altri feed conta.
	if wi.NextRunAt == nil || wi.CreateTime.IsZero() {
		t.Errorf("createTime %v, nextRunAt %v", wi.CreateTime, wi.NextRunAt)
	}
	if wi.Id == "" {
		t.Error("l'id del work item non può essere vuoto")
	}

	// Il payload è copiato così com'è: il job non lo interpreta e non lo riscrive.
	got, ok := wi.Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload di tipo %T", wi.Payload)
	}
	if got["modalita"] != "completa" || got["tentativi"] != 3 {
		t.Errorf("payload = %+v", got)
	}
}

// Senza payload il work item è nudo: è la forma che vale "esegui questo oggetto", e il
// consumatore ricava il resto da ObjectId e CreateTime.
func TestSenzaPayload(t *testing.T) {
	st := &storeFake{}

	if err := run("feed-test", st, config(core.Properties{
		PropTask:     "import-anagrafiche",
		PropObjectId: "ANAGRAFICHE",
	})); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}

	if st.inserted[0].Payload != nil {
		t.Errorf("payload = %v, atteso nil", st.inserted[0].Payload)
	}
}

// viper abbassa le chiavi della configurazione: le property devono essere lette lo stesso.
func TestPropertyConChiaviAbbassate(t *testing.T) {
	st := &storeFake{}

	if err := run("feed-test", st, config(core.Properties{
		"task":       "import-anagrafiche",
		"objectid":   "ANAGRAFICHE",
		"objecttype": "anagrafica",
		"payload":    map[string]any{"modalita": "completa"},
	})); err != nil {
		t.Fatalf("errore inatteso: %v", err)
	}

	wi := st.inserted[0]
	if wi.ObjectId != "ANAGRAFICHE" || wi.ObjectType != "anagrafica" || wi.Payload == nil {
		t.Errorf("work item = %+v", wi)
	}
}

// Una property obbligatoria mancante è un errore di CONFIGURAZIONE, e il messaggio deve
// nominarla: altrimenti si va a cercare il guasto nel database.
func TestPropertyObbligatorieMancanti(t *testing.T) {
	for nome, props := range map[string]core.Properties{
		"senza task":     {PropObjectId: "ANAGRAFICHE"},
		"senza objectId": {PropTask: "import-anagrafiche"},
		"task vuoto":     {PropTask: "", PropObjectId: "ANAGRAFICHE"},
		"objectId vuoto": {PropTask: "import-anagrafiche", PropObjectId: ""},
	} {
		t.Run(nome, func(t *testing.T) {
			st := &storeFake{}
			err := run("feed-test", st, config(props))
			if err == nil {
				t.Fatal("atteso un errore")
			}
			atteso := PropTask
			if strings.Contains(nome, "objectId") {
				atteso = PropObjectId
			}
			if !strings.Contains(err.Error(), atteso) {
				t.Errorf("il messaggio non nomina %q: %v", atteso, err)
			}
			if len(st.inserted) != 0 {
				t.Error("nessun work item deve essere creato")
			}
		})
	}
}

// L'item già attivo non è un errore: è la deduplica che fa il suo mestiere. Il tick deve
// finire bene, altrimenti un'esecuzione lunga farebbe fallire tutti i tick successivi.
func TestItemGiaAttivo(t *testing.T) {
	zero := 0
	st := &storeFake{created: &zero}

	if err := run("feed-test", st, config(core.Properties{
		PropTask:     "import-anagrafiche",
		PropObjectId: "ANAGRAFICHE",
	})); err != nil {
		t.Fatalf("un item già attivo non è un errore: %v", err)
	}
}

// Lo store che non risponde, invece, è un errore del tick: gocron lo riproverà al cron
// successivo e l'esito finisce nei log del job.
func TestStoreInErrore(t *testing.T) {
	st := &storeFake{err: core.TechnicalError().WithMessage("mongo giù")}

	err := run("feed-test", st, config(core.Properties{
		PropTask:     "import-anagrafiche",
		PropObjectId: "ANAGRAFICHE",
	}))
	if err == nil {
		t.Fatal("atteso un errore")
	}
	var appErr *core.ApplicationError
	if !errors.As(err, &appErr) {
		t.Errorf("atteso un *core.ApplicationError, ottenuto %T", err)
	}
}

// La registrazione deve dichiarare il job type con cui si scrive `jobs:`.
func TestRegister(t *testing.T) {
	reg := Register(&storeFake{})
	if reg.Type != JobType {
		t.Errorf("type = %q, atteso %q", reg.Type, JobType)
	}
	if reg.Factory == nil {
		t.Fatal("factory nil")
	}
	if task := reg.Factory("feed-test", nil, config(core.Properties{
		PropTask: "import-anagrafiche", PropObjectId: "ANAGRAFICHE",
	})); task == nil {
		t.Error("la factory deve costruire una gocron.Task")
	}
}
