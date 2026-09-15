package simplejob

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"go.uber.org/fx"
)

type fakeSvc struct{ name string }

// missingDep non è fornita da nessuno: se il runner che la dichiara finisse nel grafo fx, l'app non
// partirebbe. È così che verifichiamo che un task non referenziato non viene proprio istanziato.
type missingDep struct{}

// importRunner è la forma raccomandata: dipendenza taggata, properties del task, campo di lavorazione.
type importRunner struct {
	Svc *fakeSvc `inject:""`

	Folder string `prop:"folder" validate:"required"`
	Limit  int    `prop:"limit" default:"50"`

	scratch []byte
}

func (r *importRunner) Run(context.Context, *store.WorkItem) error { return nil }

type notifyRunner struct {
	Dep *missingDep `inject:""`
}

func (r *notifyRunner) Run(context.Context, *store.WorkItem) error { return nil }

// Un solo test costruisce davvero il grafo fx (il container di go-core-app è uno stato globale di
// processo): copre insieme istanze per voce di `tasks:`, properties per istanza, dipendenze
// condivise, task non referenziato — e il fatto che i runner registrati in modo AGNOSTICO
// (runner.Register, gruppo batch_runners) arrivino a SingleTask senza che nessuno li abbia
// registrati "per simplejob". È la cucitura fra i due perimetri, e sta qui perché è qui che si
// chiude.
func TestAgnosticRegistrationFeedsSingleTask(t *testing.T) {
	svc := &fakeSvc{name: "svc"}
	core.Supply(svc)
	core.ProvideAs[store.IWorkItemStore](func() *fakeStore { return &fakeStore{} })

	task.Apply(func() {
		runner.Register[importRunner]("Import")
		runner.Register[notifyRunner]("Notify") // nessun job/worker lo referenzia
	}, task.ActiveSet{
		Tasks: []task.Config{
			{Name: "import-in", Type: "Import", Properties: core.Properties{"folder": "/data/in"}},
			{Name: "import-bulk", Type: "Import", Properties: core.Properties{"folder": "/data/bulk", "limit": 500}},
			{Name: "notify-mail", Type: "Notify"},
		},
		Referenced: []string{"import-in", "import-bulk"},
	})
	Module()

	var runners []*runner.TaskRunner
	var jobs []scheduler.JobRegistration
	core.Invoke(func(p struct {
		fx.In
		Runners []*runner.TaskRunner        `group:"batch_runners"`
		Jobs    []scheduler.JobRegistration `group:"batch_jobs"`
	}) {
		runners, jobs = p.Runners, p.Jobs
	})

	app, err := core.Start(context.Background())
	if err != nil {
		t.Fatalf("il grafo fx deve costruirsi: %v", err)
	}
	defer func() { _ = app.Stop(context.Background()) }()

	if len(runners) != 2 {
		t.Fatalf("attese 2 istanze (una per voce di tasks: referenziata), ottenuto %d", len(runners))
	}
	byName := map[string]*importRunner{}
	for _, r := range runners {
		byName[r.TaskName] = r.Runner.(*importRunner)
	}
	in, bulk := byName["import-in"], byName["import-bulk"]
	if in == nil || bulk == nil {
		t.Fatalf("istanze attese import-in/import-bulk, ottenuto %v", byName)
	}
	if in.Folder != "/data/in" || in.Limit != 50 {
		t.Fatalf("properties della prima istanza errate: %+v", in)
	}
	if bulk.Folder != "/data/bulk" || bulk.Limit != 500 {
		t.Fatalf("properties della seconda istanza errate: %+v", bulk)
	}
	if in.Svc != svc || bulk.Svc != svc {
		t.Fatal("le due istanze devono condividere la dipendenza iniettata")
	}
	if in.scratch != nil {
		t.Fatal("il campo di lavorazione deve restare a zero")
	}
	// UNA JobRegistration, e il suo type è il JOB type: il task type non ne genera più una
	// propria, che era il punto in cui i due perimetri si confondevano.
	if len(jobs) != 1 || jobs[0].Type != JobType {
		t.Fatalf("attesa una sola JobRegistration di type %q, ottenuto %+v", JobType, jobs)
	}
}

// La registrazione è una sola, di type SingleTask, con le istanze indicizzate per NOME: è il
// nome che i job scrivono in `properties.task`.
func TestNewJobRegistration_SingleTypeRoutedByName(t *testing.T) {
	reg := newJobRegistration(&fakeStore{}, []*runner.TaskRunner{
		runner.New("import-in", &importRunner{}),
		runner.New("import-bulk", &importRunner{}),
	})
	if reg.Type != JobType {
		t.Fatalf("type = %q, atteso %q", reg.Type, JobType)
	}

	if reg.Factory == nil {
		t.Fatal("factory nil")
	}
}

// La risoluzione del task NON ha ripieghi: il vecchio "se manca `task` uso il type del job" è
// ciò che confondeva i perimetri, e un refuso finiva per eseguire silenziosamente altro.
func TestRisolvi_NienteRipieghi(t *testing.T) {
	instances := map[string]*runner.TaskRunner{"import-in": runner.New("import-in", &importRunner{})}

	t.Run("task noto", func(t *testing.T) {
		nome, tr, err := risolvi("j", instances, cfg(core.Properties{PropTask: "import-in"}))
		if err != nil || nome != "import-in" || tr == nil {
			t.Fatalf("nome=%q tr=%v err=%v", nome, tr, err)
		}
	})

	t.Run("property mancante", func(t *testing.T) {
		_, _, err := risolvi("j", instances, cfg(core.Properties{}))
		if err == nil || !strings.Contains(err.Error(), PropTask) {
			t.Fatalf("atteso un errore che nomini %q: %v", PropTask, err)
		}
	})

	// Il job type NON è più un ripiego: un job che si chiamasse come il task non lo eseguirebbe.
	t.Run("niente ripiego sul job type", func(t *testing.T) {
		c := cfg(core.Properties{})
		c.Type = "import-in"
		if _, _, err := risolvi("import-in", instances, c); err == nil {
			t.Error("il type del job non deve valere come nome del task")
		}
	})

	t.Run("task sconosciuto", func(t *testing.T) {
		_, _, err := risolvi("j", instances, cfg(core.Properties{PropTask: "boh"}))
		if err == nil || !strings.Contains(err.Error(), "boh") {
			t.Fatalf("atteso un errore che nomini il task: %v", err)
		}
	})
}

func cfg(props core.Properties) scheduler.Config {
	return scheduler.Config{Name: "j", Type: JobType, LockTimeout: time.Minute, Properties: props}
}

// fakeStore: tutti no-op, serve solo a soddisfare il grafo.
type fakeStore struct{}

func (*fakeStore) GetById(context.Context, string) (*store.WorkItem, *core.ApplicationError) {
	return &store.WorkItem{Id: "obj"}, nil
}
func (*fakeStore) MarkDone(context.Context, []string, string) *core.ApplicationError { return nil }
func (*fakeStore) MarkFailed(context.Context, string, string, string) *core.ApplicationError {
	return nil
}
func (*fakeStore) MarkPending(context.Context, string, string, time.Duration) *core.ApplicationError {
	return nil
}
func (*fakeStore) FindPending(context.Context, string, string, string) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (*fakeStore) ClaimPending(context.Context, string, string, string, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (*fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (*fakeStore) Insert(context.Context, []*store.WorkItem) *core.ApplicationError { return nil }
func (*fakeStore) InsertIfNotActive(context.Context, []*store.WorkItem) (int, *core.ApplicationError) {
	return 0, nil
}
func (*fakeStore) HasActive(context.Context, string, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (*fakeStore) DeleteIfPending(context.Context, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (*fakeStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
