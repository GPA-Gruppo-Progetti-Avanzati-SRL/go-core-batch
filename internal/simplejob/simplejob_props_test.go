package simplejob

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskreg"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
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

	taskreg.Apply(func() {
		runner.Register[importRunner]("Import")
		runner.Register[notifyRunner]("Notify") // nessun job/worker lo referenzia
	}, taskreg.ActiveSet{
		Tasks: []task.Config{
			{Name: "import-in", Type: "Import", Properties: properties.Properties{"folder": "/data/in"}},
			{Name: "import-bulk", Type: "Import", Properties: properties.Properties{"folder": "/data/bulk", "limit": 500}},
			{Name: "notify-mail", Type: "Notify"},
		},
		Referenced: []string{"import-in", "import-bulk"},
		Executed:   []string{"import-in", "import-bulk"},
	})
	Module()

	var runners []*taskrunner.TaskRunner
	var jobs []scheduler.JobRegistration
	core.Invoke(func(p struct {
		fx.In
		Runners []*taskrunner.TaskRunner    `group:"batch_runners"`
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
	reg := newJobRegistration(&fakeStore{}, []*taskrunner.TaskRunner{
		taskrunner.New("import-in", &importRunner{}),
		taskrunner.New("import-bulk", &importRunner{}),
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
	instances := map[string]*taskrunner.TaskRunner{"import-in": taskrunner.New("import-in", &importRunner{})}

	t.Run("task noto", func(t *testing.T) {
		nome, tr, err := risolvi("j", instances, cfg(properties.Properties{scheduler.PropTask: "import-in"}))
		if err != nil || nome != "import-in" || tr == nil {
			t.Fatalf("nome=%q tr=%v err=%v", nome, tr, err)
		}
	})

	t.Run("property mancante", func(t *testing.T) {
		_, _, err := risolvi("j", instances, cfg(properties.Properties{}))
		if err == nil || !strings.Contains(err.Error(), scheduler.PropTask) {
			t.Fatalf("atteso un errore che nomini %q: %v", scheduler.PropTask, err)
		}
	})

	// Il job type NON è più un ripiego: un job che si chiamasse come il task non lo eseguirebbe.
	t.Run("niente ripiego sul job type", func(t *testing.T) {
		c := cfg(properties.Properties{})
		c.Type = "import-in"
		if _, _, err := risolvi("import-in", instances, c); err == nil {
			t.Error("il type del job non deve valere come nome del task")
		}
	})

	t.Run("task sconosciuto", func(t *testing.T) {
		_, _, err := risolvi("j", instances, cfg(properties.Properties{scheduler.PropTask: "boh"}))
		if err == nil || !strings.Contains(err.Error(), "boh") {
			t.Fatalf("atteso un errore che nomini il task: %v", err)
		}
	})
}

func cfg(props properties.Properties) scheduler.Config {
	return scheduler.Config{Name: "j", Type: JobType, LockTimeout: time.Minute, Properties: props}
}

// fakeStore: tutti no-op, serve solo a soddisfare il grafo.
type fakeStore struct{}

func (*fakeStore) GetById(context.Context, string) (*store.WorkItem, *core.Error) {
	return &store.WorkItem{Id: "obj"}, nil
}
func (*fakeStore) MarkDone(context.Context, []string, string) *core.Error { return nil }
func (*fakeStore) MarkFailed(context.Context, string, string, string) *core.Error {
	return nil
}
func (*fakeStore) MarkPending(context.Context, string, string, time.Duration) *core.Error {
	return nil
}
func (*fakeStore) Release(context.Context, string, string) *core.Error { return nil }
func (*fakeStore) Purge(context.Context, string, time.Time, int) (int, *core.Error) {
	return 0, nil
}
func (*fakeStore) Backlog(context.Context, string) (int, time.Time, *core.Error) {
	return 0, time.Time{}, nil
}
func (*fakeStore) ClaimPending(context.Context, string, int) ([]*store.WorkItem, *core.Error) {
	return nil, nil
}
func (*fakeStore) RecoverOrphans(context.Context, string, time.Duration, int) ([]*store.WorkItem, *core.Error) {
	return nil, nil
}
func (*fakeStore) Insert(context.Context, []*store.WorkItem) *core.Error { return nil }
func (*fakeStore) InsertIfNotActive(context.Context, []*store.WorkItem) (int, *core.Error) {
	return 0, nil
}
func (*fakeStore) HasActive(context.Context, string, string) (bool, *core.Error) {
	return false, nil
}
func (*fakeStore) DeleteIfPending(context.Context, string) (bool, *core.Error) {
	return false, nil
}
func (*fakeStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*store.WorkItem, *core.Error) {
	return nil, nil
}

// SingleTask esegue in linea: un task nominato senza runner in questo processo (escluso dai modes,
// o type non registrato qui) deve fallire il Check, cioè l'avvio, e non ogni item a runtime.
func TestNewJobRegistration_CheckRunnerMancante(t *testing.T) {
	reg := newJobRegistration(&fakeStore{}, []*taskrunner.TaskRunner{taskrunner.New("import-in", &importRunner{})})
	if reg.Check == nil {
		t.Fatal("SingleTask deve dichiarare un Check")
	}
	if err := reg.Check("j", cfg(properties.Properties{scheduler.PropTask: "import-in"})); err != nil {
		t.Fatalf("runner presente: %v", err)
	}
	err := reg.Check("j", cfg(properties.Properties{scheduler.PropTask: "solo-worker"}))
	if err == nil || !strings.Contains(err.Error(), "solo-worker") || !strings.Contains(err.Error(), "modes") {
		t.Fatalf("atteso errore che nomini task e modes, ottenuto %v", err)
	}
}
