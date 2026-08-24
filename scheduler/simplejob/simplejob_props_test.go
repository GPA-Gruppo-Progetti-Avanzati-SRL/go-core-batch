package simplejob

import (
	"context"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
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
// processo): copre insieme istanze per voce di `tasks:`, properties per istanza, dipendenze condivise
// e task non referenziato.
func TestRegisterRunner_OneInstancePerTaskWithItsProps(t *testing.T) {
	svc := &fakeSvc{name: "svc"}
	core.Supply(svc)
	core.ProvideAs[store.IWorkItemStore](func() *fakeStore { return &fakeStore{} })

	task.Apply(func() {
		RegisterRunner[importRunner]("Import")
		RegisterRunner[notifyRunner]("Notify") // nessun job/worker lo referenzia
	}, task.ActiveSet{
		Tasks: []task.Config{
			{Name: "import-in", Type: "Import", Properties: core.Properties{"folder": "/data/in"}},
			{Name: "import-bulk", Type: "Import", Properties: core.Properties{"folder": "/data/bulk", "limit": 500}},
			{Name: "notify-mail", Type: "Notify"},
		},
		Referenced: []string{"import-in", "import-bulk"},
	})
	Module()

	var runners []*SimpleTaskRunner
	var jobs []scheduler.JobRegistration
	core.Invoke(func(p struct {
		fx.In
		Runners []*SimpleTaskRunner         `group:"batch_simple_runners"`
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
		if r.TaskType != "Import" {
			t.Fatalf("task type errato: %q", r.TaskType)
		}
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
	// Una sola JobRegistration per job type, anche con più istanze.
	if len(jobs) != 1 || jobs[0].Type != "Import" {
		t.Fatalf("attesa una JobRegistration per il solo job type Import, ottenuto %+v", jobs)
	}
}

// Una JobRegistration per task type (che per simplejob è il type del job), con la mappa delle
// istanze passata alla factory.
func TestNewJobRegistrations_GroupsByJobType(t *testing.T) {
	regs := newJobRegistrations(&fakeStore{}, []*SimpleTaskRunner{
		NewNamed("Import", "import-in", &importRunner{}),
		NewNamed("Import", "import-bulk", &importRunner{}),
		NewNamed("Hello", "Hello", &importRunner{}),
	})
	if len(regs) != 2 {
		t.Fatalf("attese 2 JobRegistration (Import, Hello), ottenuto %d", len(regs))
	}
	if regs[0].Type != "Import" || regs[1].Type != "Hello" {
		t.Fatalf("ordine/type errati: %+v", regs)
	}
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
