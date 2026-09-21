package distributedjob

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// ---- fake store: registra le operazioni di lifecycle invocate -------------------------------

type opLifecycle struct {
	op    string
	id    string
	token string
}

type fakeStore struct {
	claim []*store.WorkItem
	ops   []opLifecycle
}

func (f *fakeStore) ClaimPending(_ context.Context, _, _, _ string, _ int) ([]*store.WorkItem, *core.ApplicationError) {
	out := f.claim
	f.claim = nil
	return out, nil
}
func (f *fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) Release(_ context.Context, id, token string) *core.ApplicationError {
	f.ops = append(f.ops, opLifecycle{"release", id, token})
	return nil
}
func (f *fakeStore) MarkPending(_ context.Context, id, token string, _ time.Duration) *core.ApplicationError {
	f.ops = append(f.ops, opLifecycle{"pending", id, token})
	return nil
}
func (f *fakeStore) MarkDone(context.Context, []string, string) *core.ApplicationError { return nil }
func (f *fakeStore) MarkFailed(context.Context, string, string, string) *core.ApplicationError {
	return nil
}
func (f *fakeStore) Purge(context.Context, string, time.Time, int) (int, *core.ApplicationError) {
	return 0, nil
}
func (f *fakeStore) Backlog(context.Context, string, string, string) (int, time.Time, *core.ApplicationError) {
	return 0, time.Time{}, nil
}
func (f *fakeStore) Insert(context.Context, []*store.WorkItem) *core.ApplicationError { return nil }
func (f *fakeStore) InsertIfNotActive(context.Context, []*store.WorkItem) (int, *core.ApplicationError) {
	return 0, nil
}
func (f *fakeStore) HasActive(context.Context, string, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (f *fakeStore) GetById(context.Context, string) (*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) DeleteIfPending(context.Context, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (f *fakeStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}

// ---- fake data: conta le righe di task log scritte ------------------------------------------

type fakeData struct {
	batches int
	righe   []*store.TaskLog
	singole int
}

func (d *fakeData) SetTaskStart(context.Context, string, string, string, string)    { d.singole++ }
func (d *fakeData) SetTaskDone(context.Context, string, string, string, string)     { d.singole++ }
func (d *fakeData) SetTaskAssigned(context.Context, string, string, string, string) { d.singole++ }
func (d *fakeData) SetTaskInError(context.Context, string, string, string, string, string) {
	d.singole++
}
func (d *fakeData) SetTaskAssignationKO(context.Context, string, string, string, string, string) {
	d.singole++
}
func (d *fakeData) InsertTaskLogs(_ context.Context, logs []*store.TaskLog) {
	d.batches++
	d.righe = append(d.righe, logs...)
}
func (d *fakeData) PurgeTaskLogs(context.Context, time.Time, int) (int, *core.ApplicationError) {
	return 0, nil
}

// ---- fake dispatcher ------------------------------------------------------------------------

type fakeDispatcher struct {
	err  error
	reqs []DispatchRequest
}

func (f *fakeDispatcher) DispatchTask(_ context.Context, req DispatchRequest) error {
	f.reqs = append(f.reqs, req)
	return f.err
}

func conf(props core.Properties) scheduler.Config {
	return scheduler.Config{Name: "j", Type: JobType, LockTimeout: time.Minute, Properties: props}
}

// Un dispatch fallito deve RILASCIARE l'item, non lasciarlo IN_PROGRESS: restandoci, tornerebbe
// lavorabile solo dopo l'orphan timeout, e il recupero gli consumerebbe un ritentativo che non ha
// mai usato — con max-retry configurato, una saturazione temporanea del pool esaurirebbe il
// budget di item mai eseguiti. Release è MarkPending SENZA l'incremento di retry: il test
// pretende proprio la release e non un pending qualunque.
func TestDispatchFallito_RilasciaSenzaConsumareRitentativo(t *testing.T) {
	item := &store.WorkItem{Id: "wi-1", TaskName: "T", LockToken: "tok-1", Status: store.StatusInProgress}
	items := &fakeStore{claim: []*store.WorkItem{item}}
	data := &fakeData{}
	disp := &fakeDispatcher{err: errors.New("pool saturo")}

	factory := makeClaimingFactory(disp, items, nil, data)
	_ = factory("j", &scheduler.Services{}, conf(core.Properties{"task": "T", "limit": 10}))

	if err := jobTick(t, disp, items, data); err != nil {
		t.Fatalf("il tick non deve fallire per un dispatch rifiutato: %v", err)
	}

	if len(items.ops) != 1 {
		t.Fatalf("operazioni di lifecycle = %#v, attesa una sola release", items.ops)
	}
	if got := items.ops[0]; got.op != "release" || got.id != "wi-1" || got.token != "tok-1" {
		t.Fatalf("lifecycle = %#v, atteso release di wi-1 fenced dal token tok-1", got)
	}
}

// Le righe di task_logs della fase di dispatch vanno scritte in UNA volta: erano una insert
// sincrona per item, dentro il tick e quindi dentro il lock del job.
func TestDispatch_TaskLogInUnaSolaScrittura(t *testing.T) {
	var claim []*store.WorkItem
	for i := range 10 {
		claim = append(claim, &store.WorkItem{
			Id: string(rune('a' + i)), TaskName: "T", LockToken: "tok", Status: store.StatusInProgress,
		})
	}
	items := &fakeStore{claim: claim}
	data := &fakeData{}
	disp := &fakeDispatcher{}

	if err := jobTick(t, disp, items, data); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if data.batches != 1 {
		t.Fatalf("scritture di task log = %d, attesa 1 per l'intero tick", data.batches)
	}
	if len(data.righe) != 10 {
		t.Fatalf("righe di task log = %d, attese 10 (una per item)", len(data.righe))
	}
	if data.singole != 0 {
		t.Fatalf("scritture singole = %d, attese 0: la fase di dispatch non deve più farne", data.singole)
	}
}

// Il dispatcher deve ricevere l'item INTERO e il deadline del job: senza il primo il percorso
// in-process rileggerebbe dal database un item che ha già in mano, senza il secondo la task
// girerebbe oltre l'orphan timeout, cioè accanto al proprio ri-claim.
func TestDispatch_PortaItemInteroEDeadline(t *testing.T) {
	item := &store.WorkItem{Id: "wi-1", TaskName: "T", LockToken: "tok-1", Payload: "carico"}
	items := &fakeStore{claim: []*store.WorkItem{item}}
	disp := &fakeDispatcher{}

	if err := jobTick(t, disp, items, &fakeData{}); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if len(disp.reqs) != 1 {
		t.Fatalf("dispatch = %d, atteso 1", len(disp.reqs))
	}
	req := disp.reqs[0]
	if req.Item != item {
		t.Fatalf("il dispatcher ha ricevuto %#v, atteso l'item claimato", req.Item)
	}
	if req.Timeout != time.Minute {
		t.Fatalf("Timeout = %s, atteso l'orphan timeout del job (lock-timeout = 1m)", req.Timeout)
	}
}

// Una config senza le property obbligatorie deve fallire alla COSTRUZIONE del job — quando c'è
// ancora qualcuno che guarda i log di avvio — e poi a ogni tick, invece di presentarsi come un
// errore di runtime al primo tick.
func TestRisolvi_ConfigInvalida(t *testing.T) {
	cases := map[string]core.Properties{
		"senza task":     {"limit": 10},
		"task vuoto":     {"task": "", "limit": 10},
		"senza limit":    {"task": "T"},
		"limit zero":     {"task": "T", "limit": 0},
		"limit negativo": {"task": "T", "limit": -1},
	}
	for nome, props := range cases {
		t.Run(nome, func(t *testing.T) {
			if _, _, err := risolvi("j", conf(props)); err == nil {
				t.Fatal("atteso errore di configurazione, ottenuto nil")
			}
		})
	}
	if _, limit, err := risolvi("j", conf(core.Properties{"task": "T", "limit": 42})); err != nil || limit != 42 {
		t.Fatalf("config valida: limit=%d err=%v", limit, err)
	}
}

// jobTick esegue un tick completo con la configurazione standard del test.
func jobTick(t *testing.T, disp ITaskDispatcher, items store.IWorkItemStore, data store.IData) error {
	t.Helper()
	taskName, limit, err := risolvi("j", conf(core.Properties{"task": "T", "limit": 10}))
	if err != nil {
		t.Fatalf("risolvi: %v", err)
	}
	return scheduler.ClaimingTick{
		JobName: "j", JobType: JobType, TaskName: taskName, Limit: limit,
		RunTimeout: time.Minute, OrphanTimeout: time.Minute,
		Process: func(ctx context.Context, jobID string, batch []*store.WorkItem) error {
			dispatchBatch(ctx, jobID, taskName, time.Minute, batch, disp, items, data)
			return nil
		},
	}.Run(items)
}
