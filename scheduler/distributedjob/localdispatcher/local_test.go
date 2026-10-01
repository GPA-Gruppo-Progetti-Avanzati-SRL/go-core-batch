package localdispatcher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/runner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx/fxtest"
)

// blockingRunner blocca in Run finché release non viene chiuso, per tenere occupato lo slot.
type blockingRunner struct {
	started chan struct{}
	release chan struct{}
}

func (r *blockingRunner) Run(ctx context.Context, _ *store.WorkItem) error {
	select {
	case r.started <- struct{}{}:
	default:
	}
	select {
	case <-r.release:
	case <-ctx.Done():
	}
	return nil
}

// fakeStore: GetById ritorna un item fisso, i Mark* sono no-op. Resto non usato.
type fakeStore struct{}

func (fakeStore) GetById(context.Context, string) (*store.WorkItem, *core.Error) {
	return &store.WorkItem{Id: "obj", LockToken: "tok"}, nil
}
func (fakeStore) MarkDone(context.Context, []string, string) *core.Error { return nil }
func (fakeStore) MarkFailed(context.Context, string, string, string) *core.Error {
	return nil
}
func (fakeStore) MarkPending(context.Context, string, string, time.Duration) *core.Error {
	return nil
}
func (fakeStore) Release(context.Context, string, string) *core.Error { return nil }
func (fakeStore) Purge(context.Context, string, time.Time, int) (int, *core.Error) {
	return 0, nil
}
func (fakeStore) Backlog(context.Context, string) (int, time.Time, *core.Error) {
	return 0, time.Time{}, nil
}
func (fakeStore) ClaimPending(context.Context, string, int) ([]*store.WorkItem, *core.Error) {
	return nil, nil
}
func (fakeStore) RecoverOrphans(context.Context, string, time.Duration, int) ([]*store.WorkItem, *core.Error) {
	return nil, nil
}
func (fakeStore) Insert(context.Context, []*store.WorkItem) *core.Error { return nil }
func (fakeStore) InsertIfNotActive(context.Context, []*store.WorkItem) (int, *core.Error) {
	return 0, nil
}
func (fakeStore) HasActive(context.Context, string, string) (bool, *core.Error) {
	return false, nil
}
func (fakeStore) DeleteIfPending(context.Context, string) (bool, *core.Error) {
	return false, nil
}
func (fakeStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*store.WorkItem, *core.Error) {
	return nil, nil
}

type fakeData struct{ done atomic.Int32 }

func (d *fakeData) SetTaskStart(context.Context, string, string, string, string)           {}
func (d *fakeData) SetTaskDone(context.Context, string, string, string, string)            { d.done.Add(1) }
func (d *fakeData) SetTaskInError(context.Context, string, string, string, string, string) {}
func (d *fakeData) SetTaskAssigned(context.Context, string, string, string, string)        {}
func (d *fakeData) SetTaskAssignationKO(context.Context, string, string, string, string, string) {
}
func (d *fakeData) InsertTaskLogs(context.Context, []*store.TaskLog) {}
func (d *fakeData) PurgeTaskLogs(context.Context, time.Time, int) (int, *core.Error) {
	return 0, nil
}

func TestLocalDispatcher(t *testing.T) {
	br := &blockingRunner{started: make(chan struct{}, 8), release: make(chan struct{})}
	mux := runner.NewMux([]*runner.TaskRunner{runner.New("T", br)})
	data := &fakeData{}
	lc := fxtest.NewLifecycle(t)

	d := New(lc, nil, mux, fakeStore{}, data)
	d.sem = make(chan struct{}, 2) // cap ridotto per un test deterministico
	lc.RequireStart()              // necessario perché RequireStop esegua l'hook OnStop (drain)

	// Riempie i 2 slot con task bloccate.
	req := func(id string) distributedjob.DispatchRequest {
		return distributedjob.DispatchRequest{
			JobId: "j", TaskId: id, TaskName: "T",
			Item: &store.WorkItem{Id: "obj", TaskName: "T"}, Timeout: time.Minute,
		}
	}
	if err := d.DispatchTask(context.Background(), req("t1")); err != nil {
		t.Fatalf("dispatch 1: %v", err)
	}
	if err := d.DispatchTask(context.Background(), req("t2")); err != nil {
		t.Fatalf("dispatch 2: %v", err)
	}
	// Terzo dispatch: cap raggiunto → errore, nessuna goroutine in più.
	if err := d.DispatchTask(context.Background(), req("t3")); err == nil {
		t.Fatal("dispatch 3: atteso errore per cap di concorrenza, ottenuto nil")
	}

	// Sblocca le task e verifica il drain via OnStop entro il deadline.
	close(br.release)
	lc.RequireStop() // esegue gli OnStop hook (drain)

	if got := data.done.Load(); got != 2 {
		t.Fatalf("task completate = %d, want 2", got)
	}

	// Dopo lo stop, nuovi dispatch sono rifiutati.
	if err := d.DispatchTask(context.Background(), req("t4")); err == nil {
		t.Fatal("dispatch dopo stop: atteso errore, ottenuto nil")
	}
}

// countingRunner conta le esecuzioni iniziate.
type countingRunner struct{ runs atomic.Int64 }

func (r *countingRunner) Run(context.Context, *store.WorkItem) error {
	r.runs.Add(1)
	return nil
}

// Dispatch concorrenti con l'arresto: nessuna task può partire DOPO che il drain è finito. Prima il
// controllo di stopping e il wg.Go erano due passi separati, e un dispatch entrato fra le due righe
// di OnStop aggiungeva al WaitGroup una task che il drain non aspettava.
func TestLocalDispatcher_NessunaTaskDopoLArresto(t *testing.T) {
	for range 50 {
		cr := &countingRunner{}
		mux := runner.NewMux([]*runner.TaskRunner{runner.New("T", cr)})
		lc := fxtest.NewLifecycle(t)
		d := New(lc, nil, mux, fakeStore{}, &fakeData{})
		lc.RequireStart()

		stop := make(chan struct{})
		var accepted atomic.Int64
		done := make(chan struct{})
		for range 4 {
			go func() {
				defer func() { done <- struct{}{} }()
				for {
					select {
					case <-stop:
						return
					default:
					}
					req := distributedjob.DispatchRequest{JobId: "j", TaskId: "t", TaskName: "T",
						Item: &store.WorkItem{Id: "obj", TaskName: "T"}, Timeout: time.Minute}
					if d.DispatchTask(context.Background(), req) == nil {
						accepted.Add(1)
					}
				}
			}()
		}
		lc.RequireStop()
		afterDrain := cr.runs.Load()
		close(stop)
		for range 4 {
			<-done
		}
		// Ogni dispatch accettato è stato eseguito ENTRO il drain: dopo, nessuno ne parte.
		if got := cr.runs.Load(); got != afterDrain || got != accepted.Load() {
			t.Fatalf("esecuzioni: %d al termine del drain, %d alla fine, %d dispatch accettati", afterDrain, got, accepted.Load())
		}
	}
}
