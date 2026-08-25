package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// markCall registra l'ultima transizione di lifecycle applicata dallo store fake.
type markCall struct {
	op    string // "done" | "failed" | "pending" | "" (nessuna)
	id    string
	token string
	after time.Duration
}

type fakeStore struct {
	last markCall
	item *store.WorkItem
}

func (f *fakeStore) MarkDone(_ context.Context, ids []string, token string) *core.ApplicationError {
	f.last = markCall{op: "done", id: ids[0], token: token}
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, id, token, _ string) *core.ApplicationError {
	f.last = markCall{op: "failed", id: id, token: token}
	return nil
}
func (f *fakeStore) MarkPending(_ context.Context, id, token string, after time.Duration) *core.ApplicationError {
	f.last = markCall{op: "pending", id: id, token: token, after: after}
	return nil
}
func (f *fakeStore) GetById(_ context.Context, _ string) (*store.WorkItem, *core.ApplicationError) {
	return f.item, nil
}

// resto dell'interfaccia: no-op non usati da worker.Run.
func (f *fakeStore) FindPending(context.Context, string, string, string) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) ClaimPending(context.Context, string, string, string, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) Insert(context.Context, []*store.WorkItem) *core.ApplicationError { return nil }
func (f *fakeStore) InsertIfNotActive(context.Context, []*store.WorkItem) (int, *core.ApplicationError) {
	return 0, nil
}
func (f *fakeStore) HasActive(context.Context, string, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (f *fakeStore) DeleteIfPending(context.Context, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (f *fakeStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}

// fakeData implementa store.IData come no-op (solo per LogStart/LogDone/LogTaskError).
type fakeData struct{}

func (fakeData) SetTaskStart(context.Context, string, string, string, string)                 {}
func (fakeData) SetTaskDone(context.Context, string, string, string, string)                  {}
func (fakeData) SetTaskInError(context.Context, string, string, string, string, string)       {}
func (fakeData) SetTaskAssigned(context.Context, string, string, string, string)              {}
func (fakeData) SetTaskAssignationKO(context.Context, string, string, string, string, string) {}

// fakeService implementa ITaskService[*fakeService] con un esito RunTask configurabile.
type fakeService struct {
	known  bool
	result error
}

func (s *fakeService) GetServices() *fakeService { return s }
func (s *fakeService) GetTaskExecutions(string) (RunTask[*fakeService], bool) {
	if !s.known {
		return nil, false
	}
	return func(*Task, *fakeService, store.IWorkItemStore) error { return s.result }, true
}

// TestRunLifecycle verifica che worker.Run sia l'unico punto di finalizzazione e applichi
// la convenzione store.ApplyResult per ogni esito del RunTask.
func TestRunLifecycle(t *testing.T) {
	cases := []struct {
		name   string
		known  bool
		result error
		wantOp string
	}{
		{"nil → done", true, nil, "done"},
		{"ErrHandled → no-op", true, store.ErrHandled, ""},
		{"RetryError → pending", true, store.Retry(5 * time.Minute), "pending"},
		{"errore generico → failed", true, errors.New("boom"), "failed"},
		{"tipo sconosciuto → failed", false, nil, "failed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := &fakeStore{item: &store.WorkItem{Id: "obj-1", LockToken: "tok-1"}}
			svc := &fakeService{known: c.known, result: c.result}
			ctx, cancel := context.WithCancel(context.Background())
			// Nel path known-type è il RunTask (bridge) a popolare t.LockToken; qui lo simuliamo.
			// Nel path type-not-found worker.Run lo recupera da GetById (fs.item.LockToken).
			task := &Task{Id: "t1", JobId: "j1", TaskName: "MY_TASK", ObjectId: "obj-1", LockToken: "tok-1", Context: ctx, Cancel: cancel}
			sem := make(chan struct{}, 1)
			sem <- struct{}{}

			Run[*fakeService](sem, task, svc, fakeData{}, fs)

			if fs.last.op != c.wantOp {
				t.Fatalf("lifecycle op = %q, want %q", fs.last.op, c.wantOp)
			}
			if c.wantOp != "" {
				if fs.last.id != "obj-1" {
					t.Fatalf("lifecycle id = %q, want obj-1", fs.last.id)
				}
				if fs.last.token != "tok-1" {
					t.Fatalf("fencing token = %q, want tok-1", fs.last.token)
				}
			}
			if c.name == "RetryError → pending" && fs.last.after != 5*time.Minute {
				t.Fatalf("retry after = %v, want 5m", fs.last.after)
			}
		})
	}
}
