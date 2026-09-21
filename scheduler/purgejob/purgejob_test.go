package purgejob

import (
	"context"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type purgeCall struct {
	status    string
	olderThan time.Time
	limit     int
}

type fakeStore struct {
	calls []purgeCall
	n     int
}

func (f *fakeStore) Purge(_ context.Context, status string, olderThan time.Time, limit int) (int, *core.ApplicationError) {
	f.calls = append(f.calls, purgeCall{status, olderThan, limit})
	return f.n, nil
}

func (f *fakeStore) ClaimPending(context.Context, string, string, string, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) Release(context.Context, string, string) *core.ApplicationError { return nil }
func (f *fakeStore) MarkDone(context.Context, []string, string) *core.ApplicationError {
	return nil
}
func (f *fakeStore) MarkFailed(context.Context, string, string, string) *core.ApplicationError {
	return nil
}
func (f *fakeStore) MarkPending(context.Context, string, string, time.Duration) *core.ApplicationError {
	return nil
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

type fakeData struct{ purged int }

func (d *fakeData) SetTaskStart(context.Context, string, string, string, string)                 {}
func (d *fakeData) SetTaskDone(context.Context, string, string, string, string)                  {}
func (d *fakeData) SetTaskInError(context.Context, string, string, string, string, string)       {}
func (d *fakeData) SetTaskAssigned(context.Context, string, string, string, string)              {}
func (d *fakeData) SetTaskAssignationKO(context.Context, string, string, string, string, string) {}
func (d *fakeData) InsertTaskLogs(context.Context, []*store.TaskLog)                             {}
func (d *fakeData) PurgeTaskLogs(_ context.Context, _ time.Time, _ int) (int, *core.ApplicationError) {
	d.purged++
	return 0, nil
}

func conf(props core.Properties) scheduler.Config {
	return scheduler.Config{Name: "retention", Type: JobType, LockTimeout: time.Minute, Properties: props}
}

// La retention non ha default: senza `status` o senza `older-than` non si sa cosa cancellare né
// da quando, e indovinare significherebbe cancellare dati che nessuno ha chiesto di cancellare.
func TestRisolvi_ConfigInvalida(t *testing.T) {
	cases := map[string]core.Properties{
		"senza status":     {"older-than": "24h"},
		"status vuoto":     {"status": "", "older-than": "24h"},
		"senza older-than": {"status": "DONE"},
		"older-than zero":  {"status": "DONE", "older-than": "0s"},
		"limit non valido": {"status": "DONE", "older-than": "24h", "limit": 0},
	}
	for nome, props := range cases {
		t.Run(nome, func(t *testing.T) {
			if _, err := risolvi("retention", conf(props)); err == nil {
				t.Fatal("atteso errore di configurazione, ottenuto nil")
			}
		})
	}
}

func TestRisolvi_Default(t *testing.T) {
	p, err := risolvi("retention", conf(core.Properties{"status": "DONE", "older-than": "168h"}))
	if err != nil {
		t.Fatalf("risolvi: %v", err)
	}
	if p.status != "DONE" || p.olderThan != 168*time.Hour {
		t.Fatalf("parametri = %#v", p)
	}
	if p.limit != defaultLimit {
		t.Fatalf("limit = %d, atteso il default %d", p.limit, defaultLimit)
	}
	if p.taskLogs {
		t.Fatal("task-logs deve essere spento di default: cancellare i log è una scelta a parte")
	}
}

// Il job cancella nello stato richiesto, con la finestra richiesta, e tocca i task_logs solo se
// glielo si chiede.
func TestRun_CancellaSoloQuantoConfigurato(t *testing.T) {
	items := &fakeStore{n: 7}
	data := &fakeData{}
	p, err := risolvi("retention", conf(core.Properties{
		"status": "DONE", "older-than": "24h", "limit": 100,
	}))
	if err != nil {
		t.Fatalf("risolvi: %v", err)
	}

	prima := time.Now().Add(-24 * time.Hour)
	if err := run("retention", time.Minute, p, items, data); err != nil {
		t.Fatalf("run: %v", err)
	}
	dopo := time.Now().Add(-24 * time.Hour)

	if len(items.calls) != 1 {
		t.Fatalf("chiamate a Purge = %d, attesa 1", len(items.calls))
	}
	c := items.calls[0]
	if c.status != "DONE" || c.limit != 100 {
		t.Fatalf("Purge chiamata con %#v", c)
	}
	if c.olderThan.Before(prima) || c.olderThan.After(dopo) {
		t.Fatalf("cutoff = %s, atteso ~ now-24h", c.olderThan)
	}
	if data.purged != 0 {
		t.Fatal("task_logs cancellati senza che la property task-logs lo chiedesse")
	}

	// Con task-logs: true anche le righe di log vengono cancellate.
	p.taskLogs = true
	if err := run("retention", time.Minute, p, items, data); err != nil {
		t.Fatalf("run con task-logs: %v", err)
	}
	if data.purged != 1 {
		t.Fatalf("PurgeTaskLogs chiamata %d volte, attesa 1", data.purged)
	}
}
