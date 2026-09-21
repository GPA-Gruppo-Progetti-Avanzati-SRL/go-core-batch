package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type fakeStore struct {
	claim       []*store.WorkItem
	orphans     []*store.WorkItem
	claimErr    *core.ApplicationError
	backlogN    int
	backlogOld  time.Time
	backlogHits int
}

func (f *fakeStore) ClaimPending(context.Context, string, string, string, int) ([]*store.WorkItem, *core.ApplicationError) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	out := f.claim
	f.claim = nil
	return out, nil
}
func (f *fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	out := f.orphans
	f.orphans = nil
	return out, nil
}
func (f *fakeStore) Backlog(context.Context, string, string, string) (int, time.Time, *core.ApplicationError) {
	f.backlogHits++
	return f.backlogN, f.backlogOld, nil
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
func (f *fakeStore) Purge(context.Context, string, time.Time, int) (int, *core.ApplicationError) {
	return 0, nil
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

func tick(process func(context.Context, string, []*store.WorkItem) error) ClaimingTick {
	return ClaimingTick{
		JobName: "j", JobType: "T", TaskName: "task",
		Limit: 10, RunTimeout: time.Minute, OrphanTimeout: time.Minute,
		Process: process,
	}
}

// Il feed gira PRIMA del claim: un item creato dal feed dev'essere claimabile nello stesso tick.
func TestClaimingTick_OrdineFeedPoiClaim(t *testing.T) {
	var ordine []string
	items := &fakeStore{claim: []*store.WorkItem{{Id: "a"}}}
	tk := tick(func(context.Context, string, []*store.WorkItem) error {
		ordine = append(ordine, "process")
		return nil
	})
	tk.Feed = func(context.Context, string) { ordine = append(ordine, "feed") }

	if err := tk.Run(items); err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.Join(ordine, ",") != "feed,process" {
		t.Fatalf("ordine delle fasi = %v, atteso feed prima del process", ordine)
	}
}

// Un tick a vuoto non deve chiamare Process: è la garanzia, in un posto solo, che le famiglie di
// job non debbano ripetere il controllo (e dimenticarlo in una).
func TestClaimingTick_TickAVuotoNonProcessa(t *testing.T) {
	chiamato := false
	err := tick(func(context.Context, string, []*store.WorkItem) error {
		chiamato = true
		return nil
	}).Run(&fakeStore{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if chiamato {
		t.Fatal("Process chiamata su un tick senza item")
	}
}

// ClaimPending fallita MA orfani già recuperati: si lavorano quelli. Sono già IN_PROGRESS, e
// lasciarli lì significherebbe aspettare un altro giro di orphan timeout.
func TestClaimingTick_ClaimFallitaMaOrfaniRecuperati(t *testing.T) {
	items := &fakeStore{
		orphans:  []*store.WorkItem{{Id: "orfano"}},
		claimErr: core.TechnicalError().WithMessage("db giù"),
	}
	var lavorati []string
	err := tick(func(_ context.Context, _ string, batch []*store.WorkItem) error {
		for _, it := range batch {
			lavorati = append(lavorati, it.Id)
		}
		return nil
	}).Run(items)
	if err != nil {
		t.Fatalf("il tick non deve fallire se ha degli orfani da lavorare: %v", err)
	}
	if len(lavorati) != 1 || lavorati[0] != "orfano" {
		t.Fatalf("item lavorati = %v, atteso l'orfano recuperato", lavorati)
	}
}

// ClaimPending fallita e NESSUN orfano: l'errore risale, il tick è fallito.
func TestClaimingTick_ClaimFallitaSenzaOrfani(t *testing.T) {
	items := &fakeStore{claimErr: core.TechnicalError().WithMessage("db giù")}
	err := tick(func(context.Context, string, []*store.WorkItem) error { return nil }).Run(items)
	if err == nil {
		t.Fatal("atteso errore: senza orfani un claim fallito è un tick fallito")
	}
}

// La coda si misura solo se il job la chiede: è una query in più per tick.
func TestClaimingTick_BacklogSoloSeAbilitato(t *testing.T) {
	items := &fakeStore{}
	if err := tick(func(context.Context, string, []*store.WorkItem) error { return nil }).Run(items); err != nil {
		t.Fatalf("run: %v", err)
	}
	if items.backlogHits != 0 {
		t.Fatalf("Backlog interrogato %d volte senza che il job lo chiedesse", items.backlogHits)
	}

	tk := tick(func(context.Context, string, []*store.WorkItem) error { return nil })
	tk.Backlog = true
	items2 := &fakeStore{backlogN: 3, backlogOld: time.Now().Add(-time.Hour)}
	if err := tk.Run(items2); err != nil {
		t.Fatalf("run: %v", err)
	}
	if items2.backlogHits != 1 {
		t.Fatalf("Backlog interrogato %d volte, attesa 1 per tick", items2.backlogHits)
	}
}

// Due tick nello stesso secondo devono avere jobId DIVERSI: prima l'id era il nome più un
// timestamp al secondo, e le righe di task_logs di esecuzioni diverse si confondevano.
func TestNewJobID_UnicoAncheNelloStessoSecondo(t *testing.T) {
	visti := make(map[string]bool, 100)
	for range 100 {
		id := NewJobID("job")
		if visti[id] {
			t.Fatalf("jobId duplicato: %s", id)
		}
		visti[id] = true
		if !strings.HasPrefix(id, "job-") {
			t.Fatalf("jobId %q non porta il nome del job", id)
		}
	}
}
