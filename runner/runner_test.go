package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// recordingStore registra quale Mark* è stato invocato. L'interfaccia è EMBEDDATA e nil: ogni metodo
// non sovrascritto panica se qualcuno lo chiama, che è esattamente ciò che vogliamo sapere.
type recordingStore struct {
	store.IWorkItemStore
	done    bool
	failed  bool
	pending bool
	reason  string
	after   time.Duration
}

func (s *recordingStore) MarkDone(context.Context, []string, string) *core.Error {
	s.done = true
	return nil
}

func (s *recordingStore) MarkFailed(_ context.Context, _, _, reason string) *core.Error {
	s.failed, s.reason = true, reason
	return nil
}

func (s *recordingStore) MarkPending(_ context.Context, _, _ string, after time.Duration) *core.Error {
	s.pending, s.after = true, after
	return nil
}

// retryRunner chiede sempre un ritentativo: è il solo esito su cui il tetto ha effetto.
type retryRunner struct{}

func (retryRunner) Run(context.Context, *store.WorkItem) error { return store.Retry(time.Minute) }

func itemAtRetry(n int) *store.WorkItem {
	return &store.WorkItem{Id: "id-1", LockToken: "tok", TaskName: "import-in", Retry: n}
}

func runWith(t *testing.T, tr *TaskRunner, item *store.WorkItem) *recordingStore {
	t.Helper()
	items := &recordingStore{}
	_ = NewMux([]*TaskRunner{tr}).Run(context.Background(), item, items)
	return items
}

// Il tetto configurato in `tasks[].max-retry` deve arrivare fino a store.ApplyResult. È il giro che
// NON funzionava: task.Instances ricostruiva la Config senza MaxRetry, quindi ogni task ritentava
// all'infinito qualunque cosa dicesse lo YAML, e nessun test copriva il percorso.
func TestMaxRetry_ArrivaFinoAdApplyResult(t *testing.T) {
	tr := New("import-in", retryRunner{}).WithMaxRetry(3)

	// Tentativi già consumati < tetto: si ritenta.
	if items := runWith(t, tr, itemAtRetry(2)); !items.pending || items.failed {
		t.Fatalf("atteso MarkPending sotto il tetto, ottenuto pending=%v failed=%v", items.pending, items.failed)
	}

	// Raggiunto il tetto: l'item è esaurito, non si ritenta più.
	items := runWith(t, tr, itemAtRetry(3))
	if !items.failed || items.pending {
		t.Fatalf("atteso MarkFailed al tetto, ottenuto pending=%v failed=%v", items.pending, items.failed)
	}
	if !strings.Contains(items.reason, "3") {
		t.Fatalf("il motivo deve nominare il tetto raggiunto, ottenuto %q", items.reason)
	}
}

// L'assenza del campo vale illimitato, che è la condotta storica: lo zero value di un int è 0, cioè
// "nessun ritentativo", e come default silenzioso sarebbe il peggiore possibile.
func TestMaxRetry_AssenzaValeIllimitato(t *testing.T) {
	tr := New("import-in", retryRunner{})
	if got := tr.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
	if items := runWith(t, tr, itemAtRetry(99)); !items.pending || items.failed {
		t.Fatalf("senza tetto si ritenta sempre, ottenuto pending=%v failed=%v", items.pending, items.failed)
	}

	var nilRunner *TaskRunner
	if got := nilRunner.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("receiver nil: atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
}

// Stessa semantica sul contratto dei file runner: senza il campo, `max-retry` valeva per Register e
// non per RegisterFile — un knob che vale a metà è peggio di un knob che non vale.
func TestFileTaskRunner_PortaIlTetto(t *testing.T) {
	fr := NewFile("s3-in", nil)
	if got := fr.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("assenza: atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
	if got := fr.WithMaxRetry(5).ResolveMaxRetry(); got != 5 {
		t.Fatalf("atteso 5, ottenuto %d", got)
	}
	var nilFile *FileTaskRunner
	if got := nilFile.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("receiver nil: atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
}

func withMode(t *testing.T, mode string) {
	t.Helper()
	prev := core.Mode
	core.Mode = mode
	t.Cleanup(func() { core.Mode = prev })
}

func declared() task.ActiveSet {
	return task.ActiveSet{
		Tasks:      []task.Config{{Name: "import-in", Type: "IMPORT"}},
		Referenced: []string{"import-in"},
	}
}

// Il gate per-task: senza modes il comportamento è quello storico (attivo ovunque); con modes solo
// nei mode indicati.
func TestActiveInstances_GatePerMode(t *testing.T) {
	withMode(t, "WORKER")

	var senzaModes, modeGiusto, modeSbagliato []task.Config
	task.Apply(func() {
		senzaModes = activeInstances("IMPORT", nil)
		modeGiusto = activeInstances("IMPORT", []string{"WORKER"})
		modeSbagliato = activeInstances("IMPORT", []string{"SCHEDULER"})
	}, declared())

	if len(senzaModes) != 1 {
		t.Fatalf("senza modes il task è attivo in ogni mode, ottenuto %d istanze", len(senzaModes))
	}
	if len(modeGiusto) != 1 {
		t.Fatalf("mode corrispondente: attesa 1 istanza, ottenuto %d", len(modeGiusto))
	}
	if len(modeSbagliato) != 0 {
		t.Fatalf("mode non corrispondente: attese 0 istanze, ottenuto %d", len(modeSbagliato))
	}
}

// L'ORDINE è l'invariante: task.Instances va chiamata SEMPRE, anche quando il mode esclude il task,
// perché è lei ad alimentare la contabilità di task.check. Un type non dichiarato in `tasks:` deve
// quindi far fallire l'avvio anche se il mode lo esclude — altrimenti il gate mascherebbe un errore
// di configurazione.
func TestActiveInstances_InstancesChiamataAncheQuandoIlModeEsclude(t *testing.T) {
	withMode(t, "WORKER")

	var msg string
	func() {
		defer func() {
			if r := recover(); r != nil {
				if s, ok := r.(string); ok {
					msg = s
				}
			}
		}()
		task.Apply(func() { activeInstances("MAI_DICHIARATO", []string{"SCHEDULER"}) }, declared())
	}()
	if !strings.Contains(msg, "non dichiarati") {
		t.Fatalf("atteso il fail-fast sui type non dichiarati, ottenuto %q", msg)
	}
}

// Il caso opposto: un type DICHIARATO ed escluso dal mode non è un errore di configurazione, è solo
// un runner che questo processo non istanzia.
func TestActiveInstances_TypeDichiaratoEsclusoDalModeNonEUnErrore(t *testing.T) {
	withMode(t, "WORKER")
	task.Apply(func() {
		if got := activeInstances("IMPORT", []string{"SCHEDULER"}); len(got) != 0 {
			t.Fatalf("attese 0 istanze, ottenuto %d", len(got))
		}
	}, declared())
}
