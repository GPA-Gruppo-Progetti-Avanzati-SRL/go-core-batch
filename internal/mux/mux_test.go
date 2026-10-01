package mux

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
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

func runWith(t *testing.T, tr *taskrunner.TaskRunner, item *store.WorkItem) *recordingStore {
	t.Helper()
	items := &recordingStore{}
	_ = New([]*taskrunner.TaskRunner{tr}).Run(context.Background(), item, items)
	return items
}

// Il tetto configurato in `tasks[].max-retry` deve arrivare fino a store.ApplyResult. È il giro che
// NON funzionava: taskreg.Instances ricostruiva la Config senza MaxRetry, quindi ogni task ritentava
// all'infinito qualunque cosa dicesse lo YAML, e nessun test copriva il percorso.
func TestMaxRetry_ArrivaFinoAdApplyResult(t *testing.T) {
	tr := taskrunner.New("import-in", retryRunner{}).WithMaxRetry(3)

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
	tr := taskrunner.New("import-in", retryRunner{})
	if got := tr.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
	if items := runWith(t, tr, itemAtRetry(99)); !items.pending || items.failed {
		t.Fatalf("senza tetto si ritenta sempre, ottenuto pending=%v failed=%v", items.pending, items.failed)
	}

	var nilRunner *taskrunner.TaskRunner
	if got := nilRunner.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("receiver nil: atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
}

// neverRunner fallisce il test se viene eseguito.
type neverRunner struct{ t *testing.T }

func (n neverRunner) Run(context.Context, *store.WorkItem) error {
	n.t.Error("il runner non doveva essere eseguito: l'item aveva già esaurito i ritentativi")
	return nil
}

// Un orfano recuperato oltre il tetto (il runner di prima è morto senza ritornare, e RecoverOrphans
// ha incrementato il contatore) va in FAILED senza rieseguire il runner: prima il tetto si
// applicava solo al ritorno del runner, e un runner che fa morire il processo girava per sempre.
func TestMaxRetry_OrfanoOltreIlTettoNonRieseguito(t *testing.T) {
	items := runWith(t, taskrunner.New("import-in", neverRunner{t}).WithMaxRetry(3), itemAtRetry(4))
	if !items.failed || items.pending || items.done {
		t.Fatalf("atteso MarkFailed senza esecuzione, ottenuto done=%v pending=%v failed=%v", items.done, items.pending, items.failed)
	}
}
