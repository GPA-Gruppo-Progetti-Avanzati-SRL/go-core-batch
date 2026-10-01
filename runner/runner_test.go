package runner

import (
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskreg"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

func withMode(t *testing.T, mode string) {
	t.Helper()
	prev := core.Mode
	core.Mode = mode
	t.Cleanup(func() { core.Mode = prev })
}

func declared() taskreg.ActiveSet {
	return taskreg.ActiveSet{
		Tasks:      []task.Config{{Name: "import-in", Type: "IMPORT"}},
		Referenced: []string{"import-in"},
		Executed:   []string{"import-in"},
	}
}

// Il gate per-task è un filtro in AND sulla config: senza modes, o col MODE fra quelli indicati,
// il task eseguito qui viene istanziato.
func TestActiveInstances_ModesAmmettono(t *testing.T) {
	withMode(t, "WORKER")

	var senzaModes, modeGiusto []task.Config
	taskreg.Apply(func() {
		senzaModes = activeInstances("IMPORT", nil)
		modeGiusto = activeInstances("IMPORT", []string{"WORKER"})
	}, declared())

	if len(senzaModes) != 1 || len(modeGiusto) != 1 {
		t.Fatalf("attesa 1 istanza in entrambi i casi, ottenuto %d e %d", len(senzaModes), len(modeGiusto))
	}
}

// I modes non accendono ciò che la config non esegue qui.
func TestActiveInstances_ModesNonAccendono(t *testing.T) {
	withMode(t, "WORKER")
	nonEseguito := declared()
	nonEseguito.Executed = nil
	taskreg.Apply(func() {
		if got := activeInstances("IMPORT", []string{"WORKER"}); len(got) != 0 {
			t.Fatalf("task non eseguito qui: attese 0 istanze, ottenuto %d", len(got))
		}
	}, nonEseguito)
}

// Spegnere coi modes un task che un job o un worker di questo processo esegue è una contraddizione:
// ogni item fallirebbe a runtime, quindi l'avvio si ferma nominando task e modes.
func TestActiveInstances_ModesCheSpengonoUnTaskEseguitoQui(t *testing.T) {
	withMode(t, "WORKER")
	var msg string
	func() {
		defer func() {
			if r := recover(); r != nil {
				msg, _ = r.(string)
			}
		}()
		taskreg.Apply(func() { activeInstances("IMPORT", []string{"SCHEDULER"}) }, declared())
	}()
	if !strings.Contains(msg, "import-in") || !strings.Contains(msg, "SCHEDULER") || !strings.Contains(msg, "WORKER") {
		t.Fatalf("atteso un errore d'avvio che nomini task, modes e MODE corrente, ottenuto %q", msg)
	}
}

// L'ORDINE è l'invariante: taskreg.Instances va chiamata SEMPRE, anche quando il mode esclude il task,
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
		taskreg.Apply(func() { activeInstances("MAI_DICHIARATO", []string{"SCHEDULER"}) }, declared())
	}()
	if !strings.Contains(msg, "non dichiarati") {
		t.Fatalf("atteso il fail-fast sui type non dichiarati, ottenuto %q", msg)
	}
}
