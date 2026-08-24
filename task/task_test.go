package task

import (
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

func names(cs []Config) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	return out
}

// mustPanic esegue fn e ritorna il messaggio del panic (fallisce se non panica).
func mustPanic(t *testing.T, fn func()) string {
	t.Helper()
	var msg string
	func() {
		defer func() {
			if r := recover(); r != nil {
				msg = strings.ToLower(strings.TrimSpace(sprint(r)))
			}
		}()
		fn()
	}()
	if msg == "" {
		t.Fatal("atteso panic")
	}
	return msg
}

func sprint(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if e, ok := v.(error); ok {
		return e.Error()
	}
	return ""
}

// Due voci con lo stesso type e properties diverse sono due istanze distinte: è il caso "due job che
// eseguono lo stesso task con configurazione diversa".
func TestInstances_OnePerDeclaredTask(t *testing.T) {
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{
		Tasks: []Config{
			{Name: "import-in", Type: "IMPORT", Properties: core.Properties{"folder": "/data/in"}},
			{Name: "import-bulk", Type: "IMPORT", Properties: core.Properties{"folder": "/data/bulk"}},
		},
		Referenced: []string{"import-in", "import-bulk"},
	})
	if len(got) != 2 {
		t.Fatalf("attese 2 istanze, ottenuto %v", names(got))
	}
	if got[0].Properties.GetString("folder", "") != "/data/in" || got[1].Properties.GetString("folder", "") != "/data/bulk" {
		t.Fatalf("properties per istanza errate: %+v", got)
	}
}

// `name` omesso = uguale al `type`: unica scorciatoia ammessa, la voce va comunque dichiarata.
func TestInstances_NameDefaultsToType(t *testing.T) {
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{
		Tasks:      []Config{{Type: "IMPORT"}},
		Referenced: []string{"IMPORT"},
	})
	if len(got) != 1 || got[0].Name != "IMPORT" {
		t.Fatalf("atteso Name = Type, ottenuto %+v", got)
	}
}

// I task vanno SEMPRE dichiarati: un type registrato senza voce in `tasks:` fa fallire l'avvio,
// invece di lasciare un job che gira a vuoto.
func TestApply_PanicsOnUndeclaredTaskType(t *testing.T) {
	msg := mustPanic(t, func() {
		Apply(func() { Instances("IMPORT") }, ActiveSet{})
	})
	if !strings.Contains(msg, "import") || !strings.Contains(msg, "tasks") {
		t.Fatalf("il panic deve nominare il task e la sezione tasks: %q", msg)
	}
}

// Un job/worker che referenzia un task inesistente è un typo: l'app non parte.
func TestApply_PanicsOnUnknownReference(t *testing.T) {
	msg := mustPanic(t, func() {
		Apply(func() { Instances("IMPORT") }, ActiveSet{
			Tasks:      []Config{{Name: "import-in", Type: "IMPORT"}},
			Referenced: []string{"import-in", "import-sbagliato"},
		})
	})
	if !strings.Contains(msg, "import-sbagliato") {
		t.Fatalf("il panic deve nominare il riferimento sconosciuto: %q", msg)
	}
}

// Un task dichiarato ma non referenziato da alcun job/worker non viene istanziato: le sue dipendenze
// non entrano nel grafo fx (stesso principio dei consumer Kafka spenti).
func TestInstances_SkipsUnreferenced(t *testing.T) {
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{
		Tasks: []Config{
			{Name: "import-in", Type: "IMPORT"},
			{Name: "import-bulk", Type: "IMPORT"},
		},
		Referenced: []string{"import-in"},
	})
	if len(got) != 1 || got[0].Name != "import-in" {
		t.Fatalf("atteso il solo task referenziato, ottenuto %v", names(got))
	}
}

// Referenced vuota (config che non permette di dedurre i riferimenti) = nessun filtro.
func TestInstances_NoFilterWhenNoReferences(t *testing.T) {
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{Tasks: []Config{{Type: "IMPORT"}}})
	if len(got) != 1 {
		t.Fatalf("senza riferimenti tutto è attivo: %v", names(got))
	}
}

// Il caso opposto resta un Warn, non un errore: lo stesso YAML è condiviso fra i MODE e fra binari
// diversi, quindi un task dichiarato il cui runner vive altrove è legittimo.
func TestApply_TolerateDeclaredTypeWithoutRunner(t *testing.T) {
	Apply(func() { Instances("IMPORT") }, ActiveSet{
		Tasks: []Config{
			{Name: "import-in", Type: "IMPORT"},
			{Name: "notify-mail", Type: "NOTIFY"}, // nessun runner registrato qui
		},
		Referenced: []string{"import-in", "notify-mail"},
	})
}

// Registrare fuori dalla finestra aperta da Apply è un errore di wiring: lì la sezione `tasks:` non
// è nota, quindi il runner non potrebbe ricevere la sua configurazione.
func TestInstances_PanicsOutsideApply(t *testing.T) {
	if InApply() {
		t.Fatal("nessun Apply in corso")
	}
	mustPanic(t, func() { Instances("IMPORT") })
}

func TestApply_ClearsStateAfterRegister(t *testing.T) {
	Apply(func() {
		if !InApply() {
			t.Fatal("dentro register() lo stato deve essere disponibile")
		}
	}, ActiveSet{})
	if InApply() {
		t.Fatal("lo stato deve essere azzerato dopo Apply")
	}
}
