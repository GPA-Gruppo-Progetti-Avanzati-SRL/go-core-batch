package taskreg

import (
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
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
			{Name: "import-in", Type: "IMPORT", Properties: properties.Properties{"folder": "/data/in"}},
			{Name: "import-bulk", Type: "IMPORT", Properties: properties.Properties{"folder": "/data/bulk"}},
		},
		Referenced: []string{"import-in", "import-bulk"},
		Executed:   []string{"import-in", "import-bulk"},
	})
	if len(got) != 2 {
		t.Fatalf("attese 2 istanze, ottenuto %v", names(got))
	}
	if got[0].Properties.GetString("folder", "") != "/data/in" || got[1].Properties.GetString("folder", "") != "/data/bulk" {
		t.Fatalf("properties per istanza errate: %+v", got)
	}
}

// Il `name` è obbligatorio e non ha fallback sul `type`: è la chiave referenziata da jobs:/workers:
// e usata come WorkItem.Type, quindi una voce senza name non è raggiungibile da nessuno. Prima la
// fallback c'era, e nascondeva la chiave di routing.
func TestApply_PanicsOnTaskWithoutName(t *testing.T) {
	msg := mustPanic(t, func() {
		Apply(func() {}, ActiveSet{
			Tasks:      []Config{{Name: "import-in", Type: "IMPORT"}, {Type: "NOTIFY"}},
			Referenced: []string{"import-in"},
		})
	})
	if !strings.Contains(msg, "name") || !strings.Contains(msg, "notify") {
		t.Fatalf("il panic deve chiedere il name e nominare la voce colpevole: %q", msg)
	}
	if strings.Contains(msg, "import-in") {
		t.Fatalf("la voce con name non va segnalata: %q", msg)
	}
}

// checkNames gira PRIMA di register(): l'errore arriva sulla config, non su un runner costruito
// a metà.
func TestApply_NameCheckRunsBeforeRegister(t *testing.T) {
	registered := false
	mustPanic(t, func() {
		Apply(func() { registered = true }, ActiveSet{Tasks: []Config{{Type: "IMPORT"}}})
	})
	if registered {
		t.Fatal("register() non deve girare se la sezione `tasks:` è incoerente")
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

// Diventa runner solo ciò che questo processo ESEGUE. Un task nominato altrove — da un FeedTask, che
// accoda e basta, o da un job che dispatcha via gRPC a un worker remoto — è referenziato ma non
// eseguito qui: le sue dipendenze non entrano nel grafo fx.
func TestInstances_SoloIEseguitiQui(t *testing.T) {
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{
		Tasks: []Config{
			{Name: "import-in", Type: "IMPORT"},
			{Name: "import-bulk", Type: "IMPORT"},
		},
		Referenced: []string{"import-in", "import-bulk"},
		Executed:   []string{"import-in"},
	})
	if len(got) != 1 || got[0].Name != "import-in" {
		t.Fatalf("atteso il solo task eseguito qui, ottenuto %v", names(got))
	}
}

// Niente eseguito qui = niente runner: non c'è più il ripiego "nessun riferimento ⇒ tutto attivo",
// perché ActiveSet lo costruisce sempre batch, che sa cosa gira in questo processo.
func TestInstances_NienteSeNullaEEseguitoQui(t *testing.T) {
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{Tasks: []Config{{Name: "IMPORT", Type: "IMPORT"}}})
	if len(got) != 0 {
		t.Fatalf("nessun task eseguito qui: attese 0 istanze, ottenuto %v", names(got))
	}
}

// Un task che un job o un worker di questo processo esegue, ma il cui type nessun runner registra,
// fallirebbe item per item: l'avvio si ferma.
func TestApply_PanicsOnExecutedTaskWithoutRunner(t *testing.T) {
	msg := mustPanic(t, func() {
		Apply(func() { Instances("IMPORT") }, ActiveSet{
			Tasks:      []Config{{Name: "import-in", Type: "IMPORT"}, {Name: "notify-mail", Type: "NOTIFY"}},
			Referenced: []string{"import-in", "notify-mail"},
			Executed:   []string{"import-in", "notify-mail"},
		})
	})
	if !strings.Contains(msg, "notify-mail") || strings.Contains(msg, "import-in (") {
		t.Fatalf("il panic deve nominare il solo task senza runner: %q", msg)
	}
}

// Un task dichiarato e referenziato ma NON eseguito qui, il cui runner vive altrove, resta un Warn:
// lo stesso YAML è condiviso fra i MODE e fra binari diversi.
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

// La voce di `tasks:` deve arrivare al register INTERA. Ricostruirla campo per campo qui dentro è
// già costato il tetto ai ritentativi: MaxRetry non veniva copiato, quindi ResolveMaxRetry vedeva
// sempre nil e `max-retry:` non aveva alcun effetto su nessun task.
func TestInstances_ConservaMaxRetry(t *testing.T) {
	tre := 3
	var got []Config
	Apply(func() { got = Instances("IMPORT") }, ActiveSet{
		Tasks: []Config{
			{Name: "import-in", Type: "IMPORT", MaxRetry: &tre},
			{Name: "import-bulk", Type: "IMPORT"},
		},
		Referenced: []string{"import-in", "import-bulk"},
		Executed:   []string{"import-in", "import-bulk"},
	})
	if len(got) != 2 {
		t.Fatalf("attese 2 istanze, ottenuto %v", names(got))
	}
	if got[0].MaxRetry == nil || *got[0].MaxRetry != 3 || got[0].ResolveMaxRetry() != 3 {
		t.Fatalf("il tetto configurato non è arrivato al register: %+v", got[0])
	}
	if got[1].MaxRetry != nil || got[1].ResolveMaxRetry() != task.MaxRetryUnlimited {
		t.Fatalf("l'assenza del campo deve valere illimitato: %+v", got[1])
	}
}
