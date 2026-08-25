package batch

import (
	"slices"
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"
)

func job(typ string, props core.Properties) scheduler.Config {
	return scheduler.Config{Name: typ, Type: typ, Properties: props}
}

// La property `task` di un distributedjob e le `tasks` di un worker pool sono nomi scritti a mano:
// riferimenti espliciti, quindi validati (un nome inesistente è un typo che ferma l'avvio).
func TestActiveSet_ExplicitReferences(t *testing.T) {
	a := ActiveSet(&Config{
		JobsConfig: []scheduler.Config{
			job("DistribuiteTaskByQuery", core.Properties{"task": "BonifyInit"}),
			job("HelloWorld", core.Properties{"workType": "hello-nightly"}),
		},
		WorkersConfig: []worker.Config{{Name: "Default", Tasks: []string{"BonifyInit", "AggiornaLimiti"}}},
	})
	for _, want := range []string{"BonifyInit", "hello-nightly", "AggiornaLimiti"} {
		if !slices.Contains(a.Referenced, want) {
			t.Fatalf("%q deve essere un riferimento esplicito, ottenuto %v", want, a.Referenced)
		}
	}
	if len(a.Implied) != 0 {
		t.Fatalf("nessun riferimento dedotto atteso, ottenuto %v", a.Implied)
	}
}

// Un job type del framework non nomina alcun task: finisce fra i dedotti, non fra gli espliciti,
// altrimenti l'avvio fallirebbe pretendendo una voce `tasks:` di nome NotificationKafka.
func TestActiveSet_FrameworkJobTypeIsNotAnExplicitReference(t *testing.T) {
	a := ActiveSet(&Config{JobsConfig: []scheduler.Config{
		job("NotificationKafka", core.Properties{"topic": "eventi", "destination": "BACHECA"}),
	}})
	if len(a.Referenced) != 0 {
		t.Fatalf("un job type del framework non è un riferimento esplicito: %v", a.Referenced)
	}
	if !slices.Contains(a.Implied, "NotificationKafka") {
		t.Fatalf("atteso fra i dedotti, ottenuto %v", a.Implied)
	}
	// E infatti non fa fallire la validazione.
	task.Apply(func() {}, a)
}

// Il caso opposto resta servito: un simplejob senza `workType` gira il task omonimo, che va
// istanziato anche se nessun altro lo nomina.
func TestActiveSet_ImpliedActivatesSameNamedTask(t *testing.T) {
	a := ActiveSet(&Config{
		JobsConfig:  []scheduler.Config{job("HelloWorld", nil)},
		TasksConfig: []task.Config{{Type: "HelloWorld"}, {Name: "altro", Type: "HelloWorld"}},
	})
	var got []task.Config
	task.Apply(func() { got = task.Instances("HelloWorld") }, a)
	if len(got) != 1 || got[0].Name != "HelloWorld" {
		t.Fatalf("atteso il solo task omonimo del job type, ottenuto %+v", got)
	}
}

func TestActiveSet_SkipsDisabledJobs(t *testing.T) {
	a := ActiveSet(&Config{JobsConfig: []scheduler.Config{
		{Name: "spento", Type: "DistribuiteTask", Disabled: true, Properties: core.Properties{"task": "BonifyInit"}},
	}})
	if len(a.Referenced) != 0 || len(a.Implied) != 0 {
		t.Fatalf("un job disabilitato non referenzia nulla: %v / %v", a.Referenced, a.Implied)
	}
}

// In un mode che non è né scheduler né worker (es. API) il batch non costruisce nulla: la
// registrazione dei runner — e con essa il fail-fast sulla coerenza di `tasks:` — non deve girare.
func TestBatchActive_GatedOnSchedulerOrWorkerModes(t *testing.T) {
	prev := core.Mode
	defer func() { core.Mode = prev }()

	core.Mode = "API"
	if batchActive([]string{"SCHEDULER", "BATCH"}, []string{"WORKER", "BATCH"}) {
		t.Fatal("in mode API il batch è inerte: nessuna registrazione")
	}
	for _, m := range []string{"SCHEDULER", "WORKER", "BATCH"} {
		core.Mode = m
		if !batchActive([]string{"SCHEDULER", "BATCH"}, []string{"WORKER", "BATCH"}) {
			t.Fatalf("in mode %s il batch è attivo", m)
		}
	}
	// Una famiglia con modes vuoti è sempre attiva: l'app che non gate-a nulla si comporta come prima.
	core.Mode = "API"
	if !batchActive(nil, []string{"WORKER"}) {
		t.Fatal("scheduler modes vuoti = sempre attivo")
	}
	if !batchActive(nil, nil) {
		t.Fatal("nessun gate = sempre attivo")
	}
}

// Il fail-fast resta pieno nei mode che il batch lo eseguono: un riferimento esplicito a un task
// non dichiarato ferma l'avvio.
func TestApply_StillFailsOnUnknownExplicitReference(t *testing.T) {
	a := ActiveSet(&Config{JobsConfig: []scheduler.Config{
		job("DistribuiteTask", core.Properties{"task": "TaskInesistente"}),
	}})
	defer func() {
		r := recover()
		msg, _ := r.(string)
		if !strings.Contains(msg, "TaskInesistente") {
			t.Fatalf("atteso panic che nomina il riferimento sconosciuto, ottenuto %v", r)
		}
	}()
	task.Apply(func() {}, a)
}
