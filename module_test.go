package batch

import (
	"slices"
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskreg"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/worker"
)

func job(typ string, props properties.Properties) scheduler.Config {
	return scheduler.Config{Name: typ, Type: typ, Properties: props}
}

// Referenced è tutto ciò che jobs:/workers: nominano, in qualunque processo lo eseguano: è ciò che si
// valida (un nome inesistente è un typo che ferma l'avvio).
func TestActiveSet_ReferencedTuttiINomi(t *testing.T) {
	cfg := &Config{
		JobsConfig: []scheduler.Config{
			job("DistribuiteTaskByQuery", properties.Properties{"task": "BonifyInit"}),
			job("FeedTask", properties.Properties{"task": "hello-nightly"}),
		},
		WorkersConfig: []worker.Config{{Name: "Default", Tasks: []string{"BonifyInit", "AggiornaLimiti"}}},
	}
	a := activeSet(cfg, newTopology(cfg), nil, nil)
	for _, want := range []string{"BonifyInit", "hello-nightly", "AggiornaLimiti"} {
		if !slices.Contains(a.Referenced, want) {
			t.Fatalf("%q deve essere un riferimento, ottenuto %v", want, a.Referenced)
		}
	}
}

// Executed è ciò che QUESTO processo esegue, ed è ciò che diventa runner. La tabella copre le
// combinazioni che contano: dispatch locale o gRPC, mode di scheduler o di worker, job che eseguono
// o che si limitano ad accodare.
func TestActiveSet_Executed(t *testing.T) {
	withMode := func(m string) func() {
		prev := core.Mode
		core.Mode = m
		return func() { core.Mode = prev }
	}
	sched, work := []string{"SCHEDULER", "BATCH"}, []string{"WORKER", "BATCH"}
	jobs := []scheduler.Config{
		job("SingleTask", properties.Properties{"task": "single"}),
		job("DistribuiteTask", properties.Properties{"task": "dist", "limit": 10}),
		job("FeedTask", properties.Properties{"task": "accodato"}),
		job("NotificationKafka", properties.Properties{"stream": "notifiche"}),
		{Name: "spento", Type: "SingleTask", Disabled: true, Properties: properties.Properties{"task": "spento"}},
	}
	workers := []worker.Config{{Name: "pool", Size: 1, Tasks: []string{"dal-worker"}}}

	casi := []struct {
		nome, mode string
		grpcClient string
		grpcPort   int
		want       []string
	}{
		{"scheduler, dispatch locale", "SCHEDULER", "", 0, []string{"single", "dist"}},
		{"scheduler, dispatch gRPC", "SCHEDULER", "worker:9000", 0, []string{"single"}},
		{"worker col pool gRPC", "WORKER", "", 9000, []string{"dal-worker"}},
		{"BATCH: scheduler e worker insieme", "BATCH", "worker:9000", 9000, []string{"single", "dal-worker"}},
		{"API: nulla", "API", "", 9000, nil},
	}
	for _, c := range casi {
		t.Run(c.nome, func(t *testing.T) {
			defer withMode(c.mode)()
			cfg := &Config{JobsConfig: jobs, WorkersConfig: workers}
			cfg.Grpc.Client.Url = c.grpcClient
			cfg.Grpc.Server.Port = c.grpcPort
			got := activeSet(cfg, newTopology(cfg), sched, work).Executed
			if !slices.Equal(got, c.want) {
				t.Fatalf("Executed = %v, atteso %v", got, c.want)
			}
		})
	}
}

// I componenti si deducono dai job attivi: un job disabilitato non ne porta nessuno, e il dispatch
// è gRPC solo con `grpc.client.url`.
func TestTopology(t *testing.T) {
	cfg := &Config{JobsConfig: []scheduler.Config{
		job("SingleTask", nil),
		{Name: "spento", Type: "PurgeWorkItems", Disabled: true},
	}}
	topo := newTopology(cfg)
	if !topo.jobTypes["SingleTask"] || topo.jobTypes["PurgeWorkItems"] {
		t.Fatalf("job type attivi errati: %v", topo.jobTypes)
	}
	if topo.distributed() || topo.grpcDispatch || topo.workerPool {
		t.Fatalf("nessun DistribuiteTask, nessun gRPC, nessun worker pool: %+v", topo)
	}

	cfg.JobsConfig = append(cfg.JobsConfig, job("DistribuiteTaskByS3File", nil))
	cfg.Grpc.Client.Url = "worker:9000"
	cfg.WorkersConfig = []worker.Config{{Name: "pool", Tasks: []string{"x"}}}
	topo = newTopology(cfg)
	if !topo.distributed() || !topo.grpcDispatch {
		t.Fatalf("DistribuiteTaskByS3File con grpc.client.url: atteso dispatch gRPC, %+v", topo)
	}
	if topo.workerPool {
		t.Fatal("workers: senza grpc.server.port non è un worker pool")
	}
	if topo.executesLocally("DistribuiteTaskByS3File") || !topo.executesLocally("SingleTask") || topo.executesLocally("FeedTask") {
		t.Fatal("executesLocally: SingleTask sempre, DistribuiteTask* solo in-process, FeedTask mai")
	}
}

// I `workers:` sono serviti solo dal worker pool gRPC: senza `grpc.server.port` il pool non
// riceverebbe nulla, e nei worker modes l'avvio si ferma.
func TestModule_WorkersSenzaServerGrpc(t *testing.T) {
	prev := core.Mode
	defer func() { core.Mode = prev }()
	core.Mode = "WORKER"
	defer func() {
		msg, _ := recover().(string)
		if !strings.Contains(msg, "grpc.server.port") {
			t.Fatalf("atteso un panic che nomini grpc.server.port, ottenuto %q", msg)
		}
	}()
	Module(&Config{WorkersConfig: []worker.Config{{Name: "pool", Tasks: []string{"x"}}}}, nil,
		WithStore(func(...string) {}), WithLocker(func(...string) {}), WithWorkerModes("WORKER"))
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

// Il fail-fast resta pieno nei mode che il batch lo eseguono: un riferimento a un task non
// dichiarato ferma l'avvio.
func TestApply_StillFailsOnUnknownExplicitReference(t *testing.T) {
	cfg := &Config{JobsConfig: []scheduler.Config{
		job("DistribuiteTask", properties.Properties{"task": "TaskInesistente"}),
	}}
	a := activeSet(cfg, newTopology(cfg), nil, nil)
	defer func() {
		r := recover()
		msg, _ := r.(string)
		if !strings.Contains(msg, "TaskInesistente") {
			t.Fatalf("atteso panic che nomina il riferimento sconosciuto, ottenuto %v", r)
		}
	}()
	taskreg.Apply(func() {}, a)
}

// I due backend obbligatori devono fermare l'avvio se non sono passati, e il messaggio deve dire
// COSA scegliere: sono l'unica configurazione di batch.Module che non ha un default sensato —
// dove vivono i work item e dove vive il lock non sono domande a cui la libreria possa rispondere.
func TestModule_BackendObbligatori(t *testing.T) {
	casi := map[string][]Option{
		"senza WithStore":  {WithLocker(func(...string) {})},
		"senza WithLocker": {WithStore(func(...string) {})},
	}
	for nome, opts := range casi {
		t.Run(nome, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("atteso un panic al wiring")
				}
				msg, _ := r.(string)
				if !strings.Contains(msg, "obbligatori") && !strings.Contains(msg, "obbligatorio") {
					t.Fatalf("il panic non dice che l'opzione è obbligatoria: %v", r)
				}
				if !strings.Contains(msg, "Module") {
					t.Fatalf("il panic non nomina un backend da passare: %v", r)
				}
			}()
			Module(&Config{}, nil, opts...)
		})
	}
}
