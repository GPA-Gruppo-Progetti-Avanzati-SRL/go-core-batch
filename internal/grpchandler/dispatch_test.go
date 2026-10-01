package grpchandler

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/grpcproto"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type oneItem struct {
	store.IWorkItemStore
	item *store.WorkItem
}

func (o oneItem) GetById(context.Context, string) (*store.WorkItem, *core.Error) { return o.item, nil }

type countingRunner struct{ runs int }

func (c *countingRunner) Run(context.Context, *store.WorkItem) error { c.runs++; return nil }

// TestBridge_ItemRiclaimatoNonEseguito: il worker rilegge l'item dal DB, ma se il token non è più
// quello del dispatch l'item è stato ri-claimato mentre il task era in coda. Eseguirlo vorrebbe dire
// due esecutori dello stesso lavoro, e finalizzarlo col token riletto chiuderebbe l'esecuzione altrui.
func TestBridge_ItemRiclaimatoNonEseguito(t *testing.T) {
	r := &countingRunner{}
	svc := newRunnerService([]*taskrunner.TaskRunner{{TaskName: "t", Runner: r}})
	run, ok := svc.GetTaskExecutions("t")
	if !ok {
		t.Fatal("runner non instradato")
	}
	items := oneItem{item: &store.WorkItem{Id: "i1", LockToken: "nuovo"}}

	task := &worker.Task{WorkItemId: "i1", Context: t.Context(), DispatchToken: "vecchio"}
	if err := run(task, items); !errors.Is(err, store.ErrHandled) {
		t.Fatalf("atteso ErrHandled (nessun Mark*), ottenuto %v", err)
	}
	if r.runs != 0 {
		t.Fatal("il runner non doveva girare su un item ri-claimato")
	}

	task = &worker.Task{WorkItemId: "i1", Context: t.Context(), DispatchToken: "nuovo"}
	if err := run(task, items); err != nil || r.runs != 1 {
		t.Fatalf("token coincidente: err=%v runs=%d", err, r.runs)
	}
}

// TestRouter_DeadlineDalDispatch: il deadline dell'esecuzione è quello del dispatch (l'orphan
// timeout del job), non il context della RPC, che finisce con la risposta.
func TestRouter_DeadlineDalDispatch(t *testing.T) {
	ch := make(chan *worker.Task, 1)
	r := &Router{workers: workersWith("t", ch), taskServices: newRunnerService([]*taskrunner.TaskRunner{{TaskName: "t", Runner: &countingRunner{}}})}

	rpcCtx, cancelRPC := context.WithCancel(context.Background())
	if _, err := r.DistribuiteTask(rpcCtx, &proto.TaskMessage{TaskId: "x", TaskName: "t", WorkItemId: "i1", LockToken: "tok", TimeoutMs: 60_000}); err != nil {
		t.Fatal(err)
	}
	cancelRPC() // la RPC è finita: l'esecuzione deve proseguire
	task := <-ch
	if task.DispatchToken != "tok" {
		t.Fatalf("DispatchToken = %q", task.DispatchToken)
	}
	dl, ok := task.Context.Deadline()
	if !ok || time.Until(dl) > time.Minute || time.Until(dl) < 50*time.Second {
		t.Fatalf("deadline del task = %v (ok=%v), atteso ~1m dal dispatch", dl, ok)
	}
	if task.Context.Err() != nil {
		t.Fatal("la fine della RPC ha cancellato l'esecuzione")
	}
	task.Cancel()
}

func workersWith(name string, ch chan *worker.Task) *worker.Workers {
	return &worker.Workers{TaskChannel: map[string]chan *worker.Task{name: ch}, TaskRoutes: map[string]string{name: name}, StopChannel: make(chan struct{})}
}

// TestRouter_RifiutaDuranteLoStop: dopo l'OnStop del pool nessun worker preleva dal canale; un task
// accodato lì resterebbe IN_PROGRESS fino all'orphan timeout. Il router lo rifiuta, e lo scheduler
// rilascia l'item.
func TestRouter_RifiutaDuranteLoStop(t *testing.T) {
	ch := make(chan *worker.Task, 1)
	w := workersWith("t", ch)
	w.StopChannel = make(chan struct{})
	close(w.StopChannel)
	r := &Router{workers: w, taskServices: newRunnerService([]*taskrunner.TaskRunner{{TaskName: "t", Runner: &countingRunner{}}})}
	if _, err := r.DistribuiteTask(t.Context(), &proto.TaskMessage{TaskId: "x", TaskName: "t"}); err == nil {
		t.Fatal("atteso un rifiuto a pool in arresto")
	}
	if len(ch) != 0 {
		t.Fatal("il task è stato accodato comunque")
	}
}
