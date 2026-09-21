package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx/fxtest"
)

// bloccante tiene occupata la goroutine del task finché release non viene chiuso.
type bloccante struct {
	partito chan struct{}
	release chan struct{}
	finiti  atomic.Int32
}

func (b *bloccante) GetServices() *bloccante { return b }

func (b *bloccante) GetTaskExecutions(string) (RunTask[*bloccante], bool) {
	return func(t *Task, _ *bloccante, _ store.IWorkItemStore) error {
		select {
		case b.partito <- struct{}{}:
		default:
		}
		select {
		case <-b.release:
		case <-t.Context.Done():
		}
		b.finiti.Add(1)
		return nil
	}, true
}

// Il pool deve DRENARE le task in volo su OnStop, non troncarle: prima usciva al primo segnale
// SIGTERM — intercettato dentro la libreria, quindi PRIMA che OnStop girasse — e le task già
// partite venivano abbandonate a metà, coi loro item fermi IN_PROGRESS fino al recupero orfani.
func TestOnStop_DrenaLeTaskInVolo(t *testing.T) {
	svc := &bloccante{partito: make(chan struct{}, 1), release: make(chan struct{})}
	lc := fxtest.NewLifecycle(t)
	w := NewWorkers[*bloccante](lc, []Config{{Name: "Default", Size: 1, Tasks: []string{"T"}}},
		fakeData{}, svc, &fakeStore{item: &store.WorkItem{Id: "wi", LockToken: "tok"}})
	lc.RequireStart()

	ctx, cancel := context.WithCancel(context.Background())
	ch := w.GetChannel("T")
	task := GenerateTask("t1", "j1", "T", "wi", ctx, cancel)
	task.Item = &store.WorkItem{Id: "wi", LockToken: "tok"}
	ch <- &task

	select {
	case <-svc.partito:
	case <-time.After(2 * time.Second):
		t.Fatal("la task non è partita")
	}

	// Lo stop deve ATTENDERE: si sblocca la task poco dopo, e OnStop non deve tornare prima.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(svc.release)
	}()
	lc.RequireStop()

	if got := svc.finiti.Load(); got != 1 {
		t.Fatalf("task completate = %d, attesa 1: OnStop non ha atteso il drain", got)
	}
}

// Oltre il deadline del context di stop, il drain rinuncia invece di tenere in piedi il processo:
// gli item residui restano IN_PROGRESS e li recupera RecoverOrphans, che è il contratto dichiarato.
func TestOnStop_DrainScadutoNonBlocca(t *testing.T) {
	svc := &bloccante{partito: make(chan struct{}, 1), release: make(chan struct{})}
	defer close(svc.release)

	lc := fxtest.NewLifecycle(t)
	w := NewWorkers[*bloccante](lc, []Config{{Name: "Default", Size: 1, Tasks: []string{"T"}}},
		fakeData{}, svc, &fakeStore{item: &store.WorkItem{Id: "wi", LockToken: "tok"}})
	lc.RequireStart()

	ctx, cancel := context.WithCancel(context.Background())
	ch := w.GetChannel("T")
	task := GenerateTask("t1", "j1", "T", "wi", ctx, cancel)
	task.Item = &store.WorkItem{Id: "wi", LockToken: "tok"}
	ch <- &task
	<-svc.partito

	// OnStop con un deadline corto: deve tornare, non restare appeso alla task bloccata.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()
	fatto := make(chan error, 1)
	go func() { fatto <- lc.Stop(stopCtx) }()

	select {
	case <-fatto:
	case <-time.After(3 * time.Second):
		t.Fatal("OnStop non è tornato: il drain non rispetta il deadline del context di stop")
	}
}
