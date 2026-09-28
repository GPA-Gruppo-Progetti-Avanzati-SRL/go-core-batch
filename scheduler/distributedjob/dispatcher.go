package distributedjob

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// DispatchRequest è ciò che il job consegna al dispatcher per una singola esecuzione.
// È una struct e non una lista di parametri posizionali perché i campi sono cresciuti e
// perché Timeout, aggiunto per ultimo, è quello che si sbaglia più facilmente di posizione.
type DispatchRequest struct {
	// JobId identifica l'ESECUZIONE del job (scheduler.NewJobID), TaskId la singola task
	// dentro quell'esecuzione: sono le chiavi di correlazione dei task_logs.
	JobId  string
	TaskId string
	// TaskName è il nome dell'istanza di task da eseguire (= Item.TaskName).
	TaskName string
	// Item è il WorkItem INTERO, già claimato dal job: ClaimPending e RecoverOrphans ritornano
	// i record completi, quindi sul percorso in-process non c'è niente da rileggere. Il percorso
	// gRPC serializza il solo Id — è tutto ciò che può attraversare il filo — e il bridge lato
	// worker lo ricarica da lì.
	Item *store.WorkItem
	// Timeout è il tempo oltre il quale l'esecuzione va interrotta. È l'orphan timeout del job
	// (scheduler.Config.ResolveTimeouts), NON un valore scelto dal dispatcher: oltre quella
	// soglia l'item viene ri-claimato come orfano da un altro tick, e una task che continuasse a
	// girare sarebbe un secondo esecutore dello stesso item. Il fencing token impedisce al
	// perdente di finalizzare, ma non di aver già prodotto i suoi effetti.
	Timeout time.Duration
}

// ITaskDispatcher instrada una task verso chi la esegue. Le implementazioni sono due, e
// nessun'altra: localdispatcher.LocalDispatcher (in-process, goroutine + semaforo) e
// grpcdispatcher.GrpcDispatcher (client gRPC verso un processo worker). Il worker pool
// (worker.Workers) NON è un dispatcher: sta dall'altra parte del filo, dietro grpchandler.
type ITaskDispatcher interface {
	DispatchTask(ctx context.Context, req DispatchRequest) error
}
