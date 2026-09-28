package store

import "context"

// TaskLogWriter implementa le cinque Set* di IData — che sono la stessa riga con uno stato diverso
// — lasciando al backend la sola scrittura fisica.
//
// I due backend le avevano copiate carattere per carattere: una trentina di righe ciascuno in cui
// l'unica differenza era la chiamata di insert. Trenta righe identiche in due file sono trenta
// righe che possono divergere, e il filtro del livello (TaskLogLevel) andava ricordato in tutte e
// due le copie.
//
// Si incorpora nell'implementazione di IData, che resta proprietaria di InsertTaskLogs (scrittura
// in blocco) e PurgeTaskLogs (retention): lì i due backend fanno davvero cose diverse — una
// InsertMany contro una INSERT multi-valore, una delete per id contro una per ctid.
type TaskLogWriter struct {
	// Level dice quali righe scrivere. Lo zero value è "tutte", cioè la condotta storica.
	Level TaskLogLevel
	// Insert scrive UNA riga. Non ritorna errore di proposito: una riga di task_logs è
	// osservabilità, e perderla non deve cambiare l'esito del task — il backend la logga e
	// prosegue, che è ciò che entrambi già facevano.
	Insert func(ctx context.Context, tl *TaskLog)
}

func (w TaskLogWriter) SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	w.write(ctx, taskid, jobid, typeTask, objectid, TaskLogStart, "")
}

func (w TaskLogWriter) SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	w.write(ctx, taskid, jobid, typeTask, objectid, TaskLogDone, "")
}

func (w TaskLogWriter) SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	w.write(ctx, taskid, jobid, typeTask, objectid, TaskLogError, errMsg)
}

func (w TaskLogWriter) SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	w.write(ctx, taskid, jobid, typeTask, objectid, TaskLogAssigned, "")
}

func (w TaskLogWriter) SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	w.write(ctx, taskid, jobid, typeTask, objectid, TaskLogAssignedKO, errMsg)
}

// write applica il filtro del livello prima di scrivere: è il punto unico in cui `task-log:
// errors|off` ha effetto sulle righe singole (per quelle in blocco è TaskLogLevel.Filter).
func (w TaskLogWriter) write(ctx context.Context, taskid, jobid, typeTask, objectid, status, errMsg string) {
	if !w.Level.Records(status) {
		return
	}
	w.Insert(ctx, NewTaskLog(taskid, jobid, typeTask, objectid, status, errMsg))
}
