package store

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// IData is the framework-level interface for task lifecycle tracking.
// Shared by both scheduler (SetTaskAssigned*) and worker (SetTaskStart/Done/Error) sides.
// Implementations live in store/mongostore and store/sqlstore.
type IData interface {
	SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string)
	SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string)
	SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string)
	SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string)
	SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string)
	// InsertTaskLogs scrive più righe in UNA sola operazione. La fase di dispatch di un tick ne
	// produce una per item: con limit 100 erano 100 insert sincrone dentro il tick, cioè dentro
	// il lock del job, prima che potesse chiudere.
	//
	// Il filtro del TaskLogLevel è applicato dall'implementazione, come per le Set*.
	InsertTaskLogs(ctx context.Context, logs []*TaskLog)
	// PurgeTaskLogs cancella le righe più vecchie di olderThan, al più limit per chiamata.
	// Senza, task_logs cresce per sempre: è la collection che riceve più scritture di tutte.
	PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, *core.ApplicationError)
}
