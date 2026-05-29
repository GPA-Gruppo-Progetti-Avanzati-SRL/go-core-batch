package store

import "context"

// IData is the framework-level interface for task lifecycle tracking.
// Shared by both scheduler (SetTaskAssigned*) and worker (SetTaskStart/Done/Error) sides.
// Implementations live in store/mongostore and store/sqlstore.
type IData interface {
	SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string)
	SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string)
	SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string)
	SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string)
	SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string)
}
