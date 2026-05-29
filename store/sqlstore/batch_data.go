// Package sqlstore provides bun/SQL-backed implementations of store.IData and store.IWorkItemStore.
// Import this package only in applications that use a SQL database.
package sqlstore

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/rs/zerolog/log"
	"github.com/uptrace/bun"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// BatchDataSQL implements store.IData using a SQL database via bun.
type BatchDataSQL struct {
	DB *bun.DB
}

func NewBatchDataSQL(db *bun.DB) *BatchDataSQL {
	return &BatchDataSQL{DB: db}
}

var _ store.IData = (*BatchDataSQL)(nil)

func (d *BatchDataSQL) SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "START", "")
}

func (d *BatchDataSQL) SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "DONE", "")
}

func (d *BatchDataSQL) SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ERROR", errMsg)
}

func (d *BatchDataSQL) SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ASSIGNED", "")
}

func (d *BatchDataSQL) SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ASSIGNEDKO", errMsg)
}

func (d *BatchDataSQL) insertTask(ctx context.Context, taskid, jobId, typeTask, objectid, status, errMsg string) {
	obj := &store.TaskLog{
		TaskID:   taskid,
		JobID:    jobId,
		Type:     typeTask,
		Stato:    status,
		Hostname: core.GetHostname(),
		Logdate:  time.Now(),
		Objectid: objectid,
		Error:    errMsg,
	}
	if err := coresql.InsertOne(ctx, d.DB, obj); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire task log: %s", err.Message)
	}
}
