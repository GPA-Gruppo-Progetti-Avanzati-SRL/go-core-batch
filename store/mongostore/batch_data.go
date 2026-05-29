// Package mongostore provides MongoDB-backed implementations of store.IData and store.IWorkItemStore.
// Import this package only in applications that use MongoDB.
package mongostore

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	mongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
)

// BatchData implements store.IData using MongoDB.
type BatchData struct {
	Service *mongolks.LinkedService
}

func NewBatchData(ms *mongolks.LinkedService) *BatchData {
	return &BatchData{Service: ms}
}

var _ store.IData = (*BatchData)(nil)

func (d *BatchData) SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "START", "")
}

func (d *BatchData) SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "DONE", "")
}

func (d *BatchData) SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ERROR", errMsg)
}

func (d *BatchData) SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ASSIGNED", "")
}

func (d *BatchData) SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ASSIGNEDKO", errMsg)
}

func (d *BatchData) insertTask(ctx context.Context, taskid, jobId, typeTask, objectid, status, errMsg string) {
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
	if err := mongo.InsertOne(ctx, d.Service, obj); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire task log: %s", err.Message)
	}
}
