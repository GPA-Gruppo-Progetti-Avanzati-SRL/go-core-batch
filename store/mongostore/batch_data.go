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

// batchData implements store.IData using MongoDB.
type batchData struct {
	Service *mongolks.LinkedService
}

func newBatchData(ms *mongolks.LinkedService) *batchData {
	return &batchData{Service: ms}
}

var _ store.IData = (*batchData)(nil)

func (d *batchData) SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "START", "")
}

func (d *batchData) SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "DONE", "")
}

func (d *batchData) SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ERROR", errMsg)
}

func (d *batchData) SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ASSIGNED", "")
}

func (d *batchData) SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, "ASSIGNEDKO", errMsg)
}

func (d *batchData) insertTask(ctx context.Context, taskid, jobId, typeTask, objectid, status, errMsg string) {
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
	if _, err := mongo.InsertOne(ctx, d.Service, obj); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire task log: %s", err.Message)
	}
}
