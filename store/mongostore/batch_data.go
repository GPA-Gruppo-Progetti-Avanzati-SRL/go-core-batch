// Package mongostore provides MongoDB-backed implementations of store.IData and store.IWorkItemStore.
// Import this package only in applications that use MongoDB.
package mongostore

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	mongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo/mongoutil"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// batchData implements store.IData using MongoDB.
type batchData struct {
	Service *mongo.Service
	// Level dice quali righe scrivere. Lo zero value è "tutte", cioè la condotta storica.
	Level store.TaskLogLevel
}

// batchDataParams: il livello è OPZIONALE perché il wiring manuale (senza batch.Module) non lo
// fornisce, e in quel caso deve valere il default storico invece di far fallire l'avvio.
type batchDataParams struct {
	core.In
	Service *mongo.Service
	Level   store.TaskLogLevel `optional:"true"`
}

func newBatchData(p batchDataParams) *batchData {
	return &batchData{Service: p.Service, Level: p.Level}
}

var _ store.IData = (*batchData)(nil)

func (d *batchData) SetTaskStart(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, store.TaskLogStart, "")
}

func (d *batchData) SetTaskDone(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, store.TaskLogDone, "")
}

func (d *batchData) SetTaskInError(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, store.TaskLogError, errMsg)
}

func (d *batchData) SetTaskAssigned(ctx context.Context, taskid, jobid, typeTask, objectid string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, store.TaskLogAssigned, "")
}

func (d *batchData) SetTaskAssignationKO(ctx context.Context, taskid, jobid, typeTask, objectid, errMsg string) {
	d.insertTask(ctx, taskid, jobid, typeTask, objectid, store.TaskLogAssignedKO, errMsg)
}

func (d *batchData) insertTask(ctx context.Context, taskid, jobId, typeTask, objectid, status, errMsg string) {
	if !d.Level.Records(status) {
		return
	}
	obj := store.NewTaskLog(taskid, jobId, typeTask, objectid, status, errMsg)
	if _, err := d.Service.InsertOne(ctx, obj); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire task log: %s", err.Message)
	}
}

// InsertTaskLogs scrive le righe in una sola InsertMany.
func (d *batchData) InsertTaskLogs(ctx context.Context, logs []*store.TaskLog) {
	logs = d.Level.Filter(logs)
	if len(logs) == 0 {
		return
	}
	list := make([]mongo.ICollection, len(logs))
	for i, tl := range logs {
		list[i] = tl
	}
	if err := d.Service.InsertMany(ctx, list); err != nil {
		log.Error().Err(err).Msgf("Impossibile inserire %d task log: %s", len(logs), err.Message)
	}
}

// PurgeTaskLogs cancella le righe più vecchie di olderThan, al più limit per chiamata.
func (d *batchData) PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.TaskLog{}.GetCollectionName(ctx), "")
	cur, err := coll.Find(ctx, bson.M{"logdate": bson.M{"$lt": olderThan}},
		options.Find().SetSort(bson.D{{Key: "logdate", Value: 1}}).SetLimit(int64(limit)).SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	defer mongoutil.CloseCursor(ctx, cur, "PurgeTaskLogs")
	var vittime []struct {
		Id any `bson:"_id"`
	}
	if err := cur.All(ctx, &vittime); err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	if len(vittime) == 0 {
		return 0, nil
	}
	ids := make([]any, len(vittime))
	for i, v := range vittime {
		ids[i] = v.Id
	}
	res, err := coll.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	return int(res.DeletedCount), nil
}
