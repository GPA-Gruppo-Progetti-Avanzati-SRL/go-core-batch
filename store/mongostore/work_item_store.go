package mongostore

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	mongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type workItemFilter struct {
	Id          string   `field:"_id"         operator:"$eq" omitempty:"true"`
	IdIn        []string `field:"_id"         operator:"$in" omitempty:"true"`
	Type        string   `field:"type"        operator:"$eq" omitempty:"true"`
	Status      string   `field:"status"      operator:"$eq" omitempty:"true"`
	Destination string   `field:"destination" operator:"$eq" omitempty:"true"`
	ObjectType  string   `field:"objectType"  operator:"$eq" omitempty:"true"`
}

func (f workItemFilter) GetFilterCollectionName(ctx context.Context) string {
	return store.CollectionWorkItems
}

// WorkItemData implements store.IWorkItemStore using MongoDB.
type WorkItemData struct {
	Service *mongolks.LinkedService
}

func NewWorkItemData(ms *mongolks.LinkedService) *WorkItemData {
	return &WorkItemData{Service: ms}
}

var _ store.IWorkItemStore = (*WorkItemData)(nil)

func (d *WorkItemData) FindPending(ctx context.Context, workType, destination, objectType string) ([]*store.WorkItem, *core.ApplicationError) {
	filter := workItemFilter{
		Type:        workType,
		Status:      store.StatusPending,
		Destination: destination,
		ObjectType:  objectType,
	}
	sort := page.SortRequest{{Field: "createTime", Dir: page.Asc}}
	return mongo.GetObjectsByFilterSorted[store.WorkItem](ctx, d.Service, filter, sort)
}

// ClaimPending atomically claims up to limit PENDING items using optimistic locking.
// For each candidate found, it attempts an UpdateOne WHERE status=PENDING — only items
// still PENDING at the time of the update are successfully claimed.
func (d *WorkItemData) ClaimPending(ctx context.Context, workType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	candidates, appErr := mongo.GetObjectsByFilterSorted[store.WorkItem](ctx, d.Service,
		workItemFilter{Type: workType, Status: store.StatusPending},
		page.SortRequest{{Field: "createTime", Dir: page.Asc}},
	)
	if appErr != nil {
		return nil, appErr
	}

	now := time.Now()
	var claimed []*store.WorkItem
	for _, item := range candidates {
		if len(claimed) >= limit {
			break
		}
		claimFilter := workItemFilter{Id: item.Id, Status: store.StatusPending}
		update := bson.M{"$set": bson.M{
			"status":     store.StatusInProgress,
			"lockedAt":   now,
			"updateTime": now,
		}}
		if err := mongo.UpdateOne(ctx, d.Service, claimFilter, update); err != nil {
			if err.Code == "MON-AGGINC" {
				continue // already claimed by another replica — skip
			}
			return claimed, err
		}
		item.Status = store.StatusInProgress
		item.LockedAt = &now
		claimed = append(claimed, item)
	}
	return claimed, nil
}

// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
// refreshing lockedAt to now and incrementing retry. Returns the items for
// immediate processing — no reset to PENDING, no waiting for the next tick.
// Uses optimistic per-item UpdateOne so concurrent replicas don't double-claim.
func (d *WorkItemData) RecoverOrphans(ctx context.Context, workType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")

	cursor, err := coll.Find(ctx,
		bson.M{"type": workType, "status": store.StatusInProgress, "lockedAt": bson.M{"$lt": cutoff}},
		options.Find().SetSort(bson.D{{Key: "lockedAt", Value: 1}}).SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	defer cursor.Close(ctx)

	var candidates []*store.WorkItem
	if err := cursor.All(ctx, &candidates); err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}

	var claimed []*store.WorkItem
	for _, item := range candidates {
		res, err := coll.UpdateOne(ctx,
			bson.M{"_id": item.Id, "status": store.StatusInProgress, "lockedAt": bson.M{"$lt": cutoff}},
			bson.M{"$set": bson.M{"lockedAt": now, "updateTime": now}, "$inc": bson.M{"retry": 1}},
		)
		if err != nil {
			return claimed, core.TechnicalErrorWithError(err)
		}
		if res.ModifiedCount == 1 {
			item.LockedAt = &now
			item.Retry++
			claimed = append(claimed, item)
		}
	}
	return claimed, nil
}

// MarkDone marks the given items as DONE. Uses raw driver to support variable-length batches.
func (d *WorkItemData) MarkDone(ctx context.Context, ids []string) *core.ApplicationError {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	_, err := coll.UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		bson.M{"$set": bson.M{"status": store.StatusDone, "updateTime": now, "lockedAt": nil}},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	return nil
}

// MarkFailed marks the given item as FAILED. Uses raw driver to avoid count validation.
func (d *WorkItemData) MarkFailed(ctx context.Context, id, reason string) *core.ApplicationError {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	_, err := coll.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": store.StatusFailed, "error": reason, "updateTime": now, "lockedAt": nil}},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	return nil
}

func (d *WorkItemData) Insert(ctx context.Context, items []*store.WorkItem) *core.ApplicationError {
	list := make([]mongo.ICollection, len(items))
	for i, item := range items {
		list[i] = item
	}
	return mongo.InsertMany(ctx, d.Service, list)
}

func (d *WorkItemData) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	objectIds := make([]string, len(items))
	workType := items[0].Type
	for i, item := range items {
		objectIds[i] = item.ObjectId
	}
	cursor, err := coll.Find(ctx, bson.M{
		"type":     workType,
		"objectId": bson.M{"$in": objectIds},
		"status":   bson.M{"$in": []string{store.StatusPending, store.StatusInProgress}},
	})
	if err != nil {
		return 0, core.TechnicalErrorWithError(err)
	}
	defer cursor.Close(ctx)
	active := make(map[string]struct{})
	for cursor.Next(ctx) {
		var doc struct {
			ObjectId string `bson:"objectId"`
		}
		if err := cursor.Decode(&doc); err == nil {
			active[doc.ObjectId] = struct{}{}
		}
	}
	var toInsert []mongo.ICollection
	for _, item := range items {
		if _, exists := active[item.ObjectId]; !exists {
			toInsert = append(toInsert, item)
		}
	}
	if len(toInsert) == 0 {
		return 0, nil
	}
	if appErr := mongo.InsertMany(ctx, d.Service, toInsert); appErr != nil {
		return 0, appErr
	}
	return len(toInsert), nil
}
