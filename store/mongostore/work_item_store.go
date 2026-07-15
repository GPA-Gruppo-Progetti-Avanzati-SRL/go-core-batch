package mongostore

import (
	"context"
	"errors"
	"fmt"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	mongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"go.mongodb.org/mongo-driver/v2/bson"
	mgodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type workItemFilter struct {
	Id          string   `field:"_id"         operator:"$eq"  omitempty:"true"`
	IdIn        []string `field:"_id"         operator:"$in"  omitempty:"true"`
	Type        string   `field:"type"        operator:"$eq"  omitempty:"true"`
	Status      string   `field:"status"      operator:"$eq"  omitempty:"true"`
	Destination string   `field:"destination" operator:"$eq"  omitempty:"true"`
	ObjectType  string   `field:"objectType"  operator:"$eq"  omitempty:"true"`
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
// The candidate query matches items whose next_run_at is due, treating a missing/null
// next_run_at as "due now" (mirrors the SQL store's `next_run_at IS NULL OR <= NOW()`),
// and is bounded by limit so the whole PENDING backlog is never loaded into memory.
func (d *WorkItemData) ClaimPending(ctx context.Context, workType, destination, objectType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")

	query := bson.M{
		"type":   workType,
		"status": store.StatusPending,
		// {nextRunAt: null} matches both missing and null fields in MongoDB.
		"$or": []bson.M{
			{"nextRunAt": nil},
			{"nextRunAt": bson.M{"$lte": now}},
		},
	}
	if destination != "" {
		query["destination"] = destination
	}
	if objectType != "" {
		query["objectType"] = objectType
	}

	cursor, err := coll.Find(ctx, query,
		options.Find().
			SetSort(bson.D{{Key: "nextRunAt", Value: 1}, {Key: "createTime", Value: 1}}).
			SetLimit(int64(limit)),
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
func (d *WorkItemData) RecoverOrphans(ctx context.Context, workType, destination, objectType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	cutoff := time.Now().Add(-maxAge)
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")

	query := bson.M{"type": workType, "status": store.StatusInProgress, "lockedAt": bson.M{"$lt": cutoff}}
	if destination != "" {
		query["destination"] = destination
	}
	if objectType != "" {
		query["objectType"] = objectType
	}
	cursor, err := coll.Find(ctx,
		query,
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

// MarkDone transitions the given IN_PROGRESS items to DONE.
// Returns an error if any id is not found in IN_PROGRESS state.
func (d *WorkItemData) MarkDone(ctx context.Context, ids []string) *core.ApplicationError {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}, "status": store.StatusInProgress},
		bson.M{"$set": bson.M{"status": store.StatusDone, "updateTime": now, "lockedAt": nil}},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if int(res.ModifiedCount) != len(ids) {
		return core.TechnicalErrorWithCodeAndMessage("WIS-MD-001",
			fmt.Sprintf("MarkDone: expected %d IN_PROGRESS items, modified %d", len(ids), res.ModifiedCount))
	}
	return nil
}

// MarkFailed transitions the given IN_PROGRESS item to FAILED.
// Returns an error if the item is not found in IN_PROGRESS state.
func (d *WorkItemData) MarkFailed(ctx context.Context, id, reason string) *core.ApplicationError {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateOne(ctx,
		bson.M{"_id": id, "status": store.StatusInProgress},
		bson.M{"$set": bson.M{"status": store.StatusFailed, "error": reason, "updateTime": now, "lockedAt": nil}},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if res.ModifiedCount == 0 {
		return core.TechnicalErrorWithCodeAndMessage("WIS-MF-001",
			fmt.Sprintf("MarkFailed: item %q not found in IN_PROGRESS state", id))
	}
	return nil
}

// MarkPending resets the given IN_PROGRESS item back to PENDING for retry.
// Returns an error if the item is not found in IN_PROGRESS state.
func (d *WorkItemData) MarkPending(ctx context.Context, id string, after time.Duration) *core.ApplicationError {
	now := time.Now()
	nextRunAt := now.Add(after)
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateOne(ctx,
		bson.M{"_id": id, "status": store.StatusInProgress},
		bson.M{
			"$set": bson.M{"status": store.StatusPending, "lockedAt": nil, "updateTime": now, "nextRunAt": nextRunAt},
			"$inc": bson.M{"retry": 1},
		},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if res.ModifiedCount == 0 {
		return core.TechnicalErrorWithCodeAndMessage("WIS-MP-001",
			fmt.Sprintf("MarkPending: item %q not found in IN_PROGRESS state", id))
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

func (d *WorkItemData) DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.DeleteOne(ctx, bson.M{"_id": id, "status": store.StatusPending})
	if err != nil {
		return false, core.TechnicalErrorWithError(err)
	}
	return res.DeletedCount == 1, nil
}

func (d *WorkItemData) GetById(ctx context.Context, id string) (*store.WorkItem, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	var item store.WorkItem
	if err := coll.FindOne(ctx, bson.M{"_id": id}).Decode(&item); err != nil {
		if errors.Is(err, mgodriver.ErrNoDocuments) {
			return nil, core.NotFoundError()
		}
		return nil, core.TechnicalErrorWithError(err)
	}
	return &item, nil
}

func (d *WorkItemData) HasActive(ctx context.Context, workType, objectId string) (bool, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	count, err := coll.CountDocuments(ctx, bson.M{
		"type":     workType,
		"objectId": objectId,
		"status":   bson.M{"$in": []string{store.StatusPending, store.StatusInProgress}},
	})
	if err != nil {
		return false, core.TechnicalErrorWithError(err)
	}
	return count > 0, nil
}

// InsertIfNotActive inserts each item only if no active (PENDING or IN_PROGRESS) entry
// exists for the same (type, objectId). Relies on the partial unique index
// uk_workitem_active — call EnsureIndexes at startup to create it.
func (d *WorkItemData) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	var inserted int
	for _, item := range items {
		if _, err := coll.InsertOne(ctx, item); err != nil {
			if mgodriver.IsDuplicateKeyError(err) {
				continue
			}
			return inserted, core.TechnicalErrorWithError(err)
		}
		inserted++
	}
	return inserted, nil
}

func (d *WorkItemData) List(ctx context.Context, workType, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	filter := workItemFilter{Type: workType, Status: status}
	var sortOpt options.Lister[options.FindOptions]
	if len(sort) > 0 {
		sortOpt = mongo.FindSortOption(sort)
	} else {
		sortOpt = options.Find().SetSort(bson.D{{Key: "createTime", Value: -1}})
	}
	flat, err := mongo.GetPageByFilter[store.WorkItem](ctx, d.Service, filter, paging, sortOpt)
	if err != nil {
		return nil, err
	}
	items := make([]*store.WorkItem, len(flat))
	for i := range flat {
		items[i] = &flat[i]
	}
	return items, nil
}

// EnsureIndexes creates the indexes required by WorkItemData. Call once at application startup.
// The partial unique index uk_workitem_active prevents concurrent insertion of duplicate
// active (PENDING or IN_PROGRESS) items for the same (type, objectId).
func EnsureIndexes(ctx context.Context, service *mongolks.LinkedService) error {
	coll := service.GetCollection(store.CollectionWorkItems, "")
	_, err := coll.Indexes().CreateOne(ctx, mgodriver.IndexModel{
		Keys: bson.D{{Key: "type", Value: 1}, {Key: "objectId", Value: 1}},
		Options: options.Index().
			SetUnique(true).
			SetPartialFilterExpression(bson.M{
				"$or": bson.A{
					bson.M{"status": store.StatusPending},
					bson.M{"status": store.StatusInProgress},
				},
			}).
			SetName("uk_workitem_active"),
	})
	return err
}
