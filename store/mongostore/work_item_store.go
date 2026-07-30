package mongostore

import (
	"context"
	"errors"
	"sync"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	mongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
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

// workItemData implements store.IWorkItemStore using MongoDB.
type workItemData struct {
	Service     *mongolks.LinkedService
	idxWarnOnce sync.Once
}

func newWorkItemData(ms *mongolks.LinkedService) *workItemData {
	return &workItemData{Service: ms}
}

// warnIfActiveIndexMissing logga (una sola volta) un warning se l'indice partiale unico
// uk_workitem_active non esiste sulla collection work_items. Senza quell'indice
// InsertIfNotActive NON deduplica (nessun duplicate-key da intercettare) → rischio di
// work item duplicati e doppia esecuzione. L'indice NON viene creato in automatico
// (gestione manuale via EnsureIndexes o migration/ops): il warning serve a rendere
// l'eventuale assenza una scelta consapevole, non una svista.
func (d *workItemData) warnIfActiveIndexMissing(ctx context.Context) {
	d.idxWarnOnce.Do(func() {
		coll := d.Service.GetCollection(store.CollectionWorkItems, "")
		cur, err := coll.Indexes().List(ctx)
		if err != nil {
			log.Warn().Err(err).Str("collection", store.CollectionWorkItems).
				Msg("go-core-batch: impossibile verificare l'indice uk_workitem_active")
			return
		}
		defer cur.Close(ctx)
		var idx []bson.M
		if err := cur.All(ctx, &idx); err != nil {
			log.Warn().Err(err).Str("collection", store.CollectionWorkItems).
				Msg("go-core-batch: impossibile leggere gli indici di work_items")
			return
		}
		for _, ix := range idx {
			if name, _ := ix["name"].(string); name == "uk_workitem_active" {
				return
			}
		}
		log.Warn().Str("collection", store.CollectionWorkItems).
			Msg("go-core-batch: indice partiale unico 'uk_workitem_active' ASSENTE — InsertIfNotActive NON deduplica (rischio work item duplicati / doppia esecuzione). Crearlo via mongostore.EnsureIndexes o migration, oppure confermare che l'assenza è voluta.")
	})
}

var _ store.IWorkItemStore = (*workItemData)(nil)

func (d *workItemData) FindPending(ctx context.Context, workType, destination, objectType string) ([]*store.WorkItem, *core.ApplicationError) {
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
func (d *workItemData) ClaimPending(ctx context.Context, workType, destination, objectType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
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

	token := store.NewLockToken()
	host := store.Hostname()
	var claimed []*store.WorkItem
	for _, item := range candidates {
		claimFilter := workItemFilter{Id: item.Id, Status: store.StatusPending}
		update := bson.M{"$set": bson.M{
			"status":     store.StatusInProgress,
			"lockedAt":   now,
			"updateTime": now,
			"lockToken":  token,
			"lockedBy":   host,
		}}
		if err := mongo.UpdateOne(ctx, d.Service, claimFilter, update); err != nil {
			if err.Code == "MON-AGGINC" {
				continue // already claimed by another replica — skip
			}
			return claimed, err
		}
		item.Status = store.StatusInProgress
		item.LockedAt = &now
		item.LockToken = token
		item.LockedBy = host
		claimed = append(claimed, item)
	}
	return claimed, nil
}

// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
// refreshing lockedAt to now and incrementing retry. Returns the items for
// immediate processing — no reset to PENDING, no waiting for the next tick.
// Uses optimistic per-item UpdateOne so concurrent replicas don't double-claim.
func (d *workItemData) RecoverOrphans(ctx context.Context, workType, destination, objectType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
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

	token := store.NewLockToken()
	host := store.Hostname()
	var claimed []*store.WorkItem
	for _, item := range candidates {
		res, err := coll.UpdateOne(ctx,
			bson.M{"_id": item.Id, "status": store.StatusInProgress, "lockedAt": bson.M{"$lt": cutoff}},
			bson.M{
				"$set": bson.M{"lockedAt": now, "updateTime": now, "lockToken": token, "lockedBy": host},
				"$inc": bson.M{"retry": 1},
			},
		)
		if err != nil {
			return claimed, core.TechnicalErrorWithError(err)
		}
		if res.ModifiedCount == 1 {
			item.LockedAt = &now
			item.LockToken = token
			item.LockedBy = host
			item.Retry++
			claimed = append(claimed, item)
		}
	}
	return claimed, nil
}

// fencedFilter è il filtro base dei Mark*: item ancora IN_PROGRESS E con il fencing token
// del claim corrente. Se il token non matcha (item ri-claimato da un'altra replica) l'update
// non tocca nulla → il worker stale non può finalizzare l'item.
func fencedFilter(id, token string) bson.M {
	return bson.M{"_id": id, "status": store.StatusInProgress, "lockToken": token}
}

// MarkDone transitions IN_PROGRESS items to DONE in batch, fenced dal token (gli id devono
// condividere lo stesso lock_token). Idempotente: gli id non matchati (già finalizzati o token
// stale) sono ignorati — non è un errore, è l'esito atteso quando un worker stale prova a
// finalizzare item ri-claimati altrove.
func (d *workItemData) MarkDone(ctx context.Context, ids []string, token string) *core.ApplicationError {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}, "status": store.StatusInProgress, "lockToken": token},
		bson.M{"$set": bson.M{"status": store.StatusDone, "updateTime": now, "lockedAt": nil}},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if int(res.ModifiedCount) != len(ids) {
		log.Debug().Msgf("MarkDone: %d/%d item marcati DONE (gli altri già finalizzati o token stale)",
			res.ModifiedCount, len(ids))
	}
	return nil
}

// MarkFailed transitions a single IN_PROGRESS item to FAILED, fenced dal token (idempotente).
func (d *workItemData) MarkFailed(ctx context.Context, id, token, reason string) *core.ApplicationError {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateOne(ctx, fencedFilter(id, token),
		bson.M{"$set": bson.M{"status": store.StatusFailed, "error": reason, "updateTime": now, "lockedAt": nil}},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if res.ModifiedCount == 0 {
		log.Debug().Msgf("MarkFailed: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// MarkPending resets a single IN_PROGRESS item back to PENDING for retry, fenced dal token (idempotente).
func (d *workItemData) MarkPending(ctx context.Context, id, token string, after time.Duration) *core.ApplicationError {
	now := time.Now()
	nextRunAt := now.Add(after)
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateOne(ctx, fencedFilter(id, token),
		bson.M{
			"$set": bson.M{"status": store.StatusPending, "lockedAt": nil, "updateTime": now, "nextRunAt": nextRunAt},
			"$inc": bson.M{"retry": 1},
		},
	)
	if err != nil {
		return core.TechnicalErrorWithError(err)
	}
	if res.ModifiedCount == 0 {
		log.Debug().Msgf("MarkPending: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

func (d *workItemData) Insert(ctx context.Context, items []*store.WorkItem) *core.ApplicationError {
	list := make([]mongo.ICollection, len(items))
	for i, item := range items {
		list[i] = item
	}
	return mongo.InsertMany(ctx, d.Service, list)
}

func (d *workItemData) DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.DeleteOne(ctx, bson.M{"_id": id, "status": store.StatusPending})
	if err != nil {
		return false, core.TechnicalErrorWithError(err)
	}
	return res.DeletedCount == 1, nil
}

func (d *workItemData) GetById(ctx context.Context, id string) (*store.WorkItem, *core.ApplicationError) {
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

func (d *workItemData) HasActive(ctx context.Context, workType, objectId string) (bool, *core.ApplicationError) {
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
func (d *workItemData) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	d.warnIfActiveIndexMissing(ctx)
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

func (d *workItemData) List(ctx context.Context, workType, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
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

// EnsureIndexes creates the indexes required by workItemData. Call once at application startup.
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
