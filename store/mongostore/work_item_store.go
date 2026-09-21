package mongostore

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo/mongoutil"
	"slices"
	"sync"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	mongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	mgodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

type workItemFilter struct {
	Id          string   `field:"_id"         operator:"$eq"  omitempty:"true"`
	IdIn        []string `field:"_id"         operator:"$in"  omitempty:"true"`
	TaskName    string   `field:"taskName"    operator:"$eq"  omitempty:"true"`
	Status      string   `field:"status"      operator:"$eq"  omitempty:"true"`
	Destination string   `field:"destination" operator:"$eq"  omitempty:"true"`
	ObjectType  string   `field:"objectType"  operator:"$eq"  omitempty:"true"`
}

func (f workItemFilter) GetFilterCollectionName(ctx context.Context) string {
	return store.CollectionWorkItems
}

// workItemData implements store.IWorkItemStore using MongoDB.
type workItemData struct {
	Service     *mongo.Service
	idxWarnOnce sync.Once
}

func newWorkItemData(ms *mongo.Service) *workItemData {
	return &workItemData{Service: ms}
}

// indiciAttesi sono gli indici su cui gira il sottosistema. Non vengono creati in automatico
// (gestione manuale via EnsureIndexes o migration/ops): il warning serve a rendere l'eventuale
// assenza una scelta consapevole, non una svista.
//
//   - uk_workitem_active — unico parziale: senza, InsertIfNotActive NON deduplica (non c'è
//     duplicate-key da intercettare) e nascono work item doppi, con rischio di doppia esecuzione;
//   - ix_workitem_claim / ix_workitem_orphan / ix_workitem_claim_dest — servono le query di
//     ClaimPending e RecoverOrphans, che ogni job esegue a OGNI tick. Senza, il claim scandisce
//     la collection: un costo che cresce con lo storico invece che col lavoro da fare, e che
//     non si vede finché la collection è piccola.
var indiciAttesi = []string{
	"uk_workitem_active",
	"ix_workitem_claim",
	"ix_workitem_orphan",
	"ix_workitem_claim_dest",
}

// warnIfIndexesMissing logga (una sola volta) un warning per ogni indice atteso assente sulla
// collection work_items.
func (d *workItemData) warnIfIndexesMissing(ctx context.Context) {
	d.idxWarnOnce.Do(func() {
		coll := d.Service.GetCollection(store.CollectionWorkItems, "")
		cur, err := coll.Indexes().List(ctx)
		if err != nil {
			log.Warn().Err(err).Str("collection", store.CollectionWorkItems).
				Msg("go-core-batch: impossibile verificare gli indici di work_items")
			return
		}
		defer mongoutil.CloseCursor(ctx, cur, "warnIfIndexesMissing")
		var idx []bson.M
		if err := cur.All(ctx, &idx); err != nil {
			log.Warn().Err(err).Str("collection", store.CollectionWorkItems).
				Msg("go-core-batch: impossibile leggere gli indici di work_items")
			return
		}
		presenti := make(map[string]bool, len(idx))
		for _, ix := range idx {
			if name, _ := ix["name"].(string); name != "" {
				presenti[name] = true
			}
		}
		var mancanti []string
		for _, nome := range indiciAttesi {
			if !presenti[nome] {
				mancanti = append(mancanti, nome)
			}
		}
		if len(mancanti) == 0 {
			return
		}
		if slices.Contains(mancanti, "uk_workitem_active") {
			log.Warn().Str("collection", store.CollectionWorkItems).
				Msg("go-core-batch: indice partiale unico 'uk_workitem_active' ASSENTE — InsertIfNotActive NON deduplica (rischio work item duplicati / doppia esecuzione)")
		}
		log.Warn().Str("collection", store.CollectionWorkItems).Strs("indici", mancanti).
			Msg("go-core-batch: indici ASSENTI su work_items — il claim di ogni tick scandisce la collection. Crearli via mongostore.EnsureIndexes o migration, oppure confermare che l'assenza è voluta.")
	})
}

var _ store.IWorkItemStore = (*workItemData)(nil)

// ClaimPending claima atomicamente fino a limit item PENDING.
//
// Tre round-trip, non 1+N: (1) Find dei candidati, bounded da limit, ordinati per scadenza;
// (2) UNA BulkWrite di UpdateOne filtrati su status=PENDING — è il filtro dentro ogni update a
// rendere atomico il claim, e chi perde la corsa con un'altra replica semplicemente non matcha;
// (3) Find degli item che portano il token di QUESTO tick, che sono per costruzione esattamente
// quelli vinti. Prima era una Find più una UpdateOne per candidato: con limit 100, 101
// round-trip per tick per job, dove il backend SQL ne faceva due.
//
// La query dei candidati tratta un nextRunAt assente o null come "scaduto adesso" (specchio del
// `next_run_at IS NULL OR <= NOW()` del backend SQL).
func (d *workItemData) ClaimPending(ctx context.Context, taskName, destination, objectType string, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	// La verifica sta anche qui, e non solo su InsertIfNotActive: gli indici del claim servono a
	// OGNI job, compresi quelli claim-only (DistribuiteTask, NotificationKafka) che un feed non
	// ce l'hanno e quindi non passerebbero mai di là. È sync.Once: una sola lettura degli indici
	// per processo, qualunque sia la strada che ci arriva per prima.
	d.warnIfIndexesMissing(ctx)
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")

	query := bson.M{
		"taskName": taskName,
		"status":   store.StatusPending,
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
			SetLimit(int64(limit)).
			SetProjection(bson.M{"_id": 1}),
	)
	if err != nil {
		return nil, errs.Tech(errs.CodeClaim).WithCause(err)
	}
	defer mongoutil.CloseCursor(ctx, cursor, "ClaimPending")

	var candidates []struct {
		Id string `bson:"_id"`
	}
	if err := cursor.All(ctx, &candidates); err != nil {
		return nil, errs.Tech(errs.CodeClaim).WithCause(err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	ids := make([]string, len(candidates))
	models := make([]mgodriver.WriteModel, len(candidates))
	token := store.NewLockToken()
	host := store.Hostname()
	set := bson.M{"$set": bson.M{
		"status":     store.StatusInProgress,
		"lockedAt":   now,
		"updateTime": now,
		"lockToken":  token,
		"lockedBy":   host,
	}}
	for i, c := range candidates {
		ids[i] = c.Id
		models[i] = mgodriver.NewUpdateOneModel().
			SetFilter(bson.M{"_id": c.Id, "status": store.StatusPending}).
			SetUpdate(set)
	}
	if _, appErr := d.Service.BulkWrite[store.WorkItem](ctx, models, mongo.BulkUnordered()); appErr != nil {
		return nil, appErr
	}
	return d.byToken(ctx, errs.CodeClaim, ids, token)
}

// byToken rilegge gli item che portano il token di questo tick: sono esattamente quelli che la
// BulkWrite ha vinto. Il filtro su _id tiene la query sull'indice primario.
func (d *workItemData) byToken(ctx context.Context, code string, ids []string, token string) ([]*store.WorkItem, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	cur, err := coll.Find(ctx, bson.M{"_id": bson.M{"$in": ids}, "lockToken": token})
	if err != nil {
		return nil, errs.Tech(code).WithCause(err)
	}
	defer mongoutil.CloseCursor(ctx, cur, "byToken")
	var claimed []*store.WorkItem
	if err := cur.All(ctx, &claimed); err != nil {
		return nil, errs.Tech(code).WithCause(err)
	}
	return claimed, nil
}

// RecoverOrphans ri-claima gli item IN_PROGRESS più vecchi di maxAge rinfrescando lockedAt e
// incrementando retry, e li ritorna per la lavorazione immediata nel tick corrente — nessun
// ritorno a PENDING, nessuna attesa del tick successivo.
//
// Stessa forma di ClaimPending: tre round-trip invece di 1+N. Il filtro `lockedAt < cutoff`
// resta dentro ogni UpdateOne, quindi due repliche non recuperano lo stesso orfano.
func (d *workItemData) RecoverOrphans(ctx context.Context, taskName, destination, objectType string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.ApplicationError) {
	now := time.Now()
	cutoff := now.Add(-maxAge)
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")

	query := bson.M{"taskName": taskName, "status": store.StatusInProgress, "lockedAt": bson.M{"$lt": cutoff}}
	if destination != "" {
		query["destination"] = destination
	}
	if objectType != "" {
		query["objectType"] = objectType
	}
	cursor, err := coll.Find(ctx, query,
		options.Find().
			SetSort(bson.D{{Key: "lockedAt", Value: 1}}).
			SetLimit(int64(limit)).
			SetProjection(bson.M{"_id": 1}),
	)
	if err != nil {
		return nil, errs.Tech(errs.CodeRecover).WithCause(err)
	}
	defer mongoutil.CloseCursor(ctx, cursor, "RecoverOrphans")

	var candidates []struct {
		Id string `bson:"_id"`
	}
	if err := cursor.All(ctx, &candidates); err != nil {
		return nil, errs.Tech(errs.CodeRecover).WithCause(err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	ids := make([]string, len(candidates))
	models := make([]mgodriver.WriteModel, len(candidates))
	token := store.NewLockToken()
	host := store.Hostname()
	update := bson.M{
		"$set": bson.M{"lockedAt": now, "updateTime": now, "lockToken": token, "lockedBy": host},
		"$inc": bson.M{"retry": 1},
	}
	for i, c := range candidates {
		ids[i] = c.Id
		models[i] = mgodriver.NewUpdateOneModel().
			SetFilter(bson.M{"_id": c.Id, "status": store.StatusInProgress, "lockedAt": bson.M{"$lt": cutoff}}).
			SetUpdate(update)
	}
	if _, appErr := d.Service.BulkWrite[store.WorkItem](ctx, models, mongo.BulkUnordered()); appErr != nil {
		return nil, appErr
	}
	return d.byToken(ctx, errs.CodeRecover, ids, token)
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
		return errs.Tech(errs.CodeMarkDone).WithCause(err)
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
		return errs.Tech(errs.CodeMarkFailed).WithCause(err)
	}
	if res.ModifiedCount == 0 {
		log.Debug().Msgf("MarkFailed: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// Release riporta a PENDING un item claimato ma mai eseguito (dispatch fallito), fenced dal
// token e idempotente. È MarkPending meno l'$inc su retry: il tentativo non è avvenuto, quindi
// non va contato. nextRunAt = now, così il tick successivo lo riprende subito.
func (d *workItemData) Release(ctx context.Context, id, token string) *core.ApplicationError {
	now := time.Now()
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.UpdateOne(ctx, fencedFilter(id, token),
		bson.M{"$set": bson.M{
			"status": store.StatusPending, "lockedAt": nil, "updateTime": now, "nextRunAt": now,
		}},
	)
	if err != nil {
		return errs.Tech(errs.CodeRelease).WithCause(err)
	}
	if res.ModifiedCount == 0 {
		log.Debug().Msgf("Release: item %q non aggiornato (già finalizzato o token stale)", id)
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
		return errs.Tech(errs.CodeMarkPending).WithCause(err)
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
	return d.Service.InsertMany(ctx, list)
}

func (d *workItemData) DeleteIfPending(ctx context.Context, id string) (bool, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	res, err := coll.DeleteOne(ctx, bson.M{"_id": id, "status": store.StatusPending})
	if err != nil {
		return false, errs.Tech(errs.CodeDelete).WithCause(err)
	}
	return res.DeletedCount == 1, nil
}

func (d *workItemData) GetById(ctx context.Context, id string) (*store.WorkItem, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	var item store.WorkItem
	if err := coll.FindOne(ctx, bson.M{"_id": id}).Decode(&item); err != nil {
		if errors.Is(err, mgodriver.ErrNoDocuments) {
			return nil, errs.NotFound().WithCause(err)
		}
		return nil, errs.Tech(errs.CodeGet).WithCause(err)
	}
	return &item, nil
}

func (d *workItemData) HasActive(ctx context.Context, taskName, objectId string) (bool, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	count, err := coll.CountDocuments(ctx, bson.M{
		"taskName": taskName,
		"objectId": objectId,
		"status":   bson.M{"$in": []string{store.StatusPending, store.StatusInProgress}},
	})
	if err != nil {
		return false, errs.Tech(errs.CodeHasActive).WithCause(err)
	}
	return count > 0, nil
}

// InsertIfNotActive inserts each item only if no active (PENDING or IN_PROGRESS) entry
// exists for the same (taskName, objectId). Relies on the partial unique index
// uk_workitem_active — call EnsureIndexes at startup to create it.
//
// Una sola InsertMany non ordinata invece di N InsertOne: il driver prova comunque tutti gli
// item e ritorna gli errori dei soli duplicati, che è esattamente il comportamento che serve.
// Con un feed by-query da limit 100 erano 100 round-trip per tick, dove il backend SQL faceva
// una sola INSERT ... ON CONFLICT DO NOTHING.
func (d *workItemData) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.ApplicationError) {
	if len(items) == 0 {
		return 0, nil
	}
	d.warnIfIndexesMissing(ctx)
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	docs := make([]any, len(items))
	for i, item := range items {
		docs[i] = item
	}
	res, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err == nil {
		return len(res.InsertedIDs), nil
	}
	// Con SetOrdered(false) il driver tenta ogni documento e raccoglie gli errori: i duplicate
	// key (11000) sono l'esito ATTESO della deduplica, qualunque altro codice è un guasto vero e
	// va propagato. res è comunque valorizzato con ciò che è passato.
	var bwe mgodriver.BulkWriteException
	if errors.As(err, &bwe) {
		for _, we := range bwe.WriteErrors {
			if we.Code != duplicateKeyCode {
				return inserted(res), errs.Tech(errs.CodeInsert).WithCause(err)
			}
		}
		return inserted(res), nil
	}
	if mgodriver.IsDuplicateKeyError(err) {
		return inserted(res), nil
	}
	return inserted(res), errs.Tech(errs.CodeInsert).WithCause(err)
}

// duplicateKeyCode è il codice MongoDB dell'unique constraint violata: è l'esito atteso di
// InsertIfNotActive, non un errore.
const duplicateKeyCode = 11000

func inserted(res *mgodriver.InsertManyResult) int {
	if res == nil {
		return 0
	}
	return len(res.InsertedIDs)
}

// Purge cancella gli item nello stato indicato più vecchi di olderThan, al più limit per
// chiamata. Il limit tiene corta la singola cancellazione: la retention è ripetuta a ogni tick
// del job, non fatta tutta in una volta.
func (d *workItemData) Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	// Mongo non ha un LIMIT sulla delete: si selezionano prima gli id (bounded) e si cancellano
	// quelli. Due round-trip, sull'indice.
	cur, err := coll.Find(ctx,
		bson.M{"status": status, "updateTime": bson.M{"$lt": olderThan}},
		options.Find().SetSort(bson.D{{Key: "updateTime", Value: 1}}).SetLimit(int64(limit)).SetProjection(bson.M{"_id": 1}),
	)
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	defer mongoutil.CloseCursor(ctx, cur, "Purge")
	var vittime []struct {
		Id string `bson:"_id"`
	}
	if err := cur.All(ctx, &vittime); err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	if len(vittime) == 0 {
		return 0, nil
	}
	ids := make([]string, len(vittime))
	for i, v := range vittime {
		ids[i] = v.Id
	}
	res, err := coll.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}, "status": status})
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	return int(res.DeletedCount), nil
}

// Backlog conta i PENDING in attesa e ritorna la data di creazione del più vecchio.
func (d *workItemData) Backlog(ctx context.Context, taskName, destination, objectType string) (int, time.Time, *core.ApplicationError) {
	coll := d.Service.GetCollection(store.CollectionWorkItems, "")
	query := bson.M{"taskName": taskName, "status": store.StatusPending}
	if destination != "" {
		query["destination"] = destination
	}
	if objectType != "" {
		query["objectType"] = objectType
	}
	count, err := coll.CountDocuments(ctx, query)
	if err != nil {
		return 0, time.Time{}, errs.Tech(errs.CodeBacklog).WithCause(err)
	}
	if count == 0 {
		return 0, time.Time{}, nil
	}
	var oldest store.WorkItem
	if err := coll.FindOne(ctx, query,
		options.FindOne().SetSort(bson.D{{Key: "createTime", Value: 1}}),
	).Decode(&oldest); err != nil {
		if errors.Is(err, mgodriver.ErrNoDocuments) {
			return int(count), time.Time{}, nil
		}
		return int(count), time.Time{}, errs.Tech(errs.CodeBacklog).WithCause(err)
	}
	return int(count), oldest.CreateTime, nil
}

func (d *workItemData) List(ctx context.Context, taskName, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.ApplicationError) {
	filter := workItemFilter{TaskName: taskName, Status: status}
	var sortOpt options.Lister[options.FindOptions]
	if len(sort) > 0 {
		sortOpt = mongo.FindSortOption(sort)
	} else {
		sortOpt = options.Find().SetSort(bson.D{{Key: "createTime", Value: -1}})
	}
	flat, err := d.Service.GetPageByFilter[store.WorkItem](ctx, filter, paging, sortOpt)
	if err != nil {
		return nil, err
	}
	items := make([]*store.WorkItem, len(flat))
	for i := range flat {
		items[i] = &flat[i]
	}
	return items, nil
}

// EnsureIndexes crea gli indici richiesti da workItemData. Chiamarla una volta all'avvio.
//
//   - uk_workitem_active: unico parziale su (taskName, objectId) per gli stati attivi, impedisce
//     l'inserimento concorrente di item attivi duplicati per lo stesso oggetto;
//   - ix_workitem_claim: serve la query di ClaimPending (filtro + ordinamento), che ogni job
//     esegue a ogni tick;
//   - ix_workitem_orphan: serve la query di RecoverOrphans;
//   - ix_workitem_claim_dest: serve il claim filtrato per destinazione (job NotificationKafka).
//
// I tre indici del claim sono parziali sugli stati attivi: gli item DONE/FAILED non vengono mai
// claimati, quindi tenerli fuori mantiene l'indice della dimensione del LAVORO e non dello storico.
func EnsureIndexes(ctx context.Context, service *mongo.Service) error {
	coll := service.GetCollection(store.CollectionWorkItems, "")
	attivi := bson.M{"status": bson.M{"$in": bson.A{store.StatusPending, store.StatusInProgress}}}
	_, err := coll.Indexes().CreateMany(ctx, []mgodriver.IndexModel{
		{
			Keys: bson.D{{Key: "taskName", Value: 1}, {Key: "objectId", Value: 1}},
			Options: options.Index().
				SetUnique(true).
				SetPartialFilterExpression(bson.M{
					"$or": bson.A{
						bson.M{"status": store.StatusPending},
						bson.M{"status": store.StatusInProgress},
					},
				}).
				SetName("uk_workitem_active"),
		},
		{
			Keys: bson.D{
				{Key: "taskName", Value: 1}, {Key: "status", Value: 1},
				{Key: "nextRunAt", Value: 1}, {Key: "createTime", Value: 1},
			},
			Options: options.Index().SetPartialFilterExpression(attivi).SetName("ix_workitem_claim"),
		},
		{
			Keys: bson.D{
				{Key: "taskName", Value: 1}, {Key: "status", Value: 1}, {Key: "lockedAt", Value: 1},
			},
			Options: options.Index().SetPartialFilterExpression(attivi).SetName("ix_workitem_orphan"),
		},
		{
			Keys: bson.D{
				{Key: "taskName", Value: 1}, {Key: "status", Value: 1},
				{Key: "destination", Value: 1}, {Key: "objectType", Value: 1},
				{Key: "nextRunAt", Value: 1},
			},
			Options: options.Index().SetPartialFilterExpression(attivi).SetName("ix_workitem_claim_dest"),
		},
	})
	return err
}
