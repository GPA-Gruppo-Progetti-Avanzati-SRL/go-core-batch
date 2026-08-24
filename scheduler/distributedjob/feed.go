package distributedjob

import (
	"context"
	"time"
	"uuid"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/rs/zerolog/log"
)

// IFeedSource produces WorkItems from an external source (DB query, S3 listing, etc.).
// Implementations must return ready-to-insert WorkItems with at least ObjectId and Type set.
type IFeedSource interface {
	Feed(ctx context.Context, taskType string, props core.Properties, limit int) ([]*store.WorkItem, error)
}

// queryStoreFeed adapts IQueryStore to IFeedSource.
type queryStoreFeed struct {
	qs IQueryStore
}

// NewQueryFeed wraps an IQueryStore as an IFeedSource.
func NewQueryFeed(qs IQueryStore) IFeedSource {
	return &queryStoreFeed{qs: qs}
}

func (f *queryStoreFeed) Feed(ctx context.Context, taskType string, props core.Properties, limit int) ([]*store.WorkItem, error) {
	collection := props.GetString("collection", "")
	filter := props.GetString("filter", "")
	sort := props.GetString("sort", "")

	var ids []string
	var feedErr *core.ApplicationError
	if sort != "" {
		ids, feedErr = f.qs.GetIdsSorted(ctx, collection, filter, sort, limit)
	} else {
		ids, feedErr = f.qs.GetIds(ctx, collection, filter, limit)
	}
	if feedErr != nil {
		return nil, feedErr
	}

	now := time.Now()
	workItems := make([]*store.WorkItem, len(ids))
	for i, id := range ids {
		workItems[i] = &store.WorkItem{
			Id:         uuid.NewV7().String(),
			Type:       taskType,
			ObjectId:   id,
			ObjectType: props.GetString("objectType", ""),
			Status:     store.StatusPending,
			CreateTime: now,
			NextRunAt:  &now,
		}
	}
	return workItems, nil
}

// runFeedPhase executes the feed phase: generates WorkItems from the feed source
// and inserts those not already active into the store.
func runFeedPhase(ctx context.Context, feed IFeedSource, items store.IWorkItemStore, jobId, taskType string, props core.Properties, limit int) {
	workItems, err := feed.Feed(ctx, taskType, props, limit)
	if err != nil {
		log.Warn().Err(err).Msgf("[%s] feed query failed", jobId)
		return
	}
	if len(workItems) == 0 {
		return
	}
	if n, insertErr := items.InsertIfNotActive(ctx, workItems); insertErr != nil {
		log.Warn().Err(insertErr).Msgf("[%s] feed insert failed", jobId)
	} else if n > 0 {
		log.Info().Msgf("[%s] fed %d new workitem(s) from external source", jobId, n)
	}
}
