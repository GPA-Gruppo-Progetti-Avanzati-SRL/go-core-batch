// Package mongostore provides a MongoDB-backed implementation of distributedjob.IQueryStore.
// Import this package when the external feed source is a MongoDB collection.
package mongostore

import (
	"context"
	"encoding/json"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// QueryData implements distributedjob.IQueryStore against a MongoDB collection.
type QueryData struct {
	Service *mongolks.LinkedService
}

func NewQueryData(ms *mongolks.LinkedService) *QueryData {
	return &QueryData{Service: ms}
}

var _ distributedjob.IQueryStore = (*QueryData)(nil)

func (q *QueryData) GetIds(ctx context.Context, collection, filter string, limit int) ([]string, *core.ApplicationError) {
	return q.GetIdsSorted(ctx, collection, filter, "", limit)
}

func (q *QueryData) GetIdsSorted(ctx context.Context, collection, filter, sort string, limit int) ([]string, *core.ApplicationError) {
	coll := q.Service.GetCollection(collection, "")

	var query bson.M
	if filter != "" {
		if err := json.Unmarshal([]byte(filter), &query); err != nil {
			return nil, core.TechnicalErrorWithError(err)
		}
	} else {
		query = bson.M{}
	}

	opts := options.Find().SetProjection(bson.M{"_id": 1})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}
	if sort != "" {
		sortDoc := bson.D{}
		for _, part := range strings.Split(sort, ",") {
			fields := strings.SplitN(strings.TrimSpace(part), ":", 2)
			col := fields[0]
			dir := 1
			if len(fields) == 2 && strings.ToLower(fields[1]) == "desc" {
				dir = -1
			}
			sortDoc = append(sortDoc, bson.E{Key: col, Value: dir})
		}
		opts.SetSort(sortDoc)
	}

	cursor, err := coll.Find(ctx, query, opts)
	if err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	defer cursor.Close(ctx)

	var ids []string
	for cursor.Next(ctx) {
		var doc struct {
			Id string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err == nil {
			ids = append(ids, doc.Id)
		}
	}
	return ids, nil
}
