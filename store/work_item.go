package store

import (
	"context"
	"time"
)

const (
	CollectionWorkItems = "workitems"
	TableWorkItems      = "workitems"
	StatusPending       = "PENDING"
	StatusInProgress    = "IN_PROGRESS"
	StatusDone          = "DONE"
	StatusFailed        = "FAILED"
)

// WorkItem is the generic unit-of-work document.
// Implements both mongo.ICollection and coresql.IRecord so either backend can persist it.
// Job-specific data goes in Payload; Destination is a routing hint (e.g. Kafka topic, worker type).
type WorkItem struct {
	Id          string     `bson:"_id"                       bun:"id,pk"`
	Type        string     `bson:"type"                      bun:"type"`
	ObjectId    string     `bson:"objectId"                  bun:"object_id"`
	ObjectType  string     `bson:"objectType"                bun:"object_type"`
	Destination string     `bson:"destination"               bun:"destination"`
	Payload     any        `bson:"payload"                   bun:"payload"`
	Status      string     `bson:"status"                    bun:"status"`
	CreateTime  time.Time  `bson:"createTime"                bun:"create_time"`
	UpdateTime  *time.Time `bson:"updateTime,omitempty"      bun:"update_time,nullzero"`
	LockedAt    *time.Time `bson:"lockedAt,omitempty"        bun:"locked_at,nullzero"`
	Retry       int        `bson:"retry"                     bun:"retry"`
	Error       string     `bson:"error,omitempty"           bun:"error,omitempty"`
}

func (w WorkItem) GetCollectionName(ctx context.Context) string { return CollectionWorkItems }
func (w WorkItem) GetTableName(ctx context.Context) string      { return TableWorkItems }
