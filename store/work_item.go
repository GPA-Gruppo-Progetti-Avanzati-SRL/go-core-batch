package store

import (
	"context"
	"time"
)

const (
	CollectionWorkItems = "work_items"
	TableWorkItems      = "work_items"
	StatusPending       = "PENDING"
	StatusInProgress    = "IN_PROGRESS"
	StatusDone          = "DONE"
	StatusFailed        = "FAILED"
)

// WorkItem is the generic unit-of-work document.
// Implements both mongo.ICollection and coresql.IRecord so either backend can persist it.
// Job-specific data goes in Payload; Destination is a routing hint (e.g. Kafka topic, worker type).
type WorkItem struct {
	Id string `bson:"_id"                       bun:"id,pk"`
	// TaskName è il NOME dell'istanza di task che deve eseguire l'item — la voce di `tasks:`
	// referenziata dal job (`properties.task`) o elencata da un worker pool (`workers[].tasks`).
	// Non è un "tipo": ci filtra il claiming (ClaimPending/RecoverOrphans) e ci instrada il
	// MuxRunner via TaskRunner.TaskName.
	TaskName    string     `bson:"taskName"                  bun:"task_name"`
	ObjectId    string     `bson:"objectId"                  bun:"object_id"`
	ObjectType  string     `bson:"objectType"                bun:"object_type"`
	Destination string     `bson:"destination"               bun:"destination"`
	Payload     any        `bson:"payload"                   bun:"payload,type:jsonb"`
	Status      string     `bson:"status"                    bun:"status"`
	CreateTime  time.Time  `bson:"createTime"                bun:"create_time"`
	UpdateTime  *time.Time `bson:"updateTime,omitempty"      bun:"update_time,nullzero"`
	LockedAt    *time.Time `bson:"lockedAt,omitempty"        bun:"locked_at,nullzero"`
	// LockToken è il fencing token: un valore unico rigenerato ad OGNI claim/recover.
	// I Mark* lo richiedono in WHERE, così un worker "stale" (il cui item è stato ri-claimato
	// da RecoverOrphans) non può più finalizzare l'item — il suo token non matcha più.
	LockToken string `bson:"lockToken,omitempty" bun:"lock_token,nullzero"`
	// LockedBy è l'hostname del worker/replica che ha in carico l'item — solo osservabilità,
	// NON partecipa al fencing.
	LockedBy  string     `bson:"lockedBy,omitempty"        bun:"locked_by,nullzero"`
	NextRunAt *time.Time `bson:"nextRunAt,omitempty"       bun:"next_run_at,nullzero"`
	Retry     int        `bson:"retry"                     bun:"retry"`
	Error     string     `bson:"error,omitempty"           bun:"error,nullzero"`
}

func (w WorkItem) GetCollectionName(ctx context.Context) string { return CollectionWorkItems }
func (w WorkItem) GetTableName(ctx context.Context) string      { return TableWorkItems }
