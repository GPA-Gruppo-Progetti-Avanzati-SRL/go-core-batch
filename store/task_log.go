package store

import (
	"context"
	"time"
)

const TableTaskLogs = "task_logs"

// TaskLog records a single lifecycle event for a distributed task.
// Implements both mongo.ICollection (GetCollectionName) and coresql.IRecord (GetTableName).
type TaskLog struct {
	TaskID   string    `json:"taskId"            bson:"taskId"            bun:"task_id"`
	JobID    string    `json:"jobId"             bson:"jobId"             bun:"job_id"`
	Type     string    `json:"type"              bson:"type"              bun:"type"`
	Stato    string    `json:"stato"             bson:"stato"             bun:"stato"`
	Hostname string    `json:"hostname"          bson:"hostname"          bun:"hostname"`
	Logdate  time.Time `json:"logdate"           bson:"logdate"           bun:"logdate"`
	Objectid string    `json:"oggetto,omitempty" bson:"oggetto,omitempty" bun:"oggetto,nullzero"`
	Error    string    `json:"errore,omitempty"  bson:"errore,omitempty"  bun:"errore,nullzero"`
}

func (t TaskLog) GetCollectionName(ctx context.Context) string { return "tasks" }
func (t TaskLog) GetTableName(ctx context.Context) string      { return TableTaskLogs }
