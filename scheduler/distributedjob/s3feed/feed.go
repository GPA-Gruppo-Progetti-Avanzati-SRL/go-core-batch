package s3feed

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/google/uuid"
)

// S3Payload is stored in WorkItem.Payload for S3-sourced items.
// It carries the information needed by the fileRunner to download and move the file.
type S3Payload struct {
	Service  string `json:"service" bson:"service"`
	Key      string `json:"key" bson:"key"`
	DestPath string `json:"destPath" bson:"destPath"`
}

// S3Feed implements distributedjob.IFeedSource by listing objects in an S3 bucket.
type S3Feed struct {
	reg *s3.Registry
}

// New creates an S3Feed backed by the given registry.
func New(reg *s3.Registry) *S3Feed {
	return &S3Feed{reg: reg}
}

// Feed lists S3 objects matching the configured pattern and returns WorkItems.
//
// Required props:
//   - service:   logical S3 service name (key in s3.Config.Services)
//   - path:      prefix for S3 listing
//   - pattern:   glob pattern matched against the object key basename (e.g. "*.csv")
//   - dest-path: destination prefix where files are moved after successful processing
func (f *S3Feed) Feed(ctx context.Context, taskType string, props map[string]string, limit int) ([]*store.WorkItem, error) {
	serviceName := props["service"]
	prefix := props["path"]
	pattern := props["pattern"]
	destPath := props["dest-path"]

	svc, ok := f.reg.Get(serviceName)
	if !ok {
		return nil, fmt.Errorf("s3feed: unknown service %q", serviceName)
	}

	objects, err := svc.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("s3feed: list failed: %w", err)
	}

	var items []*store.WorkItem
	now := time.Now()
	for _, obj := range objects {
		if len(items) >= limit {
			break
		}
		base := path.Base(obj.Key)
		if pattern != "" {
			matched, matchErr := path.Match(pattern, base)
			if matchErr != nil {
				return nil, fmt.Errorf("s3feed: invalid pattern %q: %w", pattern, matchErr)
			}
			if !matched {
				continue
			}
		}
		items = append(items, &store.WorkItem{
			Id:       uuid.New().String(),
			Type:     taskType,
			ObjectId: obj.Key,
			Payload: S3Payload{
				Service:  serviceName,
				Key:      obj.Key,
				DestPath: destPath,
			},
			Status:     store.StatusPending,
			CreateTime: now,
			NextRunAt:  &now,
		})
	}
	return items, nil
}

// pathBase and pathMatch are thin wrappers exposed for testability.
var (
	pathBase  = path.Base
	pathMatch = path.Match
)
