package s3feed

import (
	"context"
	"fmt"
	"path"
	"time"
	"uuid"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// s3Payload is stored in WorkItem.Payload for S3-sourced items.
// It carries the information needed by the fileRunner to download and move the file.
type s3Payload struct {
	Service  string `json:"service" bson:"service"`
	Key      string `json:"key" bson:"key"`
	DestPath string `json:"destPath" bson:"destPath"`
}

// Gate decide, a ogni tick, se il feed deve elencare i file del bucket. È facoltativo: senza un Gate
// nel grafo fx il feed elenca sempre. Con un Gate che risponde false il tick non accoda nulla, ma i
// work item già accodati continuano a essere reclamati ed eseguiti: il gate governa solo l'ingresso.
// Serve a far partire l'elaborazione dei file solo quando un sistema esterno lo chiede.
type Gate interface {
	Open(ctx context.Context, taskName string) (bool, error)
	// Listed è chiamato dopo ogni elenco del bucket andato a buon fine, con il numero di file che
	// corrispondono al pattern (al più limit). Zero significa che non resta nulla da elaborare: è il
	// segnale con cui chi apre il cancello lo richiude. Un errore diventa l'esito del tick.
	Listed(ctx context.Context, taskName string, files int) error
}

// s3Feed implements distributedjob.IFeedSource by listing objects in an S3 bucket.
type s3Feed struct {
	reg  *s3client.Registry
	gate Gate
}

// newFeed creates an s3Feed backed by the given registry. gate può essere nil.
func newFeed(reg *s3client.Registry, gate Gate) *s3Feed {
	return &s3Feed{reg: reg, gate: gate}
}

// Feed lists S3 objects matching the configured pattern and returns WorkItems.
//
// Required props:
//   - service:   logical S3 service name (key in s3.Config.Services)
//   - path:      prefix for S3 listing
//   - pattern:   glob pattern matched against the object key basename (e.g. "*.csv")
//   - dest-path: destination prefix where files are moved after successful processing
func (f *s3Feed) Feed(ctx context.Context, taskName string, props properties.Properties, limit int) ([]*store.WorkItem, error) {
	serviceName := props.GetString("service", "")
	prefix := props.GetString("path", "")
	pattern := props.GetString("pattern", "")
	destPath := props.GetString("dest-path", "")

	if f.gate != nil {
		open, err := f.gate.Open(ctx, taskName)
		if err != nil {
			return nil, fmt.Errorf("s3feed: gate: %w", err)
		}
		if !open {
			return nil, nil
		}
	}

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
			Id:       uuid.NewV7().String(),
			TaskName: taskName,
			ObjectId: obj.Key,
			Payload: s3Payload{
				Service:  serviceName,
				Key:      obj.Key,
				DestPath: destPath,
			},
			Status:     store.StatusPending,
			CreateTime: now,
			NextRunAt:  &now,
		})
	}
	if f.gate != nil {
		if err := f.gate.Listed(ctx, taskName, len(items)); err != nil {
			return nil, fmt.Errorf("s3feed: gate: %w", err)
		}
	}
	return items, nil
}

// pathBase and pathMatch are thin wrappers exposed for testability.
var (
	pathBase  = path.Base
	pathMatch = path.Match
)
