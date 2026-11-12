package s3feed

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"
	"uuid"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	pub "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// s3Payload is stored in WorkItem.Payload for S3-sourced items.
// It carries the information needed by the fileRunner to download and move the file.
type s3Payload struct {
	Service  string `json:"service" bson:"service"`
	Key      string `json:"key" bson:"key"`
	DestPath string `json:"destPath" bson:"destPath"`
}

// sourceFrom legge la sorgente dalle property di un job DistribuiteTaskByS3File, l'unico job che la
// porta ancora sul job: gli altri flussi la dichiarano sul task (task.Config.S3).
func sourceFrom(p properties.Properties) s3.Source {
	return s3.Source{
		Service:  p.GetString(pub.PropService, ""),
		Path:     p.GetString(pub.PropPath, ""),
		Pattern:  p.GetString(pub.PropPattern, ""),
		DestPath: p.GetString(pub.PropDestPath, ""),
	}
}

// scan elenca il prefisso della sorgente e costruisce un work item per ogni file il cui basename
// corrisponde al pattern, al più limit. È il feed S3, l'unico: lo usano il Feeder (che serve sia il job
// FeedS3Task sia il trigger su richiesta) e il feed del job DistribuiteTaskByS3File.
//
// L'elenco si ferma appena trovati limit file: prima si elencava l'intero prefisso e si tagliava
// dopo. Le chiavi che finiscono con "/" sono i segnaposto di cartella che le console S3 creano e non
// sono file: con un pattern "*" venivano accodate ed elaborate come file vuoti.
//
// Gli item escono nell'ordine dell'elenco (lessicografico sulla chiave) e lo conservano in coda:
// il claim ordina per next_run_at e poi per create_time, quindi i due istanti sono scaglionati di
// un millisecondo per file — la risoluzione di una data Mongo —, altrimenti a pari istante l'ordine
// in cui un SingleTask li prende sarebbe quello del database.
func scan(ctx context.Context, svcs services, taskName string, src s3.Source, limit int) ([]*store.WorkItem, error) {
	svc, ok := svcs.lookup(src.Service)
	if !ok {
		return nil, fmt.Errorf("s3feed: unknown service %q", src.Service)
	}
	if err := checkPattern(src.Pattern); err != nil {
		return nil, err
	}

	var keys []string
	err := svc.Walk(ctx, src.Path, func(obj s3client.Object) bool {
		if strings.HasSuffix(obj.Key, "/") {
			return true
		}
		if src.Pattern != "" {
			if matched, _ := path.Match(src.Pattern, path.Base(obj.Key)); !matched {
				return true
			}
		}
		keys = append(keys, obj.Key)
		return len(keys) < limit
	})
	if err != nil {
		return nil, errs.Tech(errs.CodeS3List).WithCause(err)
	}

	now := time.Now()
	items := make([]*store.WorkItem, len(keys))
	for i, key := range keys {
		at := now.Add(time.Duration(i) * time.Millisecond)
		items[i] = &store.WorkItem{
			Id:         uuid.NewV7().String(),
			TaskName:   taskName,
			ObjectId:   key,
			Payload:    s3Payload{Service: src.Service, Key: key, DestPath: src.DestPath},
			Status:     store.StatusPending,
			CreateTime: at,
			NextRunAt:  &at,
		}
	}
	return items, nil
}

// checkPattern rifiuta un glob malformato prima di elencare: dentro la visita l'errore di path.Match
// non ha dove risalire, e un pattern rotto non accoderebbe mai nulla senza dirlo.
func checkPattern(pattern string) error {
	if pattern == "" {
		return nil
	}
	if _, err := path.Match(pattern, ""); err != nil {
		return fmt.Errorf("s3feed: invalid pattern %q: %w", pattern, err)
	}
	return nil
}

// s3Feed implements distributedjob.IFeedSource by listing objects in an S3 bucket.
type s3Feed struct {
	svcs services
}

// newFeed creates an s3Feed backed by the given registry.
func newFeed(reg *s3client.Registry) *s3Feed {
	return &s3Feed{svcs: registryServices{reg}}
}

// Feed lists S3 objects matching the configured pattern and returns WorkItems.
//
// Required props:
//   - service:   logical S3 service name (key in s3.Config.Services)
//   - path:      prefix for S3 listing
//   - pattern:   glob pattern matched against the object key basename (e.g. "*.csv")
//   - dest-path: destination prefix where files are moved after successful processing
func (f *s3Feed) Feed(ctx context.Context, taskName string, props properties.Properties, limit int) ([]*store.WorkItem, error) {
	return scan(ctx, f.svcs, taskName, sourceFrom(props), limit)
}
