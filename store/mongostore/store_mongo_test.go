package mongostore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	coremongo "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-mongo"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store/storetest"
)

// La suite gira solo contro un MongoDB vero: MONGO_URL, es. mongodb://localhost:27017/?directConnection=true
// (la stessa variabile della conformance di go-core-locker). Senza, i test sono saltati.
var mongoService = sync.OnceValues(func() (*coremongo.Service, error) {
	var svc *coremongo.Service
	taskLogs := store.TaskLog{}.GetCollectionName(context.Background())
	coremongo.Module(&coremongo.Config{
		Name:   "storetest",
		Host:   os.Getenv("MONGO_URL"),
		DbName: fmt.Sprintf("batch_storetest_%d", time.Now().UnixNano()),
		Collections: mongolks.CollectionsCfg{
			{Id: store.CollectionWorkItems, Name: store.CollectionWorkItems},
			{Id: taskLogs, Name: taskLogs},
		},
		ServerSelectionTimeout: 3 * time.Second,
	})
	core.Populate(&svc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := core.Start(ctx); err != nil {
		return nil, err
	}
	return svc, nil
})

func newMongoStore(t *testing.T) store.IWorkItemStore {
	t.Helper()
	if os.Getenv("MONGO_URL") == "" {
		t.Skip("MONGO_URL non impostata: la suite dello store Mongo richiede un MongoDB")
	}
	svc, err := mongoService()
	if err != nil {
		t.Fatalf("MongoDB: %v", err)
	}
	ctx := context.Background()
	if err := svc.GetCollection(store.CollectionWorkItems, "").Drop(ctx); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if err := EnsureIndexes(ctx, svc); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return newWorkItemData(svc)
}

func TestStore_Mongo(t *testing.T) {
	storetest.Run(t, newMongoStore)
	if svc, err := mongoService(); err == nil && svc != nil {
		// Il database è di questo processo di test: su un fallimento resta, per guardarci dentro.
		if !t.Failed() {
			if err := svc.Db().Drop(context.Background()); err != nil {
				t.Logf("drop del database di test: %v", err)
			}
		}
	}
}
