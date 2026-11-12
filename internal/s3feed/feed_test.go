package s3feed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/s3client"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/taskrunner"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// bucketFake è un objectStore in memoria: chiavi nell'ordine di S3, contenuti, spostamenti.
type bucketFake struct {
	keys    []string
	content map[string]string
	visited int   // oggetti consegnati a Walk: dice se l'elenco si è fermato in anticipo
	listErr error // errore di Walk
	moveErr error
	moved   map[string]string
}

func (b *bucketFake) Walk(_ context.Context, prefix string, visit func(s3client.Object) bool) error {
	if b.listErr != nil {
		return b.listErr
	}
	for _, k := range b.keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		b.visited++
		if !visit(s3client.Object{Key: k}) {
			return nil
		}
	}
	return nil
}

func (b *bucketFake) Get(_ context.Context, key string) (io.ReadCloser, error) {
	c, ok := b.content[key]
	if !ok {
		return nil, fmt.Errorf("get %q: %w", key, s3client.ErrObjectNotFound)
	}
	return io.NopCloser(strings.NewReader(c)), nil
}

func (b *bucketFake) Move(_ context.Context, src, dest string) error {
	if b.moveErr != nil {
		return b.moveErr
	}
	if b.moved == nil {
		b.moved = map[string]string{}
	}
	b.moved[src] = dest
	return nil
}

// servicesFake risolve un solo servizio, "main".
type servicesFake struct{ b *bucketFake }

func (s servicesFake) lookup(name string) (objectStore, bool) {
	if name != "main" || s.b == nil {
		return nil, false
	}
	return s.b, true
}

// storeFake implementa il solo InsertIfNotActive: l'interfaccia incorporata è nil, quindi ogni
// altra chiamata panica — se il feed ne facesse una, il test lo direbbe.
type storeFake struct {
	store.IWorkItemStore
	active   map[string]bool // ObjectId già attivi: InsertIfNotActive li scarta
	inserted []*store.WorkItem
	err      *core.Error
}

func (s *storeFake) InsertIfNotActive(_ context.Context, items []*store.WorkItem) (int, *core.Error) {
	if s.err != nil {
		return 0, s.err
	}
	n := 0
	for _, it := range items {
		if s.active[it.ObjectId] {
			continue
		}
		s.inserted = append(s.inserted, it)
		n++
	}
	return n, nil
}

func TestScan_FiltraOrdinaEFermaLElenco(t *testing.T) {
	b := &bucketFake{keys: []string{
		"in/", // segnaposto di cartella: non è un file
		"in/a.csv",
		"in/b.txt",
		"in/c.csv",
		"in/d.csv",
		"in/e.csv",
		"altro/x.csv",
	}}
	src := s3.Source{Service: "main", Path: "in/", Pattern: "*.csv", DestPath: "done"}

	items, err := scan(context.Background(), servicesFake{b}, "import", src, 3)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, it := range items {
		keys = append(keys, it.ObjectId)
	}
	if got := strings.Join(keys, ","); got != "in/a.csv,in/c.csv,in/d.csv" {
		t.Fatalf("file accodati: %s", got)
	}
	// Trovati 3 file l'elenco si ferma: in/e.csv non viene nemmeno visitato.
	if b.visited != 5 {
		t.Errorf("oggetti visitati %d, attesi 5: l'elenco non si è fermato al limite", b.visited)
	}

	first := items[0]
	if first.TaskName != "import" || first.Status != store.StatusPending || first.NextRunAt == nil {
		t.Errorf("item incompleto per il claim: %+v", first)
	}
	if p, ok := first.Payload.(s3Payload); !ok || p != (s3Payload{Service: "main", Key: "in/a.csv", DestPath: "done"}) {
		t.Errorf("payload: %#v", first.Payload)
	}
	// L'ordine dell'elenco deve sopravvivere al claim, che ordina per next_run_at e create_time.
	for i := 1; i < len(items); i++ {
		if !items[i].NextRunAt.After(*items[i-1].NextRunAt) || !items[i].CreateTime.After(items[i-1].CreateTime) {
			t.Fatalf("item %d non scaglionato rispetto al precedente", i)
		}
	}
}

func TestScan_Errori(t *testing.T) {
	b := &bucketFake{keys: []string{"in/a.csv"}}
	if _, err := scan(context.Background(), servicesFake{b}, "t", s3.Source{Service: "nope"}, 10); err == nil {
		t.Error("servizio sconosciuto: atteso errore")
	}
	if _, err := scan(context.Background(), servicesFake{b}, "t", s3.Source{Service: "main", Pattern: "[a-"}, 10); err == nil {
		t.Error("pattern malformato: atteso errore")
	}
	b.listErr = errors.New("boom")
	_, err := scan(context.Background(), servicesFake{b}, "t", s3.Source{Service: "main"}, 10)
	if appErr, ok := errors.AsType[*core.Error](err); !ok || appErr.Code != "BATCH-S3-LIST" {
		t.Errorf("elenco fallito: atteso BATCH-S3-LIST, ottenuto %v", err)
	}
}

func TestS3Feed_UsaLePropertyDelJob(t *testing.T) {
	b := &bucketFake{keys: []string{"in/a.csv", "in/b.txt"}}
	f := &s3Feed{svcs: servicesFake{b}}
	items, err := f.Feed(context.Background(), "import", properties.Properties{
		"service": "main", "path": "in/", "pattern": "*.csv", "dest-path": "done",
	}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ObjectId != "in/a.csv" {
		t.Fatalf("items: %+v", items)
	}
}

// Il payload del job S3 si decodifica con store.DecodePayload, che è il decodificatore di TUTTI
// i payload dei WorkItem. Qui resta la prova che l's3Payload sopravvive alle forme in cui i
// backend lo restituiscono — la copertura generale sta in store/payload_test.go.
func TestDecodePayload_S3Payload(t *testing.T) {
	atteso := s3Payload{Service: "main", Key: "inbox/data.csv", DestPath: "processed/"}

	// La forma con cui il driver Mongo restituisce un documento: una lista ORDINATA di coppie.
	// Espressa qui per struttura e non come bson.D, che è esattamente il modo in cui
	// store.DecodePayload la riconosce. Prima il convertitore locale di questo package passava
	// per json.Marshal — che su una lista produce un array — e il job non decodificava mai il
	// proprio payload quando il backend era Mongo.
	type coppia struct {
		Key   string
		Value any
	}
	casi := map[string]any{
		"s3Payload diretto":     atteso,
		"puntatore a s3Payload": &atteso,
		"map (colonna jsonb)":   map[string]any{"service": "main", "key": "inbox/data.csv", "destPath": "processed/"},
		"lista di coppie (bson.D di Mongo)": []coppia{
			{Key: "service", Value: "main"},
			{Key: "key", Value: "inbox/data.csv"},
			{Key: "destPath", Value: "processed/"},
		},
	}
	for nome, raw := range casi {
		t.Run(nome, func(t *testing.T) {
			var out s3Payload
			if err := store.DecodePayload(raw, &out); err != nil {
				t.Fatalf("decodifica fallita: %v", err)
			}
			if out != atteso {
				t.Errorf("got %+v, want %+v", out, atteso)
			}
		})
	}
}

// Il tetto ai ritentativi deve sopravvivere all'avvolgimento: è il *TaskRunner a finire nel gruppo
// batch_runners, quindi è il suo MaxRetry quello che mux.Runner.Run passa a store.ApplyResult. Senza
// questo passaggio `max-retry:` valeva per Register e non per RegisterFile.
func TestWrapFileRunners_ConservaMaxRetry(t *testing.T) {
	wrapped := wrapFileRunners(nil, []*taskrunner.FileTaskRunner{
		taskrunner.NewFile("s3-in", nil).WithMaxRetry(3),
		taskrunner.NewFile("s3-bulk", nil),
	})
	if len(wrapped) != 2 {
		t.Fatalf("attesi 2 runner avvolti, ottenuto %d", len(wrapped))
	}
	if wrapped[0].TaskName != "s3-in" || wrapped[0].ResolveMaxRetry() != 3 {
		t.Fatalf("il tetto si è perso nell'avvolgimento: %+v", wrapped[0])
	}
	if wrapped[1].ResolveMaxRetry() != task.MaxRetryUnlimited {
		t.Fatalf("l'assenza deve valere illimitato, ottenuto %d", wrapped[1].ResolveMaxRetry())
	}
}
