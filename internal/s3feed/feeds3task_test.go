package s3feed

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

func importTasks() []task.Config {
	return []task.Config{
		{Name: "import", Type: "IMPORT", S3: &s3.Source{Service: "main", Path: "inbox/", Pattern: "*", DestPath: "archive"}},
		{Name: "selftest", Type: "SELFTEST"},
	}
}

func feedJob(name string, props properties.Properties) scheduler.Config {
	return scheduler.Config{Name: name, Type: JobTypeFeedS3Task, ScheduledCron: "0 0 22 * * *", Properties: props}
}

func newTestFeeder(t *testing.T, b *bucketFake, st *storeFake) *feeder {
	t.Helper()
	f, err := buildFeeder(importTasks(), servicesFake{b}, st)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Un giro accoda i file nuovi, non riaccoda quelli già attivi, e l'esito dice allo script cosa fare.
func TestFeeder_Esiti(t *testing.T) {
	vuoto := newTestFeeder(t, &bucketFake{}, &storeFake{})
	res, appErr := vuoto.Feed(context.Background(), "import")
	if appErr != nil || res.Outcome() != s3.NothingToDo {
		t.Fatalf("bucket vuoto: %+v %v, atteso NOTHINGTODO", res, appErr)
	}

	b := &bucketFake{keys: []string{"inbox/1.dat", "inbox/2.dat"}}
	st := &storeFake{}
	f := newTestFeeder(t, b, st)
	res, appErr = f.Feed(context.Background(), "IMPORT") // il task si risolve case-insensitive
	if appErr != nil || res != (s3.FeedResult{Found: 2, Queued: 2}) {
		t.Fatalf("primo giro: %+v %v, atteso QUEUED 2/2", res, appErr)
	}
	if st.inserted[0].TaskName != "import" {
		t.Errorf("accodato su %q, atteso il nome del task come scritto in config", st.inserted[0].TaskName)
	}

	// I file sono ancora nel bucket (in lavorazione): il giro successivo non riaccoda nulla.
	st.active = map[string]bool{"inbox/1.dat": true, "inbox/2.dat": true}
	if res, _ = f.Feed(context.Background(), "import"); res.Outcome() != s3.InProgress {
		t.Fatalf("file già attivi: %+v, atteso INPROGRESS", res)
	}

	st.err = core.TechnicalError().WithCode("BATCH-INSERT")
	if _, appErr = f.Feed(context.Background(), "import"); appErr == nil || appErr.Code != "BATCH-INSERT" {
		t.Fatalf("insert fallita: atteso BATCH-INSERT, ottenuto %v", appErr)
	}

	b.listErr = errors.New("boom")
	st.err = nil
	if _, appErr = f.Feed(context.Background(), "import"); appErr == nil || appErr.Code != "BATCH-S3-LIST" {
		t.Fatalf("elenco fallito: atteso BATCH-S3-LIST, ottenuto %v", appErr)
	}

	_, appErr = f.Feed(context.Background(), "selftest")
	if appErr == nil || appErr.Code != "BATCH-S3-FEED" || !strings.Contains(appErr.Message, "import") {
		t.Fatalf("task senza s3: atteso BATCH-S3-FEED con i task che lo hanno, ottenuto %v", appErr)
	}
	if got := f.Sources(); len(got) != 1 || got["import"].Path != "inbox/" {
		t.Fatalf("Sources: %+v", got)
	}
}

// La validazione dei blocchi s3: avviene alla costruzione: un errore ferma l'avvio.
func TestBuildFeeder_Validazione(t *testing.T) {
	casi := map[string]s3.Source{
		"senza service":           {DestPath: "done"},
		"senza dest-path":         {Service: "main"},
		"pattern malformato":      {Service: "main", DestPath: "done", Pattern: "[a-"},
		"servizio non dichiarato": {Service: "altro", DestPath: "done"},
	}
	for nome, src := range casi {
		t.Run(nome, func(t *testing.T) {
			_, err := buildFeeder([]task.Config{{Name: "x", Type: "X", S3: &src}}, servicesFake{&bucketFake{}}, &storeFake{})
			if err == nil || !strings.Contains(err.Error(), `task "x"`) {
				t.Fatalf("atteso un errore che nomini il task, ottenuto %v", err)
			}
		})
	}
}

func TestFeedS3Registration_Check(t *testing.T) {
	reg := registerFeedS3(newTestFeeder(t, &bucketFake{}, &storeFake{}))
	if reg.Type != JobTypeFeedS3Task {
		t.Fatalf("type %q", reg.Type)
	}
	if err := reg.Check("feed-import", feedJob("feed-import", properties.Properties{"task": "import"})); err != nil {
		t.Fatalf("config valida rifiutata: %v", err)
	}
	for nome, props := range map[string]properties.Properties{
		"senza task":          {},
		"task senza s3":       {"task": "selftest"},
		"task non dichiarato": {"task": "nessuno"},
	} {
		if err := reg.Check("feed", feedJob("feed", props)); err == nil {
			t.Errorf("%s: atteso errore al boot", nome)
		}
	}
}

// fileRunnerSpy è il runner dell'app: riceve chiave e contenuto, e basta.
type fileRunnerSpy struct {
	key, content string
	err          error
}

func (r *fileRunnerSpy) Run(_ context.Context, key string, content io.Reader) error {
	b, _ := io.ReadAll(content)
	r.key, r.content = key, string(b)
	return r.err
}

// Gli item accodati dal feed arrivano al runner dell'app col file già scaricato, e a successo il
// file viene spostato: il runner non elenca, non scarica e non sposta.
func TestFileRunner_RiceveIlFileAccodatoDalFeed(t *testing.T) {
	b := &bucketFake{
		keys:    []string{"inbox/1.dat"},
		content: map[string]string{"inbox/1.dat": "riga1\n"},
	}
	st := &storeFake{}
	if _, appErr := newTestFeeder(t, b, st).Feed(context.Background(), "import"); appErr != nil {
		t.Fatal(appErr)
	}

	spy := &fileRunnerSpy{}
	fr := &fileRunner{svcs: servicesFake{b}, inner: spy}
	if err := fr.Run(context.Background(), st.inserted[0]); err != nil {
		t.Fatal(err)
	}
	if spy.key != "inbox/1.dat" || spy.content != "riga1\n" {
		t.Errorf("il runner ha ricevuto %q / %q", spy.key, spy.content)
	}
	if b.moved["inbox/1.dat"] != "archive/1.dat" {
		t.Errorf("spostamenti: %v", b.moved)
	}

	// Runner in errore: il file resta dov'è.
	b.moved = nil
	spy.err = errors.New("tracciato non valido")
	if err := fr.Run(context.Background(), st.inserted[0]); err == nil {
		t.Fatal("atteso l'errore del runner")
	}
	if len(b.moved) != 0 {
		t.Errorf("file spostato nonostante l'errore: %v", b.moved)
	}

	// Spostamento fallito: retry dello stesso item, non FAILED.
	spy.err = nil
	b.moveErr = errors.New("s3 giù")
	err := fr.Run(context.Background(), st.inserted[0])
	if _, isRetry := errors.AsType[*store.RetryError](err); !isRetry {
		t.Errorf("move fallito: atteso RetryError, ottenuto %v", err)
	}
}
