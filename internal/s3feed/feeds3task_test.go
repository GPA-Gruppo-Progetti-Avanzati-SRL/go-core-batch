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
)

func feedJob(name string, props properties.Properties) scheduler.Config {
	return scheduler.Config{Name: name, Type: JobTypeFeedS3Task, ScheduledCron: "0 0 22 * * *", Properties: props}
}

func cflowProps() properties.Properties {
	return properties.Properties{
		"task": "cflow", "service": "main", "path": "cflow/todo/", "pattern": "*", "dest-path": "cflow/done",
	}
}

// Un giro accoda i file nuovi, non riaccoda quelli già attivi, e l'esito dice allo script cosa fare.
func TestFeedOnce_Esiti(t *testing.T) {
	spec, err := specFrom("feed-cflow", feedJob("feed-cflow", cflowProps()))
	if err != nil {
		t.Fatal(err)
	}

	vuoto := &bucketFake{}
	res, err := feedOnce(context.Background(), servicesFake{vuoto}, &storeFake{}, spec)
	if err != nil || res.Outcome() != s3.NothingToDo {
		t.Fatalf("bucket vuoto: %+v %v, atteso NOTHINGTODO", res, err)
	}

	b := &bucketFake{keys: []string{"cflow/todo/1.dat", "cflow/todo/2.dat"}}
	st := &storeFake{}
	res, err = feedOnce(context.Background(), servicesFake{b}, st, spec)
	if err != nil || res != (s3.FeedResult{Found: 2, Queued: 2}) || res.Outcome() != s3.Queued {
		t.Fatalf("primo giro: %+v %v, atteso QUEUED 2/2", res, err)
	}
	if st.inserted[0].TaskName != "cflow" {
		t.Errorf("accodato su %q, atteso il task del job", st.inserted[0].TaskName)
	}

	// I file sono ancora nel bucket (in lavorazione): il giro successivo non riaccoda nulla.
	st.active = map[string]bool{"cflow/todo/1.dat": true, "cflow/todo/2.dat": true}
	res, err = feedOnce(context.Background(), servicesFake{b}, st, spec)
	if err != nil || res.Outcome() != s3.InProgress {
		t.Fatalf("file già attivi: %+v %v, atteso INPROGRESS", res, err)
	}

	st.err = core.TechnicalError().WithCode("BATCH-INSERT")
	if _, err := feedOnce(context.Background(), servicesFake{b}, st, spec); err == nil {
		t.Fatal("insert fallita: atteso errore")
	}
}

func TestSpecFrom_PropertyObbligatorie(t *testing.T) {
	for _, manca := range []string{"task", "service", "dest-path"} {
		props := cflowProps()
		delete(props, manca)
		_, err := specFrom("feed-cflow", feedJob("feed-cflow", props))
		if err == nil || !strings.Contains(err.Error(), manca) {
			t.Errorf("senza %q: atteso un errore che la nomini, ottenuto %v", manca, err)
		}
	}

	props := cflowProps()
	props["pattern"] = "[a-"
	if _, err := specFrom("feed-cflow", feedJob("feed-cflow", props)); err == nil {
		t.Error("pattern malformato: atteso errore al boot")
	}

	spec, err := specFrom("feed-cflow", feedJob("feed-cflow", cflowProps()))
	if err != nil || spec.limit != defaultFeedLimit {
		t.Errorf("limit assente: atteso il default %d, ottenuto %+v %v", defaultFeedLimit, spec, err)
	}
}

func TestFeedS3Registration_Check(t *testing.T) {
	reg := feedS3Registration(&storeFake{}, servicesFake{&bucketFake{}})
	if reg.Type != JobTypeFeedS3Task {
		t.Fatalf("type %q", reg.Type)
	}
	if err := reg.Check("feed-cflow", feedJob("feed-cflow", cflowProps())); err != nil {
		t.Fatalf("config valida rifiutata: %v", err)
	}
	props := cflowProps()
	props["service"] = "sconosciuto"
	if err := reg.Check("feed-cflow", feedJob("feed-cflow", props)); err == nil {
		t.Error("servizio non dichiarato in s3.services: atteso errore al boot")
	}
}

func TestFeeder(t *testing.T) {
	b := &bucketFake{keys: []string{"cflow/todo/1.dat"}}
	st := &storeFake{}
	solaAPI := feedJob("feed-cflow", cflowProps())
	solaAPI.Disabled = true // flusso alimentato solo da API: la voce c'è, il cron no
	jobs := []scheduler.Config{
		{Name: "altro", Type: "SingleTask", Properties: properties.Properties{"task": "x"}},
		solaAPI,
	}
	f, err := buildFeeder(jobs, servicesFake{b}, st)
	if err != nil {
		t.Fatal(err)
	}

	res, appErr := f.Feed(context.Background(), "FEED-CFLOW") // il nome si confronta case-insensitive
	if appErr != nil || res.Outcome() != s3.Queued || len(st.inserted) != 1 {
		t.Fatalf("Feed: %+v %v", res, appErr)
	}

	_, appErr = f.Feed(context.Background(), "altro")
	if appErr == nil || appErr.Code != "BATCH-S3-FEED" || !strings.Contains(appErr.Message, "feed-cflow") {
		t.Fatalf("job che non è un FeedS3Task: atteso BATCH-S3-FEED con i nomi dichiarati, ottenuto %v", appErr)
	}

	b.listErr = errors.New("boom")
	if _, appErr = f.Feed(context.Background(), "feed-cflow"); appErr == nil || appErr.Code != "BATCH-S3-LIST" {
		t.Fatalf("elenco fallito: atteso BATCH-S3-LIST, ottenuto %v", appErr)
	}

	// La validazione avviene alla costruzione, anche sulle voci disabilitate.
	rotto := feedJob("feed-rotto", properties.Properties{"task": "x", "service": "main"})
	rotto.Disabled = true
	if _, err := buildFeeder([]scheduler.Config{rotto}, servicesFake{b}, st); err == nil {
		t.Error("voce senza dest-path: atteso errore alla costruzione del Feeder")
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
		keys:    []string{"cflow/todo/1.dat"},
		content: map[string]string{"cflow/todo/1.dat": "riga1\n"},
	}
	spec, _ := specFrom("feed-cflow", feedJob("feed-cflow", cflowProps()))
	st := &storeFake{}
	if _, err := feedOnce(context.Background(), servicesFake{b}, st, spec); err != nil {
		t.Fatal(err)
	}

	spy := &fileRunnerSpy{}
	fr := &fileRunner{svcs: servicesFake{b}, inner: spy}
	if err := fr.Run(context.Background(), st.inserted[0]); err != nil {
		t.Fatal(err)
	}
	if spy.key != "cflow/todo/1.dat" || spy.content != "riga1\n" {
		t.Errorf("il runner ha ricevuto %q / %q", spy.key, spy.content)
	}
	if b.moved["cflow/todo/1.dat"] != "cflow/done/1.dat" {
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
