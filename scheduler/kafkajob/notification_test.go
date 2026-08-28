package kafkajob

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/message"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestPrepareRecordsBsonPayload riproduce il payload di un WI NotificationKafka riletto da Mongo
// (Payload = bson.D con sottodocumenti bson.D per messageValue e messageHeaders) e dalla colonna jsonb
// di SQL (map[string]any), e verifica che prepareRecords: (1) non lo scarti come payload invalido,
// (2) mappi gli header sulla lista message.Headers, (3) serializzi messageValue come OGGETTO JSON e non
// come array — che è ciò che darebbe un json.Marshal su bson.D senza la conversione a tipi nativi.
func TestPrepareRecordsBsonPayload(t *testing.T) {
	mongoItem := &store.WorkItem{
		Id: "BR-test-1",
		Payload: bson.D{
			{Key: "messageKey", Value: "BR-test-1"},
			{Key: "messageValue", Value: bson.D{
				{Key: "numeroOrdine", Value: "584"},
				{Key: "stato", Value: "KO"},
				{Key: "datiRicarica", Value: bson.D{{Key: "importoRicarica", Value: 1.23}}},
			}},
			{Key: "messageHeaders", Value: bson.D{
				{Key: "canale", Value: "APBP"},
				{Key: "stato-operazione", Value: "KO"},
			}},
		},
	}

	// Caso SQL: bun rilegge la colonna jsonb come map[string]interface{} (tipi già nativi).
	sqlItem := &store.WorkItem{
		Id: "BR-test-sql",
		Payload: map[string]any{
			"messageKey":   "BR-test-sql",
			"messageValue": map[string]any{"numeroOrdine": "584", "stato": "KO"},
			"messageHeaders": map[string]any{
				"canale":           "APBP",
				"stato-operazione": "KO",
			},
		},
	}

	for _, item := range []*store.WorkItem{mongoItem, sqlItem} {
		valid, recs, invalid := prepareRecords([]*store.WorkItem{item})
		if len(valid) != 1 || len(recs) != 1 || len(invalid) != 0 {
			t.Fatalf("[%s] atteso 1 record: valid=%d recs=%d invalid=%d (payload scartato?)", item.Id, len(valid), len(recs), len(invalid))
		}
		r := recs[0]
		if r.Headers.Get("canale") != "APBP" || r.Headers.Get("stato-operazione") != "KO" {
			t.Fatalf("[%s] header non mappati correttamente: %#v", item.Id, r.Headers)
		}
		// La chiave è JSON-encoded, non la stringa nuda: è il formato storico del job, e cambiarlo
		// cambierebbe il partizionamento dei topic già in esercizio.
		if want := `"` + item.Id + `"`; string(r.Key) != want {
			t.Fatalf("[%s] chiave = %s, attesa %s (JSON-encoded)", item.Id, r.Key, want)
		}
		if !strings.HasPrefix(strings.TrimSpace(string(r.Value)), "{") {
			t.Fatalf("[%s] messageValue serializzato come NON-oggetto (regressione bson.D): %s", item.Id, r.Value)
		}
		if !strings.Contains(string(r.Value), `"numeroOrdine":"584"`) {
			t.Fatalf("[%s] messageValue serializzato in modo inatteso: %s", item.Id, r.Value)
		}
		// Il topic NON è impostato qui: lo mette ProduceTo dalla property del job.
		if r.Topic != "" {
			t.Errorf("[%s] topic impostato in prepareRecords (%q): è una decisione del job", item.Id, r.Topic)
		}
	}
}

// TestPrepareRecords_UnPayloadRottoNonAffondaGliAltri: un payload malformato è un difetto
// deterministico di QUEL work item. Prima il json.Marshal avveniva nel producer, quindi un solo
// payload non serializzabile faceva fallire l'intero tick e lasciava anche gli item buoni in
// IN_PROGRESS fino al recupero orfani.
func TestPrepareRecords_UnPayloadRottoNonAffondaGliAltri(t *testing.T) {
	buono := &store.WorkItem{Id: "ok", Payload: map[string]any{"messageKey": "k", "messageValue": map[string]any{"a": 1}}}
	tests := []struct {
		name string
		item *store.WorkItem
	}{
		{"payload di tipo non gestito", &store.WorkItem{Id: "tipo", Payload: 42}},
		{"messageKey mancante", &store.WorkItem{Id: "nokey", Payload: map[string]any{"messageValue": "v"}}},
		{"messageValue mancante", &store.WorkItem{Id: "noval", Payload: map[string]any{"messageKey": "k"}}},
		{"header non stringa", &store.WorkItem{Id: "hdr", Payload: map[string]any{
			"messageKey": "k", "messageValue": "v", "messageHeaders": map[string]any{"n": 1},
		}}},
		{"valore non serializzabile", &store.WorkItem{Id: "nojson", Payload: map[string]any{
			"messageKey": "k", "messageValue": map[string]any{"f": func() {}},
		}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			valid, recs, invalid := prepareRecords([]*store.WorkItem{tc.item, buono})
			if len(recs) != 1 || len(valid) != 1 || valid[0].Id != "ok" {
				t.Fatalf("l'item valido non è passato: valid=%v recs=%d", valid, len(recs))
			}
			if len(invalid) != 1 || invalid[0].Id != tc.item.Id {
				t.Fatalf("l'item rotto non è stato isolato: invalid=%v", invalid)
			}
		})
	}
}

// --- il job: esito della produzione → lifecycle degli item ---

type fakeProducer struct {
	sent  []*message.ProducerRecord
	topic string
	err   *core.ApplicationError
}

func (f *fakeProducer) Produce(_ context.Context, recs []*message.ProducerRecord) *core.ApplicationError {
	f.sent = append(f.sent, recs...)
	return f.err
}

func (f *fakeProducer) ProduceTo(ctx context.Context, topic string, recs []*message.ProducerRecord) *core.ApplicationError {
	f.topic = topic
	return f.Produce(ctx, recs)
}

// fakeStore implementa i soli metodi che il job usa: il resto dell'interfaccia è embeddato, così
// un metodo nuovo su IWorkItemStore non rompe questo test — e se il job cominciasse a usarne uno non
// implementato, il nil panic direbbe esattamente quale.
type fakeStore struct {
	store.IWorkItemStore
	claim   []*store.WorkItem
	done    map[string][]string // token -> ids, per verificare il raggruppamento
	pending []string
	failed  []string
}

func (f *fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*store.WorkItem, *core.ApplicationError) {
	return nil, nil
}

func (f *fakeStore) ClaimPending(context.Context, string, string, string, int) ([]*store.WorkItem, *core.ApplicationError) {
	return f.claim, nil
}

func (f *fakeStore) MarkDone(_ context.Context, ids []string, token string) *core.ApplicationError {
	if f.done == nil {
		f.done = map[string][]string{}
	}
	f.done[token] = append(f.done[token], ids...)
	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, id, _, _ string) *core.ApplicationError {
	f.failed = append(f.failed, id)
	return nil
}

func (f *fakeStore) MarkPending(_ context.Context, id, _ string, _ time.Duration) *core.ApplicationError {
	f.pending = append(f.pending, id)
	return nil
}

func notificaConfig() scheduler.Config {
	return scheduler.Config{
		Type: JobType,
		Properties: core.Properties{
			"destination": "edwh",
			"object":      "ricarica",
			"topic":       "notifiche.topic",
		},
	}
}

func wi(id, token string, payload any) *store.WorkItem {
	return &store.WorkItem{Id: id, LockToken: token, Payload: payload}
}

func payload(key string) map[string]any {
	return map[string]any{"messageKey": key, "messageValue": map[string]any{"stato": "OK"}}
}

// Il tick riuscito: i record vanno sul topic della property, e gli item passano a DONE raggruppati
// PER TOKEN — un update per gruppo di claim invece di N, e ogni gruppo fenced dal proprio token.
func TestNotificationJobRun_MarkDoneRaggruppatoPerToken(t *testing.T) {
	st := &fakeStore{claim: []*store.WorkItem{
		wi("a", "tok-1", payload("a")),
		wi("b", "tok-1", payload("b")),
		wi("c", "tok-2", payload("c")),
	}}
	prod := &fakeProducer{}

	if err := notificationJobRun("notifica", prod, st, notificaConfig()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(prod.sent) != 3 || prod.topic != "notifiche.topic" {
		t.Fatalf("prodotti %d record sul topic %q", len(prod.sent), prod.topic)
	}
	if len(st.done) != 2 || len(st.done["tok-1"]) != 2 || len(st.done["tok-2"]) != 1 {
		t.Fatalf("MarkDone non raggruppato per token: %#v", st.done)
	}
}

// Produzione fallita: gli item claimati tornano PENDING, così il tick successivo li riprende. Senza,
// resterebbero IN_PROGRESS fino al recupero orfani — cioè fermi per l'orphan age (10m di default).
func TestNotificationJobRun_ProduzioneFallitaRimettePending(t *testing.T) {
	st := &fakeStore{claim: []*store.WorkItem{wi("a", "tok-1", payload("a")), wi("b", "tok-1", payload("b"))}}
	prod := &fakeProducer{err: core.TechnicalError().WithMessage("broker giù")}

	if err := notificationJobRun("notifica", prod, st, notificaConfig()); err == nil {
		t.Fatal("atteso errore: l'errore di produzione deve risalire al job")
	}
	if len(st.pending) != 2 {
		t.Fatalf("item rimessi PENDING = %v, attesi 2", st.pending)
	}
	if len(st.done) != 0 {
		t.Fatalf("MarkDone chiamato dopo una produzione fallita: %#v", st.done)
	}
}

// Un payload rotto in mezzo a quelli buoni: il rotto va FAILED, gli altri vengono prodotti e chiusi.
// È il caso che prima faceva cadere il tick intero.
func TestNotificationJobRun_PayloadRottoIsolato(t *testing.T) {
	st := &fakeStore{claim: []*store.WorkItem{
		wi("rotto", "tok-1", 42),
		wi("buono", "tok-1", payload("buono")),
	}}
	prod := &fakeProducer{}

	if err := notificationJobRun("notifica", prod, st, notificaConfig()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(st.failed) != 1 || st.failed[0] != "rotto" {
		t.Fatalf("item falliti = %v, atteso [rotto]", st.failed)
	}
	if len(prod.sent) != 1 {
		t.Fatalf("prodotti %d record, atteso 1 (il solo buono)", len(prod.sent))
	}
	if len(st.done["tok-1"]) != 1 || st.done["tok-1"][0] != "buono" {
		t.Fatalf("MarkDone = %#v, atteso il solo buono", st.done)
	}
}
