package kafkajob

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
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
		valid, recs, invalid := prepareRecords([]*store.WorkItem{item}, "notifiche.topic")
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
			valid, recs, invalid := prepareRecords([]*store.WorkItem{tc.item, buono}, "notifiche.topic")
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
	reasons map[string]string // id -> motivo passato a MarkFailed
	// claimedTask/recoveredTask registrano la CODA su cui il job ha claimato: è il binding fra
	// `properties.stream` e WorkItem.TaskName, che prima nessun test guardava perché il fake
	// scartava i propri argomenti.
	claimedTask   string
	recoveredTask string
}

func (f *fakeStore) RecoverOrphans(_ context.Context, taskName string, _ time.Duration, _ int) ([]*store.WorkItem, *core.ApplicationError) {
	f.recoveredTask = taskName
	return nil, nil
}

func (f *fakeStore) ClaimPending(_ context.Context, taskName string, _ int) ([]*store.WorkItem, *core.ApplicationError) {
	f.claimedTask = taskName
	return f.claim, nil
}

func (f *fakeStore) MarkDone(_ context.Context, ids []string, token string) *core.ApplicationError {
	if f.done == nil {
		f.done = map[string][]string{}
	}
	f.done[token] = append(f.done[token], ids...)
	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, id, _, reason string) *core.ApplicationError {
	f.failed = append(f.failed, id)
	if f.reasons == nil {
		f.reasons = map[string]string{}
	}
	f.reasons[id] = reason
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
			"stream": "notifiche-edwh",
			"topic":  "notifiche.topic",
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

// Le chiavi con cui questo job rilegge il payload DEVONO essere i tag json della kafka.Message che
// l'applicazione costruisce. Sono due dichiarazioni dello stesso contratto in due package — la
// struct sta in `kafka`, il lettore qui — e senza questo test un rename dei tag passerebbe la
// compilazione e romperebbe la produzione: il job non troverebbe più i campi e marcherebbe FALLITI
// tutti gli item, uno per uno, come payload non utilizzabili.
func TestChiaviDelPayload_CoincidonoCoiTagDiMessage(t *testing.T) {
	atteso := map[string]string{
		"MessageKey":    kafka.KeyMessageKey,
		"MessageValue":  kafka.KeyMessageValue,
		"MessageHeader": kafka.KeyMessageHeaders,
		"Topic":         kafka.KeyTopic,
	}
	tipo := reflect.TypeFor[kafka.Message]()
	if tipo.NumField() != len(atteso) {
		t.Fatalf("kafka.Message ha %d campi, le costanti ne coprono %d: aggiornare entrambi",
			tipo.NumField(), len(atteso))
	}
	for i := range tipo.NumField() {
		f := tipo.Field(i)
		chiave, previsto := atteso[f.Name]
		if !previsto {
			t.Fatalf("campo %q di kafka.Message senza costante corrispondente", f.Name)
		}
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if tag != chiave {
			t.Errorf("campo %s: tag json %q, costante %q — il job leggerebbe una chiave che non esiste",
				f.Name, tag, chiave)
		}
	}
}

// Il claim gira sulla CODA nominata da `properties.stream`, non sul job type. È il binding che
// rende la deduplica per-flusso: uk_workitem_active è unico su (task_name, object_id), quindi
// finché task_name era la costante "NotificationKafka" due flussi diversi sullo stesso objectId
// collidevano e InsertIfNotActive scartava il secondo in silenzio.
func TestNotificationJobRun_ClaimaLaCodaDelloStream(t *testing.T) {
	st := &fakeStore{claim: []*store.WorkItem{wi("a", "tok-1", payload("a"))}}
	if err := notificationJobRun("notifica", &fakeProducer{}, st, notificaConfig()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if st.claimedTask != "notifiche-edwh" || st.recoveredTask != "notifiche-edwh" {
		t.Errorf("claim su %q / recover su %q, atteso %q — il job non sta usando properties.stream",
			st.claimedTask, st.recoveredTask, "notifiche-edwh")
	}
	if st.claimedTask == JobType {
		t.Errorf("il claim usa ancora il job type come coda")
	}
}

// `stream` è obbligatoria: senza, il job non sa quale coda drenare e non c'è default sensato —
// il vecchio default implicito (il job type) è esattamente ciò che collassava i flussi.
func TestRisolvi_StreamObbligatoria(t *testing.T) {
	_, err := risolvi("notifica", scheduler.Config{
		Type:       JobType,
		Properties: core.Properties{"topic": "notifiche.topic"},
	})
	if err == nil || !strings.Contains(err.Error(), PropStream) {
		t.Fatalf("errore = %v, atteso un messaggio che nomini %q", err, PropStream)
	}
}

// Il topic dell'item VINCE sul default del job: è ciò che permette a un solo job di drenare un
// flusso verso topic diversi. ProduceTo stampa il default sui soli record che non ne portano uno.
func TestToRecord_IlTopicDellItemVinceSulDefault(t *testing.T) {
	conProprio := map[string]any{
		"messageKey": "k", "messageValue": map[string]any{"a": 1}, "topic": "topic.suo",
	}
	rec, err := toRecord(wi("x", "tok", conProprio), "topic.default")
	if err != nil {
		t.Fatalf("toRecord: %v", err)
	}
	if rec.Topic != "topic.suo" {
		t.Errorf("topic = %q, atteso quello dell'item", rec.Topic)
	}

	// Senza topic sull'item il record resta senza: lo stampa ProduceTo dal default del job.
	rec, err = toRecord(wi("y", "tok", payload("y")), "topic.default")
	if err != nil {
		t.Fatalf("toRecord: %v", err)
	}
	if rec.Topic != "" {
		t.Errorf("topic = %q, atteso vuoto (lo mette ProduceTo)", rec.Topic)
	}
}

// Nessun topic da nessuna delle due parti = nessuna destinazione. È un difetto deterministico di
// QUEL payload, quindi l'item fallisce da solo e gli altri del batch passano — non una Produce su
// topic vuoto, che sarebbe un errore di tutto il tick.
func TestPublishBatch_SenzaTopicLItemFallisceDaSolo(t *testing.T) {
	senzaTopic := wi("orfano", "tok-1", payload("orfano"))
	conTopic := wi("buono", "tok-1", map[string]any{
		"messageKey": "k", "messageValue": map[string]any{"a": 1}, "topic": "topic.suo",
	})
	st := &fakeStore{claim: []*store.WorkItem{senzaTopic, conTopic}}
	prod := &fakeProducer{}

	cfg := scheduler.Config{Type: JobType, Properties: core.Properties{"stream": "notifiche-edwh"}}
	if err := notificationJobRun("notifica", prod, st, cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(st.failed) != 1 || st.failed[0] != "orfano" {
		t.Fatalf("falliti = %v, atteso il solo item senza topic", st.failed)
	}
	if len(prod.sent) != 1 || prod.sent[0].Topic != "topic.suo" {
		t.Fatalf("prodotti %d record, atteso il solo item col proprio topic", len(prod.sent))
	}
}

// Il tetto ai ritentativi si applica al batch APPENA CLAIMATO, che è l'unico punto in cui copre
// anche il percorso degli orfani: RecoverOrphans incrementa `retry` senza passare dal job, quindi
// un item la cui produzione non riesce mai veniva ri-claimato per sempre, occupando uno slot del
// `limit` a ogni tick.
func TestPublishBatch_MaxRetryEsaurito(t *testing.T) {
	esaurito := wi("vecchio", "tok-1", payload("vecchio"))
	esaurito.Retry = 3
	sotto := wi("giovane", "tok-1", payload("giovane"))
	sotto.Retry = 2
	st := &fakeStore{claim: []*store.WorkItem{esaurito, sotto}}
	prod := &fakeProducer{}

	cfg := notificaConfig()
	cfg.Properties[PropMaxRetry] = 3
	if err := notificationJobRun("notifica", prod, st, cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(st.failed) != 1 || st.failed[0] != "vecchio" {
		t.Fatalf("falliti = %v, atteso il solo item oltre il tetto", st.failed)
	}
	if !strings.Contains(st.reasons["vecchio"], "max-retry") {
		t.Errorf("motivo = %q, deve nominare il tetto", st.reasons["vecchio"])
	}
	if len(prod.sent) != 1 || len(st.done["tok-1"]) != 1 || st.done["tok-1"][0] != "giovane" {
		t.Fatalf("pubblicati %d record, done=%v — atteso il solo item entro il tetto", len(prod.sent), st.done)
	}
}

// Assente = illimitato, che è la condotta storica e quella di task.Config.MaxRetry: chi aggiorna
// la libreria senza toccare la config non deve vedere item andare in FAILED.
func TestPublishBatch_SenzaMaxRetryNessunTetto(t *testing.T) {
	vecchio := wi("vecchio", "tok-1", payload("vecchio"))
	vecchio.Retry = 99
	st := &fakeStore{claim: []*store.WorkItem{vecchio}}
	prod := &fakeProducer{}

	if err := notificationJobRun("notifica", prod, st, notificaConfig()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(st.failed) != 0 || len(prod.sent) != 1 {
		t.Fatalf("falliti = %v, prodotti = %d — senza la property il tetto non esiste", st.failed, len(prod.sent))
	}
}

// Un max-retry scritto male deve fermare l'avvio, non ricadere in silenzio sull'illimitato: è la
// stessa regola di `limit` (scheduler.Props.PositiveInt).
func TestRisolvi_MaxRetryNonValido(t *testing.T) {
	cfg := notificaConfig()
	cfg.Properties[PropMaxRetry] = "tre"
	if _, err := risolvi("notifica", cfg); err == nil || !strings.Contains(err.Error(), PropMaxRetry) {
		t.Fatalf("errore = %v, atteso un messaggio che nomini %q", err, PropMaxRetry)
	}
}

// -1 è la scrittura ESPLICITA dell'illimitato, la stessa di task.Config.MaxRetry: rifiutarla
// sarebbe una trappola per chi copia la convenzione dalla sezione `tasks:`.
func TestRisolvi_MaxRetryMenoUnoEIllimitato(t *testing.T) {
	cfg := notificaConfig()
	cfg.Properties[PropMaxRetry] = -1
	p, err := risolvi("notifica", cfg)
	if err != nil {
		t.Fatalf("risolvi: %v", err)
	}
	if p.maxRetry != task.MaxRetryUnlimited {
		t.Errorf("maxRetry = %d, atteso %d", p.maxRetry, task.MaxRetryUnlimited)
	}
}

// max-retry: 0 significa "nessun ritentativo", non "assente": il primo fallimento è definitivo.
func TestRisolvi_MaxRetryZeroNonEAssente(t *testing.T) {
	cfg := notificaConfig()
	cfg.Properties[PropMaxRetry] = 0
	p, err := risolvi("notifica", cfg)
	if err != nil {
		t.Fatalf("risolvi: %v", err)
	}
	if p.maxRetry != 0 {
		t.Errorf("maxRetry = %d, atteso 0", p.maxRetry)
	}
}
