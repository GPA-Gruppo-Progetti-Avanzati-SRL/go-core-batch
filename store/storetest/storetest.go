// Package storetest è la suite che ogni backend di store.IWorkItemStore esegue identica: claim,
// recupero degli orfani, fencing dei Mark* e deduplica degli accodamenti.
//
// Esiste per la stessa ragione della conformance di go-core-locker: i backend sono due, scritti in
// due linguaggi di query diversi, e la sola garanzia che non divergano è che rispondano alle stesse
// prove. I backend la eseguono contro un database vero (PG_URL, MONGO_URL) e la saltano
// senza — il claiming è codice che un fake non verifica.
package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// tolerance è lo scarto ammesso fra l'orologio del database e quello del test quando si verifica
// un istante scritto dallo store: il database gira su un'altra macchina (o in un container), e la
// suite verifica che l'istante sia quello giusto, non che i due orologi coincidano.
const tolerance = time.Minute

// Run esegue la suite. newStore ritorna uno store su una collection/tabella vuota, isolata dagli
// altri test.
func Run(t *testing.T, newStore func(t *testing.T) store.IWorkItemStore) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s store.IWorkItemStore)
	}{
		{"ClaimPending_SoloScaduti", claimSoloScaduti},
		{"ClaimPending_UnaVoltaSola", claimUnaVoltaSola},
		{"ClaimPending_RispettaIlLimite", claimRispettaIlLimite},
		{"Mark_FencingDelToken", markFencing},
		{"MarkPending_RitardaEConta", markPendingRitardaEConta},
		{"Release_NonConta", releaseNonConta},
		{"RecoverOrphans_SoloOltreLaSoglia", recoverSoloOltreLaSoglia},
		{"RecoverOrphans_FencingDelVecchioClaim", recoverFencing},
		{"RecoverOrphans_LeaseMalformatoScaduto", recoverLeaseMalformato},
		{"InsertIfNotActive_Deduplica", insertIfNotActive},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore(t)) })
	}
}

const task = "storetest"

func item(id string, nextRunAt *time.Time) *store.WorkItem {
	return &store.WorkItem{
		Id: id, TaskName: task, ObjectId: id, Status: store.StatusPending,
		CreateTime: time.Now(), NextRunAt: nextRunAt, Payload: map[string]any{"id": id},
	}
}

func at(d time.Duration) *time.Time { t := time.Now().Add(d); return &t }

func insert(t *testing.T, s store.IWorkItemStore, items ...*store.WorkItem) {
	t.Helper()
	if err := s.Insert(context.Background(), items); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func claim(t *testing.T, s store.IWorkItemStore, limit int) []*store.WorkItem {
	t.Helper()
	got, err := s.ClaimPending(context.Background(), task, limit)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	return got
}

func get(t *testing.T, s store.IWorkItemStore, id string) *store.WorkItem {
	t.Helper()
	it, err := s.GetById(context.Background(), id)
	if err != nil {
		t.Fatalf("GetById(%s): %v", id, err)
	}
	return it
}

func ids(items []*store.WorkItem) map[string]bool {
	m := map[string]bool{}
	for _, it := range items {
		m[it.Id] = true
	}
	return m
}

func near(t *testing.T, what string, got *time.Time, want time.Time) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: nil, atteso ~%s", what, want)
	}
	if d := got.Sub(want); d < -tolerance || d > tolerance {
		t.Fatalf("%s = %s, atteso ~%s (scarto %s)", what, got, want, d)
	}
}

func claimSoloScaduti(t *testing.T, s store.IWorkItemStore) {
	insert(t, s, item("senza-scadenza", nil), item("scaduto", at(-time.Hour)), item("futuro", at(time.Hour)))

	got := claim(t, s, 10)
	if m := ids(got); len(got) != 2 || !m["senza-scadenza"] || !m["scaduto"] {
		t.Fatalf("claimati %v, attesi senza-scadenza e scaduto", m)
	}
	for _, it := range got {
		if it.Status != store.StatusInProgress || it.LockToken == "" || it.LockedBy == "" {
			t.Fatalf("item %s ritornato senza i campi del claim: %+v", it.Id, it)
		}
		if it.LockToken != got[0].LockToken {
			t.Fatalf("un claim ha un solo token: %s e %s", it.LockToken, got[0].LockToken)
		}
		near(t, "lockedAt ritornato", it.LockedAt, time.Now())
		// Ciò che il claim ritorna è ciò che ha scritto.
		stored := get(t, s, it.Id)
		if stored.Status != store.StatusInProgress || stored.LockToken != it.LockToken {
			t.Fatalf("item %s su disco: %+v", it.Id, stored)
		}
		near(t, "lockedAt su disco", stored.LockedAt, *it.LockedAt)
	}
	if st := get(t, s, "futuro").Status; st != store.StatusPending {
		t.Fatalf("l'item futuro è %s, atteso PENDING", st)
	}
}

func claimUnaVoltaSola(t *testing.T, s store.IWorkItemStore) {
	insert(t, s, item("a", nil))
	if got := claim(t, s, 10); len(got) != 1 {
		t.Fatalf("primo claim: %d item, atteso 1", len(got))
	}
	if got := claim(t, s, 10); len(got) != 0 {
		t.Fatalf("un item IN_PROGRESS è stato claimato di nuovo: %v", ids(got))
	}
}

func claimRispettaIlLimite(t *testing.T, s store.IWorkItemStore) {
	for i := range 5 {
		insert(t, s, item(fmt.Sprintf("i%d", i), at(-time.Duration(5-i)*time.Minute)))
	}
	got := claim(t, s, 2)
	// Vanno per primi i più scaduti.
	if m := ids(got); len(got) != 2 || !m["i0"] || !m["i1"] {
		t.Fatalf("claimati %v, attesi i due più scaduti (i0, i1)", m)
	}
	if got := claim(t, s, 10); len(got) != 3 {
		t.Fatalf("secondo claim: %d item, attesi i 3 rimasti", len(got))
	}
}

func markFencing(t *testing.T, s store.IWorkItemStore) {
	ctx := context.Background()
	insert(t, s, item("done", nil), item("failed", nil))
	got := claim(t, s, 10)
	token := got[0].LockToken

	// Un token che non è quello del claim non finalizza nulla, e non è un errore.
	if err := s.MarkDone(ctx, []string{"done"}, "altro-token"); err != nil {
		t.Fatalf("MarkDone con token stale: %v", err)
	}
	if err := s.MarkFailed(ctx, "failed", "altro-token", "no"); err != nil {
		t.Fatalf("MarkFailed con token stale: %v", err)
	}
	for _, id := range []string{"done", "failed"} {
		if st := get(t, s, id).Status; st != store.StatusInProgress {
			t.Fatalf("%s finalizzato da un token stale: %s", id, st)
		}
	}

	if err := s.MarkDone(ctx, []string{"done"}, token); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}
	// Un motivo che comincia con `$` è testo, non un'espressione: su Mongo l'update a pipeline lo
	// leggerebbe come un campo.
	if err := s.MarkFailed(ctx, "failed", token, "$boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	done, failed := get(t, s, "done"), get(t, s, "failed")
	if done.Status != store.StatusDone || done.LockedAt != nil || done.ExecutedBy == "" {
		t.Fatalf("done: %+v", done)
	}
	near(t, "updateTime di done", done.UpdateTime, time.Now())
	if failed.Status != store.StatusFailed || failed.Error != "$boom" || failed.LockedAt != nil {
		t.Fatalf("failed: %+v", failed)
	}

	// Idempotenti: un secondo Mark* su un item terminale non lo tocca.
	if err := s.MarkFailed(ctx, "done", token, "tardi"); err != nil {
		t.Fatalf("MarkFailed su item DONE: %v", err)
	}
	if st := get(t, s, "done").Status; st != store.StatusDone {
		t.Fatalf("un item DONE è diventato %s", st)
	}
}

func markPendingRitardaEConta(t *testing.T, s store.IWorkItemStore) {
	ctx := context.Background()
	insert(t, s, item("a", nil))
	token := claim(t, s, 10)[0].LockToken
	if err := s.MarkFailed(ctx, "a", "altro-token", "ignorato"); err != nil {
		t.Fatal(err)
	}

	if err := s.MarkPending(ctx, "a", token, time.Hour); err != nil {
		t.Fatalf("MarkPending: %v", err)
	}
	a := get(t, s, "a")
	if a.Status != store.StatusPending || a.Retry != 1 || a.LockedAt != nil || a.Error != "" {
		t.Fatalf("dopo MarkPending: %+v", a)
	}
	near(t, "nextRunAt", a.NextRunAt, time.Now().Add(time.Hour))
	if got := claim(t, s, 10); len(got) != 0 {
		t.Fatalf("claimato un item rimandato di un'ora: %v", ids(got))
	}

	// Ritardo zero: di nuovo claimabile subito.
	insert(t, s, item("b", nil))
	token = claim(t, s, 10)[0].LockToken
	if err := s.MarkPending(ctx, "b", token, 0); err != nil {
		t.Fatalf("MarkPending: %v", err)
	}
	if got := claim(t, s, 10); len(got) != 1 || got[0].Id != "b" {
		t.Fatalf("un item rimandato di 0 non è stato claimato: %v", ids(got))
	}
}

func releaseNonConta(t *testing.T, s store.IWorkItemStore) {
	ctx := context.Background()
	insert(t, s, item("a", nil))
	token := claim(t, s, 10)[0].LockToken

	if err := s.Release(ctx, "a", "altro-token"); err != nil {
		t.Fatalf("Release con token stale: %v", err)
	}
	if st := get(t, s, "a").Status; st != store.StatusInProgress {
		t.Fatalf("rilasciato da un token stale: %s", st)
	}
	if err := s.Release(ctx, "a", token); err != nil {
		t.Fatalf("Release: %v", err)
	}
	a := get(t, s, "a")
	if a.Status != store.StatusPending || a.Retry != 0 || a.LockedAt != nil || a.ExecutedBy != "" {
		t.Fatalf("dopo Release: %+v", a)
	}
	near(t, "nextRunAt", a.NextRunAt, time.Now())
	if got := claim(t, s, 10); len(got) != 1 {
		t.Fatalf("un item rilasciato non è di nuovo claimabile: %v", ids(got))
	}
}

func recoverSoloOltreLaSoglia(t *testing.T, s store.IWorkItemStore) {
	ctx := context.Background()
	insert(t, s, item("a", nil), item("pending", nil))
	first := claim(t, s, 1)[0]

	got, err := s.RecoverOrphans(ctx, task, time.Hour, 10)
	if err != nil {
		t.Fatalf("RecoverOrphans: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("recuperato un item claimato adesso con soglia di un'ora: %v", ids(got))
	}

	time.Sleep(20 * time.Millisecond)
	got, err = s.RecoverOrphans(ctx, task, 10*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("RecoverOrphans: %v", err)
	}
	// Un item PENDING non è un orfano, qualunque sia la soglia.
	if len(got) != 1 || got[0].Id != first.Id {
		t.Fatalf("recuperati %v, atteso solo %s", ids(got), first.Id)
	}
	r := got[0]
	if r.Status != store.StatusInProgress || r.Retry != 1 || r.LockToken == first.LockToken {
		t.Fatalf("orfano recuperato: %+v (token del claim: %s)", r, first.LockToken)
	}
	if !r.LockedAt.After(*first.LockedAt) {
		t.Fatalf("lockedAt non rinfrescato: %s, era %s", r.LockedAt, first.LockedAt)
	}
	stored := get(t, s, r.Id)
	if stored.Retry != 1 || stored.LockToken != r.LockToken {
		t.Fatalf("orfano su disco: %+v", stored)
	}
}

func recoverFencing(t *testing.T, s store.IWorkItemStore) {
	ctx := context.Background()
	insert(t, s, item("a", nil))
	old := claim(t, s, 10)[0].LockToken
	time.Sleep(20 * time.Millisecond)
	got, err := s.RecoverOrphans(ctx, task, 10*time.Millisecond, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("RecoverOrphans: %v %v", ids(got), err)
	}

	// Il worker del primo claim arriva tardi: non può chiudere l'esecuzione di chi l'ha recuperato.
	if err := s.MarkDone(ctx, []string{"a"}, old); err != nil {
		t.Fatal(err)
	}
	if st := get(t, s, "a").Status; st != store.StatusInProgress {
		t.Fatalf("finalizzato dal token del claim precedente: %s", st)
	}
	if err := s.MarkDone(ctx, []string{"a"}, got[0].LockToken); err != nil {
		t.Fatal(err)
	}
	if st := get(t, s, "a").Status; st != store.StatusDone {
		t.Fatalf("il token del recupero non finalizza: %s", st)
	}
}

// Un item IN_PROGRESS senza lockedAt non ha un lease da far scadere: se non valesse come scaduto
// nessun claim lo riprenderebbe più.
func recoverLeaseMalformato(t *testing.T, s store.IWorkItemStore) {
	it := item("a", nil)
	it.Status = store.StatusInProgress
	insert(t, s, it)
	got, err := s.RecoverOrphans(context.Background(), task, time.Hour, 10)
	if err != nil {
		t.Fatalf("RecoverOrphans: %v", err)
	}
	if len(got) != 1 || got[0].LockToken == "" {
		t.Fatalf("lease malformato non recuperato: %v", ids(got))
	}
	near(t, "lockedAt", got[0].LockedAt, time.Now())
}

func insertIfNotActive(t *testing.T, s store.IWorkItemStore) {
	ctx := context.Background()
	mk := func(id, objectId string) *store.WorkItem {
		it := item(id, nil)
		it.ObjectId = objectId
		return it
	}
	n, err := s.InsertIfNotActive(ctx, []*store.WorkItem{mk("a1", "obj-a"), mk("b1", "obj-b")})
	if err != nil || n != 2 {
		t.Fatalf("primo accodamento: %d, %v", n, err)
	}
	// Stesso oggetto ancora attivo (PENDING, poi IN_PROGRESS): non si accoda.
	if n, err = s.InsertIfNotActive(ctx, []*store.WorkItem{mk("a2", "obj-a")}); err != nil || n != 0 {
		t.Fatalf("accodato un duplicato attivo: %d, %v", n, err)
	}
	got := claim(t, s, 10)
	if n, err = s.InsertIfNotActive(ctx, []*store.WorkItem{mk("a3", "obj-a")}); err != nil || n != 0 {
		t.Fatalf("accodato un duplicato IN_PROGRESS: %d, %v", n, err)
	}
	// Finito il primo, lo stesso oggetto si può riaccodare.
	var token string
	for _, it := range got {
		token = it.LockToken
	}
	if err := s.MarkDone(ctx, []string{"a1", "b1"}, token); err != nil {
		t.Fatal(err)
	}
	if n, err = s.InsertIfNotActive(ctx, []*store.WorkItem{mk("a4", "obj-a")}); err != nil || n != 1 {
		t.Fatalf("riaccodamento dopo DONE: %d, %v", n, err)
	}
}
