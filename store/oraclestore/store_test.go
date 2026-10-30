package oraclestore

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	_ "github.com/sijms/go-ora/v2"
	"github.com/uptrace/bun/dialect/oracledialect"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store/storetest"
)

// Lo store Oracle si verifica solo contro un Oracle vero: ORACLE_URL, es.
// oracle://system:pw@localhost:1521/XEPDB1. La versione minima supportata è la 19c, quindi il
// database di prova non deve essere una 23ai, che accetta sintassi che la 19c rifiuta.
//
// Il fixture passa da coresql.Module e dal registry di go-core-app, che è uno per processo: con
// anche PG_URL impostata la seconda coresql.Module darebbe un duplicate provide. Le due suite si
// lanciano quindi una alla volta.
var oracleService = sync.OnceValues(func() (*coresql.Service, error) {
	var svc *coresql.Service
	coresql.Module(&coresql.Config{Driver: "oracle", DSN: os.Getenv("ORACLE_URL")}, oracledialect.New())
	core.Populate(&svc)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := core.Start(ctx); err != nil {
		return nil, err
	}
	// Due volte: la seconda prova l'idempotenza per codice d'errore, che su 19c sostituisce
	// IF NOT EXISTS.
	for range 2 {
		if err := EnsureIndexes(ctx, svc.DB()); err != nil {
			return nil, err
		}
	}
	return svc, nil
})

func oracleFixture(t *testing.T) *coresql.Service {
	t.Helper()
	if os.Getenv("ORACLE_URL") == "" {
		t.Skip("ORACLE_URL non impostata: il ramo Oracle dello store SQL richiede un Oracle")
	}
	if os.Getenv("PG_URL") != "" {
		t.Skip("PG_URL e ORACLE_URL insieme: le due suite vanno lanciate una alla volta")
	}
	svc, err := oracleService()
	if err != nil {
		t.Fatalf("Oracle: %v", err)
	}
	for _, table := range []string{`"work_items"`, `"task_logs"`} {
		if _, err := svc.DB().ExecContext(context.Background(), "TRUNCATE TABLE "+table); err != nil {
			t.Fatalf("TRUNCATE %s: %v", table, err)
		}
	}
	return svc
}

func newOracleStore(t *testing.T) store.IWorkItemStore {
	t.Helper()
	st, err := newWorkItemStore(oracleFixture(t))
	if err != nil {
		t.Fatalf("newWorkItemStore: %v", err)
	}
	return st
}

func TestStore_Oracle(t *testing.T) {
	storetest.Run(t, newOracleStore)
}

// Il payload si scrive con un bind e non come letterale interpolato da bun: un letterale Oracle
// oltre i 4000 byte fallisce con ORA-01704. E torna indietro intero, decodificabile.
func TestStore_Oracle_PayloadLungo(t *testing.T) {
	s := newOracleStore(t)
	ctx := context.Background()
	lungo := strings.Repeat("x", 20_000)
	it := &store.WorkItem{
		Id: "big", TaskName: "storetest", ObjectId: "big", Status: store.StatusPending,
		CreateTime: time.Now(), Payload: map[string]any{"testo": lungo},
	}
	if err := s.Insert(ctx, []*store.WorkItem{it}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, err := s.GetById(ctx, "big")
	if err != nil {
		t.Fatalf("GetById: %v", err)
	}
	m, ok := store.PayloadMap(got.Payload)
	if !ok || m["testo"] != lungo {
		t.Fatalf("payload non restituito intero (%T)", got.Payload)
	}
}

// Gli istanti scritti da Go tornano gli stessi istanti, qualunque sia il fuso del processo: le
// colonne sono TIMESTAMP WITH TIME ZONE e l'INSERT porta l'offset.
func TestStore_Oracle_IstantiDaGo(t *testing.T) {
	s := newOracleStore(t)
	ctx := context.Background()
	roma, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Skip("tzdata non disponibile")
	}
	created := time.Now().In(roma).Truncate(time.Millisecond)
	it := &store.WorkItem{
		Id: "tz", TaskName: "storetest", ObjectId: "tz", Status: store.StatusPending, CreateTime: created,
	}
	if err := s.Insert(ctx, []*store.WorkItem{it}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, appErr := s.GetById(ctx, "tz")
	if appErr != nil {
		t.Fatalf("GetById: %v", appErr)
	}
	if !got.CreateTime.Equal(created) {
		t.Fatalf("create_time = %s, scritto %s: l'istante non è sopravvissuto", got.CreateTime, created)
	}
}

func TestBatchData_Oracle_TaskLog(t *testing.T) {
	svc := oracleFixture(t)
	d, err := newData(sqlbackend.DataParams{Sql: svc})
	if err != nil {
		t.Fatalf("newData: %v", err)
	}
	ctx := context.Background()
	vecchio := time.Now().Add(-48 * time.Hour)
	d.InsertTaskLogs(ctx, []*store.TaskLog{
		{TaskID: "1", JobID: "j", TaskName: "t", Stato: "DONE", Logdate: vecchio, Error: strings.Repeat("e", 10_000)},
		{TaskID: "2", JobID: "j", TaskName: "t", Stato: "DONE", Logdate: time.Now()},
	})
	n, appErr := d.PurgeTaskLogs(ctx, time.Now().Add(-24*time.Hour), 100)
	if appErr != nil {
		t.Fatalf("PurgeTaskLogs: %v", appErr)
	}
	if n != 1 {
		t.Fatalf("cancellate %d righe, attesa 1 (solo quella più vecchia di 24h)", n)
	}
}
