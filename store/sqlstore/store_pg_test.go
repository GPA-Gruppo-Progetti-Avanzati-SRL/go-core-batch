package sqlstore

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/uptrace/bun/dialect/pgdialect"
	_ "github.com/uptrace/bun/driver/pgdriver"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store/storetest"
)

// Lo store è PostgreSQL-specifico (FOR UPDATE SKIP LOCKED, NULLS FIRST, gli indici parziali), quindi
// la suite gira solo contro un Postgres vero: PG_URL, es.
// postgres://postgres:pg@localhost:5432/batch?sslmode=disable. Senza, i test sono saltati.
var pgService = sync.OnceValues(func() (*coresql.Service, error) {
	var svc *coresql.Service
	coresql.Module(&coresql.Config{Driver: "pg", DSN: os.Getenv("PG_URL")}, pgdialect.New())
	core.Populate(&svc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := core.Start(ctx); err != nil {
		return nil, err
	}
	db := svc.DB()
	for _, model := range []any{(*store.WorkItem)(nil), (*store.TaskLog)(nil)} {
		if _, err := db.NewCreateTable().Model(model).IfNotExists().Exec(ctx); err != nil {
			return nil, err
		}
	}
	if err := EnsureIndexes(ctx, db); err != nil {
		return nil, err
	}
	return svc, nil
})

func newPgStore(t *testing.T) store.IWorkItemStore {
	t.Helper()
	if os.Getenv("PG_URL") == "" {
		t.Skip("PG_URL non impostata: la suite dello store SQL richiede un PostgreSQL")
	}
	svc, err := pgService()
	if err != nil {
		t.Fatalf("PostgreSQL: %v", err)
	}
	if _, err := svc.DB().ExecContext(context.Background(), "TRUNCATE work_items"); err != nil {
		t.Fatalf("TRUNCATE: %v", err)
	}
	return newWorkItemDataSQL(svc)
}

func TestStore_Postgres(t *testing.T) {
	storetest.Run(t, newPgStore)
}
