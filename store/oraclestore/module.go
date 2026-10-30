// Package oraclestore è lo store di go-core-batch su Oracle (19c+): le implementazioni di
// store.IData e store.IWorkItemStore, da passare a batch.WithStore. È il gemello di store/sqlstore
// (PostgreSQL), con cui non condivide import: un'app Oracle non compila il codice Postgres.
//
// Il driver lo sceglie l'app (go-ora, puro Go, o godror) insieme al dialetto:
//
//	coresql.Module(&svc.Sql, oracledialect.New())   // + import _ "github.com/sijms/go-ora/v2"
//	batch.Module(&svc.Batch, Register, batch.WithStore(oraclestore.Module), ...)
package oraclestore

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/uptrace/bun"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend/oraclebackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Module fornisce le implementazioni Oracle di store.IData e store.IWorkItemStore.
// Richiede via fx un *coresql.Service su Oracle: un altro dialetto ferma l'avvio con
// BATCH-SQL-DIALECT. Se modes è vuoto registra sempre; altrimenti solo nei modes indicati.
func Module(modes ...string) {
	core.ProvideAs[store.IData](newData, modes...)
	core.ProvideAs[store.IWorkItemStore](newWorkItemStore, modes...)
}

func newData(p sqlbackend.DataParams) (*sqlbackend.Data, error) {
	return sqlbackend.NewData(p, oraclebackend.New)
}

func newWorkItemStore(svc *coresql.Service) (*sqlbackend.WorkItemStore, error) {
	return sqlbackend.NewWorkItemStore(svc, oraclebackend.New)
}

// EnsureIndexes crea tabelle, colonne e indici dello store. Chiamarla una volta all'avvio.
//
// A differenza di PostgreSQL crea ANCHE le due tabelle: su Oracle CreateTable di bun non è
// usabile (emette IF NOT EXISTS, che arriva con la 23ai, e il tipo jsonb del modello). Le colonne
// temporali sono TIMESTAMP WITH TIME ZONE, il payload un CLOB, gli identificatori minuscoli
// quotati ("work_items") — quelli che bun scrive dal modello: una tabella creata a mano deve
// usare gli stessi. Oracle non ha indici parziali: uk_workitem_active è function-based, gli altri
// sono composti pieni con gli stessi nomi. È idempotente per codice d'errore (ORA-00955,
// ORA-01408, ORA-01430), perché fino alla 19c IF NOT EXISTS non c'è. Su un database che non è
// Oracle ritorna BATCH-SQL-DIALECT.
func EnsureIndexes(ctx context.Context, db *bun.DB) error {
	return sqlbackend.EnsureIndexes(ctx, db, oraclebackend.New)
}
