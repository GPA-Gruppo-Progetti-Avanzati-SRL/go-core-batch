// Package sqlstore è lo store di go-core-batch su PostgreSQL: le implementazioni di store.IData e
// store.IWorkItemStore, da passare a batch.WithStore. Per Oracle c'è store/oraclestore: i due
// package non si importano a vicenda, quindi un'app Postgres non compila il codice Oracle.
package sqlstore

import (
	"context"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/uptrace/bun"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend/pgbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Module fornisce le implementazioni PostgreSQL di store.IData e store.IWorkItemStore.
// I costruttori concreti non sono esportati: l'unico entry-point è Module().
// Richiede via fx un *coresql.Service (fornito dall'app tramite coresql.Module) su PostgreSQL:
// un altro dialetto ferma l'avvio con BATCH-SQL-DIALECT.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.ProvideAs[store.IData](newData, modes...)
	core.ProvideAs[store.IWorkItemStore](newWorkItemStore, modes...)
}

func newData(p sqlbackend.DataParams) (*sqlbackend.Data, error) {
	return sqlbackend.NewData(p, pgbackend.New)
}

func newWorkItemStore(svc *coresql.Service) (*sqlbackend.WorkItemStore, error) {
	return sqlbackend.NewWorkItemStore(svc, pgbackend.New)
}

// EnsureIndexes crea colonne e indici richiesti dallo store. Chiamarla una volta all'avvio.
// Include:
//   - le colonne di fencing lock_token/locked_by, usate da ClaimPending/RecoverOrphans/Mark* per
//     impedire che un worker stale finalizzi un item ri-claimato, più executed_by, che i Mark*
//     riempiono con l'hostname di CHI HA ESEGUITO (diverso da locked_by, che è di chi ha claimato,
//     quando il dispatch passa per gRPC);
//   - uk_workitem_active, unico parziale, che impedisce l'inserimento concorrente di item attivi
//     duplicati per lo stesso (task_name, object_id);
//   - ix_workitem_claim / ix_workitem_orphan, che servono le query di claim e recupero orfani
//     eseguite da ogni job a ogni tick. Sono parziali sugli stati ATTIVI: gli item DONE/FAILED non
//     vengono mai claimati, quindi tenerli fuori mantiene l'indice della dimensione del LAVORO e
//     non dello storico;
//   - ix_workitem_purge, che serve la query del job PurgeWorkItems ed è parziale sugli stati
//     TERMINALI per la ragione speculare: la retention lavora solo lì.
//
// Le tabelle restano dell'applicazione (migration, o CreateTable di bun dal modello). Su un
// database che non è PostgreSQL ritorna BATCH-SQL-DIALECT.
func EnsureIndexes(ctx context.Context, db *bun.DB) error {
	return sqlbackend.EnsureIndexes(ctx, db, pgbackend.New)
}
