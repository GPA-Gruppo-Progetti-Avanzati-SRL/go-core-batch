package sqlstore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Module fornisce le implementazioni SQL (bun) di store.IData e store.IWorkItemStore.
// I costruttori concreti non sono esportati: l'unico entry-point è Module().
// Richiede via fx un *bun.DB (fornito dall'app tramite coresql.NewService).
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.ProvideAs[store.IData](newBatchDataSQL, modes...)
	core.ProvideAs[store.IWorkItemStore](newWorkItemDataSQL, modes...)
}
