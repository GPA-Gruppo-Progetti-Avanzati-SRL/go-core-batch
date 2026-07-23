package mongostore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Module fornisce le implementazioni MongoDB di store.IData e store.IWorkItemStore.
// I costruttori concreti non sono esportati: l'unico entry-point è Module().
// Richiede via fx un *mongolks.LinkedService (fornito dall'app tramite coremongo.NewService).
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.ProvideAs[store.IData](newBatchData, modes...)
	core.ProvideAs[store.IWorkItemStore](newWorkItemData, modes...)
}
