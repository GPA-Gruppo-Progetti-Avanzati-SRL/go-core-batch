package mongostore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

// Module fornisce le implementazioni MongoDB di store.IData e store.IWorkItemStore.
// I costruttori concreti non sono esportati: l'unico entry-point è Module()/ModuleIf().
// Richiede via fx un *mongolks.LinkedService (fornito dall'app tramite coremongo.NewService).
func Module() {
	core.Provides(
		fx.Annotate(newBatchData, fx.As(new(store.IData))),
		fx.Annotate(newWorkItemData, fx.As(new(store.IWorkItemStore))),
	)
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvidesIf(fx.Annotate(newBatchData, fx.As(new(store.IData))), modes...)
	core.ProvidesIf(fx.Annotate(newWorkItemData, fx.As(new(store.IWorkItemStore))), modes...)
}
