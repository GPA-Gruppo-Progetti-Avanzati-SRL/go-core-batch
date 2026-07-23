package sqlstore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

// Module fornisce le implementazioni SQL (bun) di store.IData e store.IWorkItemStore.
// I costruttori concreti non sono esportati: l'unico entry-point è Module()/ModuleIf().
// Richiede via fx un *bun.DB (fornito dall'app tramite coresql.NewService).
func Module() {
	core.Provides(
		fx.Annotate(newBatchDataSQL, fx.As(new(store.IData))),
		fx.Annotate(newWorkItemDataSQL, fx.As(new(store.IWorkItemStore))),
	)
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvideIf(fx.Annotate(newBatchDataSQL, fx.As(new(store.IData))), modes...)
	core.ProvideIf(fx.Annotate(newWorkItemDataSQL, fx.As(new(store.IWorkItemStore))), modes...)
}
