package sqlstore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"go.uber.org/fx"
)

// Module fornisce l'implementazione SQL (bun) di distributedjob.IQueryStore (feed by query).
// Il costruttore concreto non è esportato: l'unico entry-point è Module()/ModuleIf().
// Richiede via fx un *bun.DB (fornito dall'app tramite coresql.NewService).
func Module() {
	core.Provides(fx.Annotate(newQueryDataSQL, fx.As(new(distributedjob.IQueryStore))))
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvidesIf(fx.Annotate(newQueryDataSQL, fx.As(new(distributedjob.IQueryStore))), modes...)
}
