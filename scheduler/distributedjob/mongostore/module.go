package mongostore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"go.uber.org/fx"
)

// Module fornisce l'implementazione MongoDB di distributedjob.IQueryStore (feed by query).
// Il costruttore concreto non è esportato: l'unico entry-point è Module()/ModuleIf().
// Richiede via fx un *mongolks.LinkedService (fornito dall'app tramite coremongo.NewService).
func Module() {
	core.Provides(fx.Annotate(newQueryData, fx.As(new(distributedjob.IQueryStore))))
}

// ModuleIf è come Module ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvideIf(fx.Annotate(newQueryData, fx.As(new(distributedjob.IQueryStore))), modes...)
}
