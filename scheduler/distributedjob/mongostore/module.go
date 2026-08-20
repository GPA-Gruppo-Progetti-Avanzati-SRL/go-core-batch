package mongostore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
)

// Module fornisce l'implementazione MongoDB di distributedjob.IQueryStore (feed by query).
// Il costruttore concreto non è esportato: l'unico entry-point è Module().
// Richiede via fx un *coremongo.Service (fornito da coremongo.Module).
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.ProvideAs[distributedjob.IQueryStore](newQueryData, modes...)
}
