package sqlstore

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
)

// Module fornisce l'implementazione SQL (bun) di distributedjob.IQueryStore (feed by query).
// Il costruttore concreto non è esportato: l'unico entry-point è Module().
// Richiede via fx un *bun.DB (fornito da coresql.Module insieme al *coresql.Service):
// questo query store costruisce solo query bun native, quindi non passa dai CRUD generici.
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(modes ...string) {
	core.ProvideAs[distributedjob.IQueryStore](newQueryDataSQL, modes...)
}
