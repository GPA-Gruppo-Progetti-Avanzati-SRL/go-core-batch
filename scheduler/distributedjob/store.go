package distributedjob

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// IQueryStore fetches object IDs from an external source (any table or collection).
// Used by RegisterByQuery to populate workitems before the claiming phase.
// Implementations live in distributedjob/mongostore and distributedjob/sqlstore.
//
// SICUREZZA: collection e sort sono identificatori (validati dalle implementazioni), mentre
// filter è una clausola di filtro GREZZA interpolata così com'è (WHERE SQL / query Mongo).
// collection/filter/sort devono provenire SOLO dalle Properties del job (config trusted,
// developer-authored) — MAI da input esterno o utente: filter non è parametrizzato.
type IQueryStore interface {
	GetIds(ctx context.Context, collection, filter string, limit int) ([]string, *core.ApplicationError)
	GetIdsSorted(ctx context.Context, collection, filter, sort string, limit int) ([]string, *core.ApplicationError)
}
