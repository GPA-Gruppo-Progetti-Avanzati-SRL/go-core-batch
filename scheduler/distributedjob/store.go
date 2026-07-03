package distributedjob

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// IQueryStore fetches object IDs from an external source (any table or collection).
// Used by RegisterByQuery to populate workitems before the claiming phase.
// Implementations live in distributedjob/mongostore and distributedjob/sqlstore.
type IQueryStore interface {
	GetIds(ctx context.Context, collection, filter string, limit int) ([]string, *core.ApplicationError)
	GetIdsSorted(ctx context.Context, collection, filter, sort string, limit int) ([]string, *core.ApplicationError)
}
