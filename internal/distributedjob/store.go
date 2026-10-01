package distributedjob

import (
	"context"
	"strings"

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
//
// Un metodo solo: c'erano GetIds e GetIdsSorted, ma la prima era letteralmente la seconda con
// sort vuoto — due implementazioni identiche per backend — e l'unico chiamante ramificava su
// `sort != ""` per scegliere quale chiamare, cioè rifaceva a mano ciò che l'alias già faceva.
// `sort` vuoto significa "nessun ordinamento", e lo sa dire la stessa firma.
type IQueryStore interface {
	GetIds(ctx context.Context, collection, filter, sort string, limit int) ([]string, *core.Error)
}

// SortField è una voce dell'ordinamento richiesto dal feed, nella forma `colonna[:desc]`.
type SortField struct {
	Column string
	Desc   bool
}

// ParseSort interpreta la property `sort` di un job by-query: una lista separata da virgole di
// `colonna` o `colonna:desc`. Una voce vuota viene saltata.
//
// Sta qui e non nelle implementazioni perché la grammatica è dell'INTERFACCIA, non del backend: i
// due query store la parsavano ciascuno per conto proprio, con due copie dello stesso ciclo, e
// nulla garantiva che `sort` significasse la stessa cosa passando da Mongo a SQL. Cosa farne
// dopo — un bson.D o un ORDER BY con l'identificatore validato — resta di chi implementa.
func ParseSort(sort string) []SortField {
	var out []SortField
	for part := range strings.SplitSeq(sort, ",") {
		campo, dir, _ := strings.Cut(strings.TrimSpace(part), ":")
		campo = strings.TrimSpace(campo)
		if campo == "" {
			continue
		}
		out = append(out, SortField{Column: campo, Desc: strings.EqualFold(strings.TrimSpace(dir), "desc")})
	}
	return out
}
