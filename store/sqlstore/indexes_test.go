package sqlstore

import (
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// EnsureIndexes CREA gli indici con un DDL scritto a mano, mentre warnIfIndexesMissing segnala
// l'assenza di quelli che store.ExpectedIndexes elenca. Sono due liste che devono coincidere: se
// il DDL ne crea uno con un altro nome, la verifica lo segnalerebbe come mancante a ogni avvio —
// e se ne dimentica uno, la verifica non se ne accorge. Il DDL resta un literal perché si legge,
// ma il suo contenuto è vincolato qui.
func TestEnsureIndexes_CreaTuttiGliIndiciAttesi(t *testing.T) {
	ddl := ensureIndexesDDL
	for _, nome := range store.ExpectedIndexes {
		if !strings.Contains(ddl, nome) {
			t.Errorf("il DDL di EnsureIndexes non crea %q, che store.ExpectedIndexes pretende: "+
				"la verifica di avvio lo segnalerebbe assente per sempre", nome)
		}
	}
}
