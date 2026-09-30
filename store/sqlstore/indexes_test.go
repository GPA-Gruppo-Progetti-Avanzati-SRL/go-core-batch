package sqlstore

import (
	"reflect"
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

// Le colonne che la libreria scrive ma che una tabella creata prima non ha: se EnsureIndexes non
// le aggiunge, il primo Mark* fallisce con un errore del database invece che all'avvio. Sono
// dedotte dal modello (i tag bun di store.WorkItem), non elencate a mano, così una colonna nuova
// sul WorkItem che nessuno aggiunge al DDL fa fallire questo test.
func TestEnsureIndexes_AggiungeLeColonneDelFencing(t *testing.T) {
	for _, col := range []string{"lock_token", "locked_by", "executed_by"} {
		if !strings.Contains(ensureColumnsDDL, col) {
			t.Errorf("EnsureIndexes non aggiunge la colonna %q: su una tabella preesistente il primo "+
				"Mark* fallirebbe con un errore del database", col)
		}
		if !strings.Contains(modelloWorkItem(), col) {
			t.Errorf("la colonna %q non esiste sul modello store.WorkItem", col)
		}
	}
}

// modelloWorkItem ritorna i nomi di colonna dichiarati dai tag bun di store.WorkItem.
func modelloWorkItem() string {
	t := reflect.TypeFor[store.WorkItem]()
	var cols []string
	for i := range t.NumField() {
		tag, _, _ := strings.Cut(t.Field(i).Tag.Get("bun"), ",")
		cols = append(cols, tag)
	}
	return strings.Join(cols, " ")
}

// Il DDL si esegue un'istruzione alla volta: ogni istruzione è una sola, senza `;` residui, e c'è
// anche l'indice di retention di task_logs.
func TestEnsureIndexes_IstruzioniSingole(t *testing.T) {
	stmts := ddlStatements(ensureColumnsDDL + ensureIndexesDDL)
	if len(stmts) != 8 {
		t.Fatalf("attese 8 istruzioni (3 ADD COLUMN + 5 indici), trovate %d: %q", len(stmts), stmts)
	}
	for _, s := range stmts {
		if strings.Contains(s, ";") {
			t.Errorf("istruzione con più statement: %q", s)
		}
	}
	if !strings.Contains(ensureIndexesDDL, store.IndexTaskLogPurge) {
		t.Error("manca l'indice di retention di task_logs")
	}
}
