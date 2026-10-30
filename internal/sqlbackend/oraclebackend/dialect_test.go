package oraclebackend

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/oracledialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/schema"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// I test di questo file non toccano un database: verificano ciò che il dialetto decide prima di
// parlarci — quale database accetta e che SQL scrive.

// noConnector è un driver.Connector che non si connette mai: bun.NewDB non apre connessioni, e
// questi test non eseguono query.
type noConnector struct{}

func (noConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("nessun database")
}
func (noConnector) Driver() driver.Driver { return nil }

func newBunDB(t *testing.T, d schema.Dialect) *bun.DB {
	t.Helper()
	db := bun.NewDB(sql.OpenDB(noConnector{}), d)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// New rifiuta un database che non è Oracle: un'app che passa oraclestore.Module con un
// *coresql.Service su PostgreSQL deve fermarsi all'avvio, e il messaggio dice quale store usare.
func TestNew_SoloOracle(t *testing.T) {
	if _, err := New(newBunDB(t, oracledialect.New())); err != nil {
		t.Errorf("Oracle rifiutato: %v", err)
	}
	_, err := New(newBunDB(t, pgdialect.New()))
	if err == nil || err.Code != "BATCH-SQL-DIALECT" || !strings.Contains(err.Message, "store/sqlstore") {
		t.Fatalf("PostgreSQL accettato, o messaggio che non indica store/sqlstore: %v", err)
	}
}

// captureExec registra l'ultima INSERT invece di eseguirla.
type captureExec struct {
	query string
	args  []any
}

func (c *captureExec) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	c.query, c.args = q, args
	return nil, nil
}

// L'INSERT Oracle passa ogni valore come bind posizionale (il payload non diventa un letterale
// interpolato), gli istanti con l'offset dentro TO_TIMESTAMP_TZ, e i campi nullzero a zero come
// NULL — come li scrive bun sugli altri dialetti.
func TestOracle_InsertRow(t *testing.T) {
	o := oracleDialect{db: newBunDB(t, oracledialect.New())}
	created := time.Date(2026, 10, 30, 12, 0, 0, 0, time.FixedZone("CET", 3600))
	it := &store.WorkItem{
		Id: "a", TaskName: "t", ObjectId: "o", Status: store.StatusPending,
		CreateTime: created, Payload: map[string]any{"k": "v"},
	}
	var c captureExec
	if err := o.insertRow(context.Background(), &c, it); err != nil {
		t.Fatalf("insertRow: %v", err)
	}
	if !strings.HasPrefix(c.query, `INSERT INTO "work_items" (`) {
		t.Fatalf("INSERT inattesa: %s", c.query)
	}
	if strings.Contains(c.query, "'") && !strings.Contains(c.query, "TO_TIMESTAMP_TZ(") {
		t.Fatalf("un valore è finito nel testo della query: %s", c.query)
	}
	cols := strings.Split(c.query[strings.Index(c.query, "(")+1:strings.Index(c.query, ")")], ", ")
	if len(cols) != len(c.args) {
		t.Fatalf("%d colonne e %d bind", len(cols), len(c.args))
	}
	byCol := map[string]any{}
	for i, col := range cols {
		byCol[strings.Trim(col, `"`)] = c.args[i]
	}
	if got := byCol["create_time"]; got != "2026-10-30 12:00:00.000000 +01:00" {
		t.Errorf("create_time = %v: l'istante deve portare l'offset", got)
	}
	if !strings.Contains(c.query, "TO_TIMESTAMP_TZ(:") {
		t.Errorf("create_time non passa da TO_TIMESTAMP_TZ: %s", c.query)
	}
	if got := byCol["payload"]; got != `{"k":"v"}` {
		t.Errorf("payload = %v, atteso il suo JSON", got)
	}
	for _, col := range []string{"locked_at", "next_run_at", "lock_token", "error"} {
		if got, ok := byCol[col]; !ok || got != nil {
			t.Errorf("%s = %v, atteso NULL", col, got)
		}
	}
	if got := byCol["retry"]; got != 0 {
		t.Errorf("retry = %v, atteso 0 (non è nullzero)", got)
	}
}

// Il motivo di MarkFailed passa dal builder di bun come letterale: su Oracle va contenuto sotto i
// 4000 byte, senza spezzare un carattere multibyte.
func TestOracle_FitText(t *testing.T) {
	o := oracleDialect{}
	if got := o.FitText("breve"); got != "breve" {
		t.Errorf("testo breve modificato: %q", got)
	}
	got := o.FitText(strings.Repeat("è", 4000)) // 2 byte a carattere
	if len(got) > oracleMaxLiteral || !utf8.ValidString(got) {
		t.Errorf("troncato a %d byte, valido=%v", len(got), utf8.ValidString(got))
	}
}

// Il DDL Oracle deve creare ciò che la verifica di avvio pretende, come quello PostgreSQL.
func TestOracle_DDLCreaTuttiGliIndiciAttesi(t *testing.T) {
	for _, nome := range append(store.ExpectedIndexes, store.IndexTaskLogPurge) {
		if !strings.Contains(oracleIndexesDDL, nome) {
			t.Errorf("il DDL Oracle non crea %q", nome)
		}
	}
	for _, col := range []string{"lock_token", "locked_by", "executed_by"} {
		if !strings.Contains(oracleColumnsDDL, `"`+col+`"`) {
			t.Errorf("il DDL Oracle non aggiunge la colonna %q", col)
		}
	}
}

// La tabella Oracle è creata dalla libreria, non dal modello: deve avere tutte le colonne che il
// modello bun scrive, altrimenti il primo INSERT fallisce a regime invece che nei test.
func TestOracle_DDLHaTutteLeColonneDelModello(t *testing.T) {
	db := newBunDB(t, oracledialect.New())
	for _, model := range []any{(*store.WorkItem)(nil), (*store.TaskLog)(nil)} {
		table := db.Table(reflect.TypeOf(model).Elem())
		for _, f := range table.Fields {
			if !strings.Contains(oracleTablesDDL, `"`+f.Name+`"`) {
				t.Errorf("%s: colonna %q assente dal DDL Oracle", table.Name, f.Name)
			}
		}
	}
	for _, s := range sqlbackend.DDLStatements(oracleTablesDDL + oracleColumnsDDL + oracleIndexesDDL) {
		if strings.Contains(s, ";") {
			t.Errorf("istruzione con più statement: %q", s)
		}
	}
}
