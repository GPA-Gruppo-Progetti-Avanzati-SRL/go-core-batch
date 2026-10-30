// Package oraclebackend è il Dialect Oracle (19c+) dello store SQL: lo importa store/oraclestore,
// e solo lui.
package oraclebackend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/rs/zerolog/log"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/schema"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/sqlbackend"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// oracleDialect è lo store su Oracle, dalla 19c.
//
// Tre cose lo separano dal ramo PostgreSQL, e sono tutte del database, non dello store:
//
//   - gli identificatori. bun quota tutto ciò che deduce dal modello, e su Oracle un nome quotato
//     è case-sensitive: lo schema ha quindi nomi minuscoli quotati ("work_items", "task_name"),
//     ed è quello che crea EnsureIndexes. Ogni query di questo file li quota.
//   - gli istanti. Le colonne sono TIMESTAMP WITH TIME ZONE e l'orologio è SYSTIMESTAMP, così un
//     istante resta un istante qualunque sia il fuso della sessione. bun scrive un time.Time come
//     TO_TIMESTAMP('<ora di parete>') e il fuso lo perde: gli istanti che arrivano da Go passano
//     quindi da TO_TIMESTAMP_TZ, con l'offset scritto.
//   - i valori lunghi. bun interpola gli argomenti nel testo SQL, e un letterale Oracle supera i
//     4000 byte solo per fallire (ORA-01704): payload e task log si scrivono con i bind del
//     driver, non col builder di bun.
type oracleDialect struct {
	db *bun.DB
}

// New è il sqlbackend.NewDialect di Oracle.
func New(db *bun.DB) (sqlbackend.Dialect, *core.Error) {
	if err := sqlbackend.CheckDialect(db, dialect.Oracle, "store/oraclestore"); err != nil {
		return nil, err
	}
	return oracleDialect{db: db}, nil
}

const (
	// oracleTSTZFormat e goTSTZLayout sono la stessa forma vista dai due lati: è il formato in cui
	// un istante di Go arriva a TO_TIMESTAMP_TZ senza perdere l'offset.
	oracleTSTZFormat = "YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM"
	goTSTZLayout     = "2006-01-02 15:04:05.000000 -07:00"

	// oracleMaxLiteral è il tetto, in byte, di un testo che passa dal builder di bun come
	// letterale: 4000 è il limite di Oracle, il margine copre gli apici raddoppiati dall'escape.
	oracleMaxLiteral = 3000
)

func (oracleDialect) Now() string     { return "SYSTIMESTAMP" }
func (oracleDialect) AfterMs() string { return "SYSTIMESTAMP + NUMTODSINTERVAL(? / 1000, 'SECOND')" }

// fitText tronca il motivo di MarkFailed a oracleMaxLiteral byte, senza spezzare una runa. Il
// testo intero resta nella riga di task_logs, che si scrive con un bind.
func (oracleDialect) FitText(s string) string {
	if len(s) <= oracleMaxLiteral {
		return s
	}
	s = s[:oracleMaxLiteral]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// tstz è l'espressione SQL dell'istante t, offset compreso.
func tstz(t time.Time) string {
	return "TO_TIMESTAMP_TZ('" + t.Format(goTSTZLayout) + "', '" + oracleTSTZFormat + "')"
}

// claim è una transazione in tre passi, perché Oracle non ha l'UPDATE … RETURNING multi-riga:
// blocca i candidati (FOR UPDATE SKIP LOCKED), li marca col token di questo claim, li rilegge per
// token. È la stessa forma del backend Mongo.
func (o oracleDialect) Claim(ctx context.Context, taskName string, limit int, token, host string) ([]*store.WorkItem, error) {
	return o.lockAndMark(ctx, limit, token,
		`SELECT "id" FROM "work_items"
		WHERE "task_name" = ? AND "status" = ? AND ("next_run_at" IS NULL OR "next_run_at" <= SYSTIMESTAMP)
		ORDER BY "next_run_at" ASC NULLS FIRST, "create_time" ASC
		FOR UPDATE SKIP LOCKED`,
		[]any{taskName, store.StatusPending},
		`UPDATE "work_items"
		SET "status" = ?, "locked_at" = SYSTIMESTAMP, "update_time" = SYSTIMESTAMP, "lock_token" = ?, "locked_by" = ?
		WHERE "id" IN (?)`,
		[]any{store.StatusInProgress, token, host},
		`ORDER BY "next_run_at" ASC NULLS FIRST, "create_time" ASC`,
	)
}

// recoverOrphans: la soglia è SYSTIMESTAMP - maxAge, l'età del lease misurata dall'orologio che
// l'ha scritto. Un IN_PROGRESS senza locked_at vale come scaduto.
func (o oracleDialect) RecoverOrphans(ctx context.Context, taskName string, maxAge time.Duration, limit int, token, host string) ([]*store.WorkItem, error) {
	return o.lockAndMark(ctx, limit, token,
		`SELECT "id" FROM "work_items"
		WHERE "task_name" = ? AND "status" = ?
		  AND ("locked_at" IS NULL OR "locked_at" < SYSTIMESTAMP - NUMTODSINTERVAL(? / 1000, 'SECOND'))
		ORDER BY "locked_at" ASC NULLS FIRST
		FOR UPDATE SKIP LOCKED`,
		[]any{taskName, store.StatusInProgress, maxAge.Milliseconds()},
		`UPDATE "work_items"
		SET "locked_at" = SYSTIMESTAMP, "lock_token" = ?, "locked_by" = ?, "retry" = "retry" + 1, "update_time" = SYSTIMESTAMP
		WHERE "id" IN (?)`,
		[]any{token, host},
		`ORDER BY "create_time" ASC`,
	)
}

// lockAndMark è il corpo comune di claim e recoverOrphans.
//
// Il limite NON sta nella SELECT: Oracle non ammette FETCH FIRST insieme a FOR UPDATE, e un
// ROWNUM verrebbe applicato prima dello SKIP LOCKED, restituendo meno righe del dovuto quando
// altre sono bloccate. Con SKIP LOCKED Oracle blocca le righe mentre le si legge dal cursore,
// quindi si legge fino a limit e si chiude. Il driver può prefetchare qualche riga in più, che
// resta bloccata (non modificata) fino al commit: un'altra replica la salta per la durata di
// questa transazione, cioè qualche millisecondo.
func (o oracleDialect) lockAndMark(
	ctx context.Context, limit int, token string,
	selectSQL string, selectArgs []any,
	updateSQL string, updateArgs []any,
	orderBy string,
) ([]*store.WorkItem, error) {
	var items []*store.WorkItem
	err := o.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		rows, err := tx.QueryContext(ctx, selectSQL, selectArgs...)
		if err != nil {
			return err
		}
		ids, err := firstIDs(rows, limit)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if _, err := tx.NewRaw(updateSQL, append(updateArgs, bun.List(ids))...).Exec(ctx); err != nil {
			return err
		}
		return tx.NewRaw(`SELECT * FROM "work_items" WHERE "lock_token" = ? AND "id" IN (?) `+orderBy,
			token, bun.List(ids)).Scan(ctx, &items)
	})
	return items, err
}

// firstIDs legge al più limit id dal cursore e lo chiude: la UPDATE successiva gira sulla stessa
// connessione, che finché il cursore è aperto non è libera.
func firstIDs(rows *sql.Rows, limit int) (ids []string, err error) {
	defer func() {
		cerr := rows.Close()
		switch {
		case cerr == nil:
		case err == nil:
			err = cerr
		default: // risale l'errore della lettura; quello della chiusura non va perso
			log.Warn().Err(cerr).Msg("go-core-batch: chiusura del cursore del claim")
		}
	}()
	for len(ids) < limit && rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// insert scrive tutti gli item o nessuno.
func (o oracleDialect) Insert(ctx context.Context, items []*store.WorkItem) *core.Error {
	if len(items) == 0 {
		return nil
	}
	err := o.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, it := range items {
			if err := o.insertRow(ctx, tx.Tx, it); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errs.Tech(errs.CodeInsert).WithCause(err)
	}
	return nil
}

// insertIfNotActive è un INSERT per item, senza transazione: è ciò che fa ON CONFLICT DO NOTHING
// riga per riga. ORA-00001 su uk_workitem_active è la deduplica, non un errore.
func (o oracleDialect) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, error) {
	inserted := 0
	for _, it := range items {
		err := o.insertRow(ctx, o.db.DB, it)
		if isOracleError(err, "ORA-00001") {
			continue
		}
		if err != nil {
			return inserted, err
		}
		inserted++
	}
	return inserted, nil
}

func (o oracleDialect) Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, error) {
	res, err := o.db.NewRaw(`
		DELETE FROM "work_items"
		WHERE "id" IN (
			SELECT "id" FROM "work_items"
			WHERE "status" = ? AND "update_time" < `+tstz(olderThan)+`
			ORDER BY "update_time" ASC
			FETCH FIRST ? ROWS ONLY
		)
	`, status, limit).Exec(ctx)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

func (o oracleDialect) InsertTaskLogs(ctx context.Context, logs []*store.TaskLog) *core.Error {
	err := o.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, tl := range logs {
			if err := o.insertRow(ctx, tx.Tx, tl); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return errs.Tech(errs.CodeInsert).WithCause(err)
	}
	return nil
}

// purgeTaskLogs: task_logs non ha chiave primaria, quindi il limit passa da ROWID.
func (o oracleDialect) PurgeTaskLogs(ctx context.Context, olderThan time.Time, limit int) (int, error) {
	res, err := o.db.NewRaw(`
		DELETE FROM "task_logs"
		WHERE ROWID IN (
			SELECT ROWID FROM "task_logs" WHERE "logdate" < `+tstz(olderThan)+`
			ORDER BY "logdate" ASC FETCH FIRST ? ROWS ONLY
		)
	`, limit).Exec(ctx)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// indexNames: il nome della tabella è quello quotato, quindi minuscolo; gli indici sono creati non
// quotati, quindi maiuscoli, e si confrontano abbassati.
func (o oracleDialect) IndexNames(ctx context.Context) ([]string, error) {
	var presenti []string
	err := o.db.NewRaw(
		"SELECT LOWER(index_name) FROM user_indexes WHERE table_name = ?", store.TableWorkItems,
	).Scan(ctx, &presenti)
	return presenti, err
}

// ensureIndexes crea anche le tabelle: su Oracle CreateTable di bun non è usabile (emette IF NOT
// EXISTS, che arriva con la 23ai, e il tipo jsonb del modello). L'idempotenza è per codice
// d'errore, perché fino alla 19c IF NOT EXISTS non c'è.
func (o oracleDialect) EnsureIndexes(ctx context.Context) error {
	return sqlbackend.ExecDDL(ctx, o.db, oracleTablesDDL+oracleColumnsDDL+oracleIndexesDDL, func(err error) bool {
		return isOracleError(err, "ORA-00955") || // nome già usato: tabella o indice esiste
			isOracleError(err, "ORA-01408") || // la stessa lista di colonne è già indicizzata
			isOracleError(err, "ORA-01430") // la colonna da aggiungere esiste già
	})
}

// execer è *sql.DB o *sql.Tx: le scritture con bind passano di qui e non dai metodi di bun, che
// interpolerebbero gli argomenti nel testo. Non attraversano quindi gli hook di query di bun
// (query logger, span bunotel).
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// insertRow scrive row (un *store.WorkItem o un *store.TaskLog) con un INSERT a bind posizionali.
// Le colonne sono quelle del modello bun, non una lista scritta qui: una colonna nuova sul
// WorkItem entra nell'INSERT senza toccare questo file.
func (o oracleDialect) insertRow(ctx context.Context, ex execer, row any) error {
	rv := reflect.ValueOf(row).Elem()
	table := o.db.Table(rv.Type())
	cols := make([]string, 0, len(table.Fields))
	vals := make([]string, 0, len(table.Fields))
	args := make([]any, 0, len(table.Fields))
	for _, f := range table.Fields {
		arg, isTime, err := oracleArg(f, rv)
		if err != nil {
			return fmt.Errorf("colonna %s: %w", f.Name, err)
		}
		args = append(args, arg)
		cols = append(cols, string(f.SQLName))
		ph := fmt.Sprintf(":%d", len(args))
		if isTime && arg != nil {
			ph = "TO_TIMESTAMP_TZ(" + ph + ", '" + oracleTSTZFormat + "')"
		}
		vals = append(vals, ph)
	}
	q := "INSERT INTO " + string(table.SQLName) +
		" (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
	_, err := ex.ExecContext(ctx, q, args...)
	return err
}

// oracleArg è il valore da legare alla colonna f. Un campo nullzero a zero è NULL (come lo
// scrive bun); un istante è la sua forma testuale con l'offset; un valore composto — il Payload —
// è il suo JSON.
func oracleArg(f *schema.Field, strct reflect.Value) (arg any, isTime bool, err error) {
	v := f.Value(strct)
	switch t := v.Interface().(type) {
	case time.Time:
		if t.IsZero() {
			return nil, true, nil
		}
		return t.Format(goTSTZLayout), true, nil
	case *time.Time:
		if t == nil || t.IsZero() {
			return nil, true, nil
		}
		return t.Format(goTSTZLayout), true, nil
	}
	if f.NullZero && f.HasZeroValue(strct) {
		return nil, false, nil
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Map, reflect.Slice, reflect.Pointer:
		if v.IsNil() {
			return nil, false, nil
		}
		b, err := json.Marshal(v.Interface())
		return string(b), false, err
	case reflect.Struct:
		b, err := json.Marshal(v.Interface())
		return string(b), false, err
	}
	return v.Interface(), false, nil
}

// isOracleError dice se err è l'errore Oracle code (es. "ORA-00001"). Si riconosce dal testo, che
// è lo stesso con go-ora e con godror: importare un driver per il suo tipo d'errore imporrebbe
// quel driver all'applicazione.
func isOracleError(err error, code string) bool {
	return err != nil && strings.Contains(err.Error(), code)
}

// oracleTablesDDL: le due tabelle con gli identificatori minuscoli quotati, cioè quelli che bun
// scrive dal modello. Il payload è un CLOB e non un JSON: il tipo JSON arriva con la 21c.
const oracleTablesDDL = `
		CREATE TABLE "work_items" (
			"id"          VARCHAR2(64) PRIMARY KEY,
			"task_name"   VARCHAR2(255),
			"object_id"   VARCHAR2(255),
			"payload"     CLOB,
			"status"      VARCHAR2(32),
			"create_time" TIMESTAMP WITH TIME ZONE,
			"update_time" TIMESTAMP WITH TIME ZONE,
			"locked_at"   TIMESTAMP WITH TIME ZONE,
			"lock_token"  VARCHAR2(64),
			"locked_by"   VARCHAR2(255),
			"executed_by" VARCHAR2(255),
			"next_run_at" TIMESTAMP WITH TIME ZONE,
			"retry"       NUMBER(10) DEFAULT 0 NOT NULL,
			"error"       VARCHAR2(4000)
		);

		CREATE TABLE "task_logs" (
			"task_id"   VARCHAR2(255),
			"job_id"    VARCHAR2(255),
			"task_name" VARCHAR2(255),
			"stato"     VARCHAR2(32),
			"hostname"  VARCHAR2(255),
			"logdate"   TIMESTAMP WITH TIME ZONE,
			"oggetto"   VARCHAR2(255),
			"errore"    CLOB
		);
	`

// oracleColumnsDDL è il gemello di ensureColumnsDDL, per una tabella creata prima delle colonne
// del fencing.
const oracleColumnsDDL = `
		ALTER TABLE "work_items" ADD ("lock_token"  VARCHAR2(64));
		ALTER TABLE "work_items" ADD ("locked_by"   VARCHAR2(255));
		ALTER TABLE "work_items" ADD ("executed_by" VARCHAR2(255));
	`

// oracleIndexesDDL: Oracle non ha indici parziali. L'unico attivo è function-based — una chiave
// tutta NULL non entra in un indice B-tree, quindi gli item DONE/FAILED ne restano fuori come
// nell'unico parziale di PostgreSQL. Gli altri sono composti pieni con gli stessi nomi: contano i
// nomi, perché sono quelli che store.ExpectedIndexes verifica.
const oracleIndexesDDL = `
		CREATE UNIQUE INDEX uk_workitem_active ON "work_items" (
			CASE WHEN "status" IN ('PENDING', 'IN_PROGRESS') THEN "task_name" END,
			CASE WHEN "status" IN ('PENDING', 'IN_PROGRESS') THEN "object_id" END
		);

		CREATE INDEX ix_workitem_claim ON "work_items" ("task_name", "status", "next_run_at", "create_time");

		CREATE INDEX ix_workitem_orphan ON "work_items" ("task_name", "status", "locked_at");

		CREATE INDEX ix_workitem_purge ON "work_items" ("status", "update_time");

		CREATE INDEX ix_tasklog_purge ON "task_logs" ("logdate");
	`
