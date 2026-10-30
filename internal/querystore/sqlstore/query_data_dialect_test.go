package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/oracledialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/schema"
)

type noConnector struct{}

func (noConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("nessun database")
}
func (noConnector) Driver() driver.Driver { return nil }

func feedQuery(t *testing.T, d schema.Dialect, ident Ident) string {
	t.Helper()
	db := bun.NewDB(sql.OpenDB(noConnector{}), d)
	t.Cleanup(func() { _ = db.Close() })
	q := &queryDataSQL{DB: db, ident: ident}
	return db.NewSelect().TableExpr("?", q.ident("legacy.my_table")).ColumnExpr("id").
		OrderExpr("? ASC", q.ident("created_at")).String()
}

// Quoted (guscio PostgreSQL) quota gli identificatori del feed; Unquoted (guscio Oracle) no, perché lì un nome
// quotato è case-sensitive e la tabella di uno schema legacy si chiama MY_TABLE, non "my_table".
func TestIdent_QuotedUnquoted(t *testing.T) {
	if got := feedQuery(t, pgdialect.New(), Quoted); !strings.Contains(got, `"legacy"."my_table"`) || !strings.Contains(got, `"created_at" ASC`) {
		t.Errorf("PostgreSQL: identificatori non quotati: %s", got)
	}
	got := feedQuery(t, oracledialect.New(), Unquoted)
	if strings.Contains(got, `"`) {
		t.Errorf("Oracle: identificatori quotati, non troverebbero la tabella legacy: %s", got)
	}
	if !strings.Contains(got, "FROM legacy.my_table") || !strings.Contains(got, "created_at ASC") {
		t.Errorf("Oracle: query inattesa: %s", got)
	}
}
