// Package sqlstore provides a SQL-backed implementation of distributedjob.IQueryStore.
// Import this package when the external feed source is a SQL database.
package sqlstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/uptrace/bun"
)

// QueryDataSQL implements distributedjob.IQueryStore against a SQL database.
type QueryDataSQL struct {
	DB *bun.DB
}

func NewQueryDataSQL(db *bun.DB) *QueryDataSQL {
	return &QueryDataSQL{DB: db}
}

var _ distributedjob.IQueryStore = (*QueryDataSQL)(nil)

func (q *QueryDataSQL) GetIds(ctx context.Context, table, filter string, limit int) ([]string, *core.ApplicationError) {
	return q.GetIdsSorted(ctx, table, filter, "", limit)
}

func (q *QueryDataSQL) GetIdsSorted(ctx context.Context, table, filter, sort string, limit int) ([]string, *core.ApplicationError) {
	query := q.DB.NewSelect().TableExpr(table).ColumnExpr("id")
	if filter != "" {
		query = query.Where(filter)
	}
	if sort != "" {
		for _, part := range strings.Split(sort, ",") {
			fields := strings.SplitN(strings.TrimSpace(part), ":", 2)
			col := fields[0]
			dir := "ASC"
			if len(fields) == 2 && strings.ToLower(fields[1]) == "desc" {
				dir = "DESC"
			}
			query = query.OrderExpr(fmt.Sprintf("%s %s", col, dir))
		}
	}
	if limit > 0 {
		query = query.Limit(limit)
	}
	var ids []string
	if err := query.Scan(ctx, &ids); err != nil {
		return nil, core.TechnicalErrorWithError(err)
	}
	return ids, nil
}

// parseLimit converts the "limit" property string to int.
func parseLimit(s string) (int, error) {
	return strconv.Atoi(s)
}
