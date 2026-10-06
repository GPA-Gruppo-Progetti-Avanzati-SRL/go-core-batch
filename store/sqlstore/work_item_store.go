package sqlstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/errs"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
	coresql "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-sql"
	"github.com/rs/zerolog/log"

	"github.com/uptrace/bun"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// NOTA: qui c'era un `workItemFilter` gemello di quello di mongostore, e non aveva alcun lettore:
// in questo backend ogni query costruisce il proprio WHERE con bun, compresa List. Restava in vita
// perché dichiarava i filtri su destination/object_type, che nessuno passava; spariti quelli, è
// sparito anche lui.

// workItemDataSQL implements store.IWorkItemStore using a SQL database via bun.
type workItemDataSQL struct {
	Sql         *coresql.Service
	DB          *bun.DB // handle bun del Service, per le query native e il DDL
	idxWarnOnce sync.Once
}

func newWorkItemDataSQL(sqlService *coresql.Service) *workItemDataSQL {
	return &workItemDataSQL{Sql: sqlService, DB: sqlService.DB()}
}

var _ store.IWorkItemStore = (*workItemDataSQL)(nil)

// warnIfIndexesMissing legge gli indici della tabella e delega a store.WarnMissingIndexes il
// confronto con quelli attesi: l'elenco e il messaggio sono gli stessi dei due backend, qui resta
// solo il modo di sapere cosa esiste. Una sola volta per processo (sync.Once).
func (d *workItemDataSQL) warnIfIndexesMissing(ctx context.Context) {
	d.idxWarnOnce.Do(func() {
		var presenti []string
		if err := d.DB.NewRaw(
			"SELECT indexname FROM pg_indexes WHERE tablename = ?", store.TableWorkItems,
		).Scan(ctx, &presenti); err != nil {
			log.Warn().Err(err).Msg("go-core-batch: impossibile verificare gli indici di work_items")
			return
		}
		store.WarnMissingIndexes(presenti, "sqlstore.EnsureIndexes")
	})
}

// ClaimPending atomically selects up to limit PENDING items of taskName, marks them IN_PROGRESS
// and returns the full records. Uses SELECT FOR UPDATE SKIP LOCKED — safe across multiple replicas.
//
// Ogni istante del lease è dell'orologio del DATABASE (NOW()), mai di quello del processo: la
// scadenza di next_run_at la valuta il database, quindi un locked_at scritto con l'orologio di una
// replica e confrontato con quello del database è una misura fatta con due orologi — qualche
// secondo di scarto basta a far recuperare come orfano un item appena claimato, o a lasciare non
// claimabile un item appena rilasciato. È un'istruzione sola, quindi atomica senza transazione, e
// ritorna ciò che ha scritto (RETURNING) invece di ricostruirlo.
func (d *workItemDataSQL) ClaimPending(ctx context.Context, taskName string, limit int) ([]*store.WorkItem, *core.Error) {
	// La verifica sta anche qui, e non solo su InsertIfNotActive: gli indici del claim servono a
	// OGNI job, compresi quelli claim-only (DistribuiteTask, NotificationKafka) che un feed non
	// ce l'hanno e quindi non passerebbero mai di là. È sync.Once: una sola lettura per processo.
	d.warnIfIndexesMissing(ctx)
	var items []*store.WorkItem
	err := d.DB.NewRaw(`
		WITH claimed AS (
			SELECT id FROM work_items
			WHERE task_name = ? AND status = ? AND (next_run_at IS NULL OR next_run_at <= NOW())
			ORDER BY next_run_at ASC NULLS FIRST, create_time ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE work_items w
			SET status = ?, locked_at = NOW(), update_time = NOW(), lock_token = ?, locked_by = ?
			FROM claimed WHERE w.id = claimed.id
			RETURNING w.*
		)
		SELECT * FROM updated ORDER BY next_run_at ASC NULLS FIRST, create_time ASC
	`, taskName, store.StatusPending, limit,
		store.StatusInProgress, store.NewLockToken(), store.Hostname(),
	).Scan(ctx, &items)
	if err != nil {
		return nil, errs.Tech(errs.CodeClaim).WithCause(err)
	}
	return items, nil
}

// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
// refreshing locked_at to now and incrementing retry. Returns the items for
// immediate processing — no reset to PENDING, no waiting for the next tick.
// Uses a CTE with FOR UPDATE SKIP LOCKED so it is safe across replicas.
//
// La soglia è NOW() - maxAge, cioè l'età del lease misurata dall'orologio che l'ha scritto (vedi
// ClaimPending). Un item IN_PROGRESS senza locked_at è un lease malformato e vale come scaduto:
// altrimenti nessun claim lo riprenderebbe più.
func (d *workItemDataSQL) RecoverOrphans(ctx context.Context, taskName string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.Error) {
	var items []*store.WorkItem
	err := d.DB.NewRaw(`
		WITH recovered AS (
			SELECT id FROM work_items
			WHERE task_name = ? AND status = ?
			  AND (locked_at IS NULL OR locked_at < NOW() - (? * INTERVAL '1 millisecond'))
			ORDER BY locked_at ASC NULLS FIRST
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		UPDATE work_items
		SET locked_at = NOW(), lock_token = ?, locked_by = ?, retry = retry + 1, update_time = NOW()
		WHERE id IN (SELECT id FROM recovered)
		RETURNING *
	`, taskName, store.StatusInProgress, maxAge.Milliseconds(), limit,
		store.NewLockToken(), store.Hostname(),
	).Scan(ctx, &items)
	if err != nil {
		return nil, errs.Tech(errs.CodeRecover).WithCause(err)
	}
	return items, nil
}

// MarkDone transitions IN_PROGRESS items to DONE in batch, fenced dal token (gli id devono
// condividere lo stesso lock_token). Idempotente: gli id non matchati (già finalizzati o token
// stale) sono ignorati — è l'esito atteso quando un worker stale prova a finalizzarli.
func (d *workItemDataSQL) MarkDone(ctx context.Context, ids []string, token string) *core.Error {
	if len(ids) == 0 {
		return nil
	}
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusDone).
		Set("update_time = NOW()").
		Set("executed_by = ?", store.Hostname()).
		Set("locked_at = NULL").
		Where("id IN (?) AND status = ? AND lock_token = ?", bun.List(ids), store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeMarkDone).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); int(affected) != len(ids) {
		log.Debug().Msgf("MarkDone: %d/%d item marcati DONE (gli altri già finalizzati o token stale)",
			affected, len(ids))
	}
	return nil
}

// MarkFailed transitions a single IN_PROGRESS item to FAILED, fenced dal token (idempotente).
func (d *workItemDataSQL) MarkFailed(ctx context.Context, id, token, reason string) *core.Error {
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusFailed).
		Set("error = ?", reason).
		Set("update_time = NOW()").
		Set("executed_by = ?", store.Hostname()).
		Set("locked_at = NULL").
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeMarkFailed).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkFailed: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// L'`error` di un fallimento precedente viene AZZERATO: un item che torna PENDING è un item vivo,
// e lasciargli addosso il motivo per cui l'ultimo tentativo non era riuscito fa leggere come
// fallito qualcosa che è solo in attesa. Il motivo resta nella riga di task_logs.
//
// Release riporta a PENDING un item claimato ma mai eseguito (dispatch fallito), fenced dal
// token e idempotente. È MarkPending meno l'incremento di retry: il tentativo non è avvenuto,
// quindi non va contato. next_run_at = now, così il tick successivo lo riprende subito.
func (d *workItemDataSQL) Release(ctx context.Context, id, token string) *core.Error {
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusPending).
		Set("locked_at = NULL").
		Set("update_time = NOW()").
		Set("next_run_at = NOW()").
		Set("error = NULL").
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeRelease).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("Release: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

// MarkPending resets a single IN_PROGRESS item back to PENDING for retry, fenced dal token (idempotente).
//
// L'`error` di un fallimento precedente viene AZZERATO: un item che torna PENDING è un item vivo,
// e lasciargli addosso il motivo per cui l'ultimo tentativo non era riuscito fa leggere come
// fallito qualcosa che è solo in attesa. Il motivo resta nella riga di task_logs.
func (d *workItemDataSQL) MarkPending(ctx context.Context, id, token string, after time.Duration) *core.Error {
	res, err := d.DB.NewUpdate().TableExpr(store.TableWorkItems).
		Set("status = ?", store.StatusPending).
		Set("locked_at = NULL").
		Set("update_time = NOW()").
		Set("executed_by = ?", store.Hostname()).
		Set("retry = retry + 1").
		// Il ritardo è relativo, e lo somma il database al proprio orologio: è lo stesso con cui
		// ClaimPending lo confronterà.
		Set("next_run_at = NOW() + (? * INTERVAL '1 millisecond')", after.Milliseconds()).
		Set("error = NULL").
		Where("id = ? AND status = ? AND lock_token = ?", id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeMarkPending).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkPending: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

func (d *workItemDataSQL) Insert(ctx context.Context, items []*store.WorkItem) *core.Error {
	return d.Sql.InsertMany[store.WorkItem](ctx, items)
}

func (d *workItemDataSQL) DeleteIfPending(ctx context.Context, id string) (bool, *core.Error) {
	res, err := d.DB.NewDelete().TableExpr(store.TableWorkItems).
		Where("id = ? AND status = ?", id, store.StatusPending).
		Exec(ctx)
	if err != nil {
		return false, errs.Tech(errs.CodeDelete).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return affected == 1, nil
}

func (d *workItemDataSQL) GetById(ctx context.Context, id string) (*store.WorkItem, *core.Error) {
	return d.Sql.GetById[store.WorkItem](ctx, id)
}

func (d *workItemDataSQL) HasActive(ctx context.Context, taskName, objectId string) (bool, *core.Error) {
	var count int
	if err := d.DB.NewSelect().TableExpr(store.TableWorkItems).
		ColumnExpr("COUNT(*)").
		Where("task_name = ? AND object_id = ? AND status IN (?, ?)",
			taskName, objectId, store.StatusPending, store.StatusInProgress).
		Scan(ctx, &count); err != nil {
		return false, errs.Tech(errs.CodeHasActive).WithCause(err)
	}
	return count > 0, nil
}

// InsertIfNotActive inserts each item only if no active (PENDING or IN_PROGRESS) entry
// exists for the same (task_name, object_id). Relies on the partial unique index
// uk_workitem_active — call EnsureIndexes at startup to create it.
func (d *workItemDataSQL) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.Error) {
	if len(items) == 0 {
		return 0, nil
	}
	d.warnIfIndexesMissing(ctx)
	res, err := d.DB.NewInsert().
		Model(&items).
		On("CONFLICT DO NOTHING").
		Exec(ctx)
	if err != nil {
		return 0, errs.Tech(errs.CodeInsert).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// Purge cancella gli item nello stato indicato più vecchi di olderThan, al più limit per
// chiamata. Il limit tiene corta la singola transazione: la retention è ripetuta a ogni tick
// del job, non fatta tutta in una volta.
func (d *workItemDataSQL) Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, *core.Error) {
	res, err := d.DB.NewRaw(`
		DELETE FROM work_items
		WHERE id IN (
			SELECT id FROM work_items
			WHERE status = ? AND update_time < ?
			ORDER BY update_time ASC
			LIMIT ?
		)
	`, status, olderThan, limit).Exec(ctx)
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// Backlog conta i PENDING in attesa e ritorna la data di creazione del più vecchio.
func (d *workItemDataSQL) Backlog(ctx context.Context, taskName string) (int, time.Time, *core.Error) {
	where := "task_name = ? AND status = ?"
	args := []any{taskName, store.StatusPending}
	var row struct {
		N      int        `bun:"n"`
		Oldest *time.Time `bun:"oldest"`
	}
	if err := d.DB.NewRaw(
		"SELECT COUNT(*) AS n, MIN(create_time) AS oldest FROM work_items WHERE "+where, args...,
	).Scan(ctx, &row); err != nil {
		return 0, time.Time{}, errs.Tech(errs.CodeBacklog).WithCause(err)
	}
	if row.Oldest == nil {
		return row.N, time.Time{}, nil
	}
	return row.N, *row.Oldest, nil
}

func (d *workItemDataSQL) List(ctx context.Context, taskName, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.Error) {
	// Il filtro si esprime una volta sola: la COUNT e la SELECT paginata devono guardare le
	// stesse righe per costruzione. Erano due catene di Where scritte a mano — la prima mutata
	// da ColumnExpr("COUNT(*)") e poi buttata, la seconda ricostruita da zero — quindi due
	// posti in cui aggiungere un filtro, e uno da cui dimenticarlo.
	filtrata := func() *bun.SelectQuery {
		q := d.DB.NewSelect().TableExpr(store.TableWorkItems).Where("task_name = ?", taskName)
		if status != "" {
			q = q.Where("status = ?", status)
		}
		return q
	}

	var total int64
	if err := filtrata().ColumnExpr("COUNT(*)").Scan(ctx, &total); err != nil {
		return nil, errs.Tech(errs.CodeList).WithCause(err)
	}
	paging.SetTotalItems(total)

	offset, appErr := paging.Paging()
	if appErr != nil {
		return nil, appErr
	}

	q := filtrata()
	if len(sort) == 0 {
		q = q.OrderExpr("create_time DESC")
	} else {
		var sortErr error
		if q, sortErr = coresql.ApplySort(q, sort); sortErr != nil {
			return nil, errs.Tech(errs.CodeList).WithCause(sortErr)
		}
	}
	if offset >= 0 {
		q = q.Offset(int64(offset)).Limit(int64(paging.PageSize))
	}

	var items []*store.WorkItem
	if err := q.Scan(ctx, &items); err != nil {
		return nil, errs.Tech(errs.CodeList).WithCause(err)
	}
	return items, nil
}

// EnsureIndexes crea colonne e indici richiesti da workItemDataSQL. Chiamarla una volta
// all'avvio. Include:
//   - le colonne di fencing lock_token/locked_by (ADD COLUMN IF NOT EXISTS), usate da
//     ClaimPending/RecoverOrphans/Mark* per impedire che un worker stale finalizzi un item
//     ri-claimato, più executed_by, che i Mark* riempiono con l'hostname di CHI HA ESEGUITO
//     (diverso da locked_by, che è di chi ha claimato, quando il dispatch passa per gRPC);
//   - uk_workitem_active, unico parziale, che impedisce l'inserimento concorrente di item attivi
//     duplicati per lo stesso (task_name, object_id);
//   - ix_workitem_claim / ix_workitem_orphan, che servono le query di claim e recupero orfani
//     eseguite da ogni job a ogni tick. Sono parziali sugli stati ATTIVI: gli item DONE/FAILED non
//     vengono mai claimati, quindi tenerli fuori mantiene l'indice della dimensione del LAVORO e
//     non dello storico;
//   - ix_workitem_purge, che serve la query del job PurgeWorkItems ed è parziale sugli stati
//     TERMINALI per la ragione speculare: la retention lavora solo lì.
//
// È Postgres-specifico (come il resto delle utility DDL del modulo). Su MySQL/SQLite le colonne
// e gli indici vanno creati manualmente via migration.
func EnsureIndexes(ctx context.Context, db *bun.DB) error {
	// Un'istruzione per Exec: un blocco multi-statement lo accettano solo alcuni driver (il
	// protocollo semplice di PostgreSQL sì, MySQL solo con multiStatements=true), e se una delle
	// istruzioni fallisce l'errore non dice quale.
	for _, stmt := range ddlStatements(ensureColumnsDDL + ensureIndexesDDL) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("EnsureIndexes: %s: %w", strings.Join(strings.Fields(stmt), " "), err)
		}
	}
	return nil
}

// ddlStatements divide un blocco DDL nelle sue istruzioni (separate da `;`), scartando quelle vuote.
func ddlStatements(ddl string) []string {
	var out []string
	for _, s := range strings.Split(ddl, ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// ensureColumnsDDL sono le colonne che la libreria aggiunge a una tabella già esistente. Come per
// gli indici è una costante e non un literal in linea, perché ha un secondo lettore: il test che
// verifica che ci sia una ADD COLUMN per ogni colonna scritta dal claim e dai Mark*.
const ensureColumnsDDL = `
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS lock_token  TEXT;
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS locked_by   TEXT;
		ALTER TABLE work_items ADD COLUMN IF NOT EXISTS executed_by TEXT;
	`

// ensureIndexesDDL è il DDL degli indici, estratto in una costante perché ha due lettori:
// EnsureIndexes che lo esegue e un test che verifica che crei tutti gli store.ExpectedIndexes —
// cioè esattamente quelli che la verifica di avvio pretende di trovare.
const ensureIndexesDDL = `
		CREATE UNIQUE INDEX IF NOT EXISTS uk_workitem_active
		ON work_items (task_name, object_id)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_claim
		ON work_items (task_name, status, next_run_at, create_time)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_orphan
		ON work_items (task_name, status, locked_at)
		WHERE status IN ('PENDING', 'IN_PROGRESS');

		CREATE INDEX IF NOT EXISTS ix_workitem_purge
		ON work_items (status, update_time)
		WHERE status IN ('DONE', 'FAILED');

		CREATE INDEX IF NOT EXISTS ix_tasklog_purge
		ON task_logs (logdate);
	`
