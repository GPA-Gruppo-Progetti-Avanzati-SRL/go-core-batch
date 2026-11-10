package sqlbackend

import (
	"context"
	"database/sql"
	"errors"
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
//
// Gli identificatori delle query di questo file sono QUOTATI ("work_items", "status"): su
// PostgreSQL quotato e non quotato coincidono, su Oracle no — bun quota ciò che scrive dal modello
// (INSERT), e un nome non quotato diventerebbe maiuscolo e non troverebbe la colonna. Le
// query che differiscono fra i due database stanno nei Dialect (pgbackend, oraclebackend).

// WorkItemStore implements store.IWorkItemStore using a SQL database via bun.
type WorkItemStore struct {
	Sql         *coresql.Service
	DB          *bun.DB // handle bun del Service, per le query native e il DDL
	dialect     Dialect
	idxWarnOnce sync.Once
}

// NewWorkItemStore fallisce se il database non è quello del dialetto: l'app non parte, invece di
// fallire al primo claim.
func NewWorkItemStore(sqlService *coresql.Service, newDialect NewDialect) (*WorkItemStore, error) {
	d, err := newDialect(sqlService.DB())
	if err != nil {
		return nil, err
	}
	return &WorkItemStore{Sql: sqlService, DB: sqlService.DB(), dialect: d}, nil
}

var _ store.IWorkItemStore = (*WorkItemStore)(nil)

// warnIfIndexesMissing legge gli indici della tabella e delega a store.WarnMissingIndexes il
// confronto con quelli attesi: l'elenco e il messaggio sono gli stessi dei due backend, qui resta
// solo il modo di sapere cosa esiste. Una sola volta per processo (sync.Once).
func (d *WorkItemStore) warnIfIndexesMissing(ctx context.Context) {
	d.idxWarnOnce.Do(func() {
		presenti, err := d.dialect.IndexNames(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("go-core-batch: impossibile verificare gli indici di work_items")
			return
		}
		store.WarnMissingIndexes(presenti, "sqlstore.EnsureIndexes")
	})
}

// ClaimPending atomically selects up to limit PENDING items of taskName, marks them IN_PROGRESS
// and returns the full records. Uses SELECT FOR UPDATE SKIP LOCKED — safe across multiple replicas.
//
// Ogni istante del lease è dell'orologio del DATABASE, mai di quello del processo: la scadenza di
// next_run_at la valuta il database, quindi un locked_at scritto con l'orologio di una replica e
// confrontato con quello del database è una misura fatta con due orologi — qualche secondo di
// scarto basta a far recuperare come orfano un item appena claimato, o a lasciare non claimabile
// un item appena rilasciato.
func (d *WorkItemStore) ClaimPending(ctx context.Context, taskName string, limit int) ([]*store.WorkItem, *core.Error) {
	// La verifica sta anche qui, e non solo su InsertIfNotActive: gli indici del claim servono a
	// OGNI job, compresi quelli claim-only (DistribuiteTask, NotificationKafka) che un feed non
	// ce l'hanno e quindi non passerebbero mai di là. È sync.Once: una sola lettura per processo.
	d.warnIfIndexesMissing(ctx)
	items, err := d.dialect.Claim(ctx, taskName, limit, store.NewLockToken(), store.Hostname())
	if err != nil {
		return nil, errs.Tech(errs.CodeClaim).WithCause(err)
	}
	return items, nil
}

// RecoverOrphans atomically re-claims IN_PROGRESS items older than maxAge by
// refreshing locked_at to now and incrementing retry. Returns the items for
// immediate processing — no reset to PENDING, no waiting for the next tick.
//
// La soglia è now - maxAge, cioè l'età del lease misurata dall'orologio che l'ha scritto (vedi
// ClaimPending). Un item IN_PROGRESS senza locked_at è un lease malformato e vale come scaduto:
// altrimenti nessun claim lo riprenderebbe più.
func (d *WorkItemStore) RecoverOrphans(ctx context.Context, taskName string, maxAge time.Duration, limit int) ([]*store.WorkItem, *core.Error) {
	items, err := d.dialect.RecoverOrphans(ctx, taskName, maxAge, limit, store.NewLockToken(), store.Hostname())
	if err != nil {
		return nil, errs.Tech(errs.CodeRecover).WithCause(err)
	}
	return items, nil
}

// MarkDone transitions IN_PROGRESS items to DONE in batch, fenced dal token (gli id devono
// condividere lo stesso lock_token). Idempotente: gli id non matchati (già finalizzati o token
// stale) sono ignorati — è l'esito atteso quando un worker stale prova a finalizzarli.
func (d *WorkItemStore) MarkDone(ctx context.Context, ids []string, token string) *core.Error {
	if len(ids) == 0 {
		return nil
	}
	res, err := d.DB.NewUpdate().TableExpr(`"work_items"`).
		Set(`"status" = ?`, store.StatusDone).
		Set(`"update_time" = `+d.dialect.Now()).
		Set(`"executed_by" = ?`, store.Hostname()).
		Set(`"locked_at" = NULL`).
		Where(`"id" IN (?) AND "status" = ? AND "lock_token" = ?`, bun.List(ids), store.StatusInProgress, token).
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
func (d *WorkItemStore) MarkFailed(ctx context.Context, id, token, reason string) *core.Error {
	res, err := d.DB.NewUpdate().TableExpr(`"work_items"`).
		Set(`"status" = ?`, store.StatusFailed).
		Set(`"error" = ?`, d.dialect.FitText(reason)).
		Set(`"update_time" = `+d.dialect.Now()).
		Set(`"executed_by" = ?`, store.Hostname()).
		Set(`"locked_at" = NULL`).
		Where(`"id" = ? AND "status" = ? AND "lock_token" = ?`, id, store.StatusInProgress, token).
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
func (d *WorkItemStore) Release(ctx context.Context, id, token string) *core.Error {
	res, err := d.DB.NewUpdate().TableExpr(`"work_items"`).
		Set(`"status" = ?`, store.StatusPending).
		Set(`"locked_at" = NULL`).
		Set(`"update_time" = `+d.dialect.Now()).
		Set(`"next_run_at" = `+d.dialect.Now()).
		Set(`"error" = NULL`).
		Where(`"id" = ? AND "status" = ? AND "lock_token" = ?`, id, store.StatusInProgress, token).
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
func (d *WorkItemStore) MarkPending(ctx context.Context, id, token string, after time.Duration) *core.Error {
	res, err := d.DB.NewUpdate().TableExpr(`"work_items"`).
		Set(`"status" = ?`, store.StatusPending).
		Set(`"locked_at" = NULL`).
		Set(`"update_time" = `+d.dialect.Now()).
		Set(`"executed_by" = ?`, store.Hostname()).
		Set(`"retry" = "retry" + 1`).
		// Il ritardo è relativo, e lo somma il database al proprio orologio: è lo stesso con cui
		// ClaimPending lo confronterà.
		Set(`"next_run_at" = `+d.dialect.AfterMs(), after.Milliseconds()).
		Set(`"error" = NULL`).
		Where(`"id" = ? AND "status" = ? AND "lock_token" = ?`, id, store.StatusInProgress, token).
		Exec(ctx)
	if err != nil {
		return errs.Tech(errs.CodeMarkPending).WithCause(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		log.Debug().Msgf("MarkPending: item %q non aggiornato (già finalizzato o token stale)", id)
	}
	return nil
}

func (d *WorkItemStore) Insert(ctx context.Context, items []*store.WorkItem) *core.Error {
	if len(items) == 0 {
		return nil
	}
	return d.dialect.Insert(ctx, items)
}

func (d *WorkItemStore) DeleteIfPending(ctx context.Context, id string) (bool, *core.Error) {
	res, err := d.DB.NewDelete().TableExpr(`"work_items"`).
		Where(`"id" = ? AND "status" = ?`, id, store.StatusPending).
		Exec(ctx)
	if err != nil {
		return false, errs.Tech(errs.CodeDelete).WithCause(err)
	}
	affected, _ := res.RowsAffected()
	return affected == 1, nil
}

func (d *WorkItemStore) GetById(ctx context.Context, id string) (*store.WorkItem, *core.Error) {
	// Non passa da coresql.GetById: quella scrive la colonna senza virgolette (WHERE id = ?), che
	// su Oracle diventa ID e non trova la colonna "id" di una tabella a identificatori quotati
	// (ORA-00904). Qui l'identificatore è quotato come nel resto del file.
	var item store.WorkItem
	err := d.DB.NewSelect().TableExpr(`"work_items"`).Where(`"id" = ?`, id).Scan(ctx, &item)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errs.NotFound().WithCause(err)
		}
		return nil, errs.Tech(errs.CodeGet).WithCause(err)
	}
	return &item, nil
}

func (d *WorkItemStore) HasActive(ctx context.Context, taskName, objectId string) (bool, *core.Error) {
	var count int
	if err := d.DB.NewSelect().TableExpr(`"work_items"`).
		ColumnExpr("COUNT(*)").
		Where(`"task_name" = ? AND "object_id" = ? AND "status" IN (?, ?)`,
			taskName, objectId, store.StatusPending, store.StatusInProgress).
		Scan(ctx, &count); err != nil {
		return false, errs.Tech(errs.CodeHasActive).WithCause(err)
	}
	return count > 0, nil
}

// InsertIfNotActive inserts each item only if no active (PENDING or IN_PROGRESS) entry
// exists for the same (task_name, object_id). Relies on the unique index
// uk_workitem_active — call EnsureIndexes at startup to create it.
func (d *WorkItemStore) InsertIfNotActive(ctx context.Context, items []*store.WorkItem) (int, *core.Error) {
	if len(items) == 0 {
		return 0, nil
	}
	d.warnIfIndexesMissing(ctx)
	n, err := d.dialect.InsertIfNotActive(ctx, items)
	if err != nil {
		return n, errs.Tech(errs.CodeInsert).WithCause(err)
	}
	return n, nil
}

// Purge cancella gli item nello stato indicato più vecchi di olderThan, al più limit per
// chiamata. Il limit tiene corta la singola transazione: la retention è ripetuta a ogni tick
// del job, non fatta tutta in una volta.
func (d *WorkItemStore) Purge(ctx context.Context, status string, olderThan time.Time, limit int) (int, *core.Error) {
	n, err := d.dialect.Purge(ctx, status, olderThan, limit)
	if err != nil {
		return 0, errs.Tech(errs.CodePurge).WithCause(err)
	}
	return n, nil
}

// Backlog conta i PENDING in attesa e ritorna la data di creazione del più vecchio.
func (d *WorkItemStore) Backlog(ctx context.Context, taskName string) (int, time.Time, *core.Error) {
	var row struct {
		N      int        `bun:"n"`
		Oldest *time.Time `bun:"oldest"`
	}
	// Alias quotati: su Oracle un alias non quotato tornerebbe maiuscolo, e non si legherebbe ai
	// campi `bun:"n"`/`bun:"oldest"`.
	if err := d.DB.NewRaw(
		`SELECT COUNT(*) AS "n", MIN("create_time") AS "oldest" FROM "work_items" WHERE "task_name" = ? AND "status" = ?`,
		taskName, store.StatusPending,
	).Scan(ctx, &row); err != nil {
		return 0, time.Time{}, errs.Tech(errs.CodeBacklog).WithCause(err)
	}
	if row.Oldest == nil {
		return row.N, time.Time{}, nil
	}
	return row.N, *row.Oldest, nil
}

func (d *WorkItemStore) List(ctx context.Context, taskName, status string, paging *page.Paging, sort page.SortRequest) ([]*store.WorkItem, *core.Error) {
	// Il filtro si esprime una volta sola: la COUNT e la SELECT paginata devono guardare le
	// stesse righe per costruzione. Erano due catene di Where scritte a mano — la prima mutata
	// da ColumnExpr("COUNT(*)") e poi buttata, la seconda ricostruita da zero — quindi due
	// posti in cui aggiungere un filtro, e uno da cui dimenticarlo.
	filtrata := func() *bun.SelectQuery {
		q := d.DB.NewSelect().TableExpr(`"work_items"`).Where(`"task_name" = ?`, taskName)
		if status != "" {
			q = q.Where(`"status" = ?`, status)
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
		q = q.OrderExpr(`"create_time" DESC`)
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
