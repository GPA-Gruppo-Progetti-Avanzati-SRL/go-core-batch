# Codici di errore — go-core-batch

In batch gli errori vivono su due piani distinti:

1. **`*core.ApplicationError`** — errori di store/infrastruttura, con codici.
2. **Errori di ritorno del runner** — senza codice: sono **sentinelle** che il framework
   classifica per decidere il lifecycle del `WorkItem`. È il piano che conta per chi scrive un
   task.

> **`Ambit` = `go-core-batch`** (costante `errs.Ambit`, in `internal/errs`) su ogni errore del
> modulo: è il campo che dice da quale libreria viene il guasto. I codici stanno tutti in
> `internal/errs/errs.go` e passano dal costruttore `errs.Tech(code)` / `errs.NotFound()`.

## 1. Codici emessi

### Store del WorkItem (mongostore e sqlstore, stessi codici)

I `BATCH-MARK-*` sono separati perché distinguono **"non sono riuscito a prendere il lavoro"**
da **"l'ho fatto e non riesco a scriverne l'esito"**: nel secondo caso il WorkItem resta
`IN_PROGRESS` fino al `RecoverOrphans`, ed è l'unica traccia che lo spiega.

| Codice | HTTP | Costante | Origine |
|---|---|---|---|
| `BATCH-CLAIM` | 500 | `errs.CodeClaim` | `store/mongostore/work_item_store.go:121,127`, `store/sqlstore/work_item_store.go:123` |
| `BATCH-RECOVER` | 500 | `errs.CodeRecover` | `mongostore:178,184,199`, `sqlstore:166` |
| `BATCH-MARK-DONE` | 500 | `errs.CodeMarkDone` | `mongostore:234`, `sqlstore:186` |
| `BATCH-MARK-FAILED` | 500 | `errs.CodeMarkFailed` | `mongostore:251`, `sqlstore:206` |
| `BATCH-MARK-PENDING` | 500 | `errs.CodeMarkPending` | `mongostore:271`, `sqlstore:227` — è il retry di `store.Retry(d)` |
| `BATCH-DELETE` | 500 | `errs.CodeDelete` | `mongostore:291`, `sqlstore:244` |
| `BATCH-GET` | 500 | `errs.CodeGet` | `mongostore:303` |
| `BATCH-HASACTIVE` | 500 | `errs.CodeHasActive` | `mongostore:316`, `sqlstore:261` |
| `BATCH-INSERT` | 500 | `errs.CodeInsert` | `mongostore:336`, `sqlstore:279` — `InsertIfNotActive` |
| `BATCH-LIST` | 500 | `errs.CodeList` | `sqlstore:293,318` |
| `NOT-FOUND` | 404 | — | `mongostore:301` — WorkItem inesistente, causa `mongo.ErrNoDocuments` |

### Feed by-query (distributedjob)

| Codice | HTTP | Costante | Origine | Significato |
|---|---|---|---|---|
| `BATCH-QUERY` | 500 | `errs.CodeQuery` | `scheduler/distributedjob/mongostore/query_data.go:62`, `sqlstore/query_data.go:85` | query di feed fallita |
| `BATCH-QUERY-CUR` | 500 | `errs.CodeQueryCur` | `mongostore/query_data.go:89` | lettura del cursore del feed fallita |
| `BATCH-QUERY-IDENT` | 500 | `errs.CodeQueryIdent` | `sqlstore/query_data.go:29,34` | nome di tabella/colonna che non è un identificatore SQL valido. È il **guard anti SQL-injection** sui pezzi di query che arrivano dalla config: nessun escape, si rifiuta |

### Producer Kafka

go-core-batch **non ha più un producer Kafka**: il job `NotificationKafka` produce con quello di
go-core-kafka (`corekafka.IProducer`), quindi gli errori di produzione arrivano da lì con il codice
`KAFKA-PRODUCE` e `Ambit = go-core-kafka` (vedi `go-core-kafka/ERRORI.md`). I quattro codici
`BATCH-KAFKA-*` del vecchio `internal/kafkaproducer` non esistono più.

Restano di questa libreria gli errori del **lifecycle** dell'item: un payload che non si riesce a
tradurre in record Kafka non produce un `ApplicationError` ma un `MarkFailed` di quel singolo item
(`scheduler/kafkajob/notification.go`), e un errore di produzione rimette i claimati in `PENDING`.

### Job

| Codice | HTTP | Costante | Origine | Significato |
|---|---|---|---|---|
| `BATCH-JOB-PROPS` | 500 | `errs.CodeJobProperties` | `scheduler/kafkajob/notification.go:40,44,48` | property infrastrutturale mancante in `jobs[].properties`: rispettivamente `destination`, `object`, `topic` |

### Cambiamenti rispetto al censimento precedente

- **33 siti ricadevano su `TECH500`**: un claim fallito e un `MarkDone` fallito erano
  indistinguibili, benché il primo significhi "nessun lavoro preso" e il secondo "lavoro
  eseguito, esito perso".
- `PROPERTIES` → **`BATCH-JOB-PROPS`** e `QRY-IDENT` → **`BATCH-QUERY-IDENT`**: il primo
  collideva con l'omonimo di go-core-mongo, il secondo non diceva da quale libreria venisse.
  Sono errori di configurazione del framework: nessuna app ci fa branching sopra.

## 2. Esito del runner → lifecycle del WorkItem

`store.ApplyResult` (`store/task_runner.go:45`) classifica il valore ritornato da
`ITaskRunner.Run` in un `store.Outcome`. La catena è ispezionata con `errors.Is`/`errors.As`,
quindi **funziona anche se l'errore è avvolto** in un `*ApplicationError` (per questo
`ApplicationError.Unwrap` non ritorna mai nil).

| Ritorno del runner | `Outcome` | Azione del framework |
|---|---|---|
| `nil` | `OutcomeDone` | `MarkDone` |
| `store.ErrHandled` | `OutcomeHandled` | **nessun Mark\***: il runner ha già finalizzato il lifecycle (es. MarkDone + insert dei figli nella stessa transazione, outbox) |
| `*store.RetryError` (`store.Retry(d)` / `store.RetryWithCause(d, err)`) | `OutcomeRetry` | `MarkPending(d)` → `next_run_at = now + d`; `d == 0` = riclaimabile al tick successivo |
| qualsiasi altro errore | `OutcomeFailed` | `MarkFailed` con `err.Error()` come messaggio |

Sentinelle correlate:

| Errore | Origine | Significato |
|---|---|---|
| `store.ErrHandled` | `store/errors.go:12` | vedi tabella sopra |
| `store.RetryError` | `store/errors.go:21` | guasto transitorio; `Unwrap()` espone la `Cause` |
| `s3client.ErrObjectNotFound` | `internal/s3client/service.go:23` | oggetto S3 assente nel feed da file |
| `lock.ErrNotAcquired` / `lock.ErrLockLost` | `go-core-app/lock`, via `scheduler/gocronlock` | un'altra replica tiene il lock del tick: gocron **salta l'esecuzione**. È dispatch-dedup, non correttezza — quella la garantisce il claiming sul DB |

## 3. Errori di runtime senza codice

| Messaggio | Origine | Quando |
|---|---|---|
| `execution type not found: <TaskName>` | `worker/workers.go:144` | il worker pool ha ricevuto un WorkItem il cui `TaskName` non corrisponde a nessun runner registrato → `OutcomeFailed` → `MarkFailed` |
| `simplejob: job %q: nessun task %q registrato per il type %q` | `scheduler/simplejob/simplejob.go:163` | il job non trova il runner per il task risolto (property `task`, o dedotto dal `type`) |

## 4. Fail-fast all'avvio (panic — l'app non parte)

Errori di **configurazione o di wiring**, deliberatamente non recuperabili:

| Messaggio | Origine | Causa |
|---|---|---|
| `batch.Module: WithStore è obbligatorio` | `module.go:173` | manca il backend dello store (`storemongo.Module` / `storesql.Module`) |
| `batch.Module: WithLocker è obbligatorio` | `module.go:176` | manca il backend del lock distribuito (`mongolocker` / `sqllocker` / `redislocker`) |
| `batch: task <type> registrato fuori dalla funzione passata a batch.Module` | `task/task.go:108` | `runner.Register`/`simplejob.RegisterRunner` in un `init()`: lì la sezione `tasks:` non è ancora nota |
| `batch: la sezione tasks: richiede un name su ogni voce` | `task/task.go:167` | voce senza `name`. Il nome è la **chiave di routing** (`WorkItem.TaskName`) e non ha fallback sul `type` |
| `batch: <problemi>` | `task/task.go:201` | riferimenti incoerenti: `jobs[].properties.task` o `workers[].tasks` che nominano un task non dichiarato, task type registrato senza voce in `tasks:` |

Il fail-fast è **gate-ato sui modes**: in un processo `MODE=API` né `register` né la
validazione girano (lo store resta wirato, così l'API può accodare WorkItem).
