# Codici di errore — go-core-batch

In batch gli errori vivono su due piani distinti:

1. **`*core.ApplicationError`** — errori di store/infrastruttura, con codici.
2. **Errori di ritorno del runner** — non hanno codice: sono **sentinelle** che il framework
   classifica per decidere il lifecycle del `WorkItem`. È il piano che conta di più per chi
   scrive un task.

## 1. Codici emessi

| Codice | HTTP | Origine | Significato |
|---|---|---|---|
| `PROPERTIES` | 500 | `scheduler/kafkajob/notification.go:40,44,48` | property infrastrutturale mancante nel job `NotificationKafka`: rispettivamente `destination`, `object`, `topic` non presenti in `jobs[].properties` |
| `QRY-IDENT` | 500 | `scheduler/distributedjob/sqlstore/query_data.go:28,33` | nome di tabella/colonna della query di feed non è un identificatore SQL valido. È il **guard anti SQL-injection** sui pezzi di query che arrivano dalla config: nessun escape, si rifiuta |
| `NOT-FOUND` | 404 | `store/mongostore/work_item_store.go:300` | WorkItem non trovato per id; causa `mongo.ErrNoDocuments` allegata |
| `TECH500` | 500 | tutto lo strato di store e produzione (37 siti: `store/mongostore/work_item_store.go`, `store/sqlstore/work_item_store.go`, `internal/kafkaproducer/producer.go`, `scheduler/distributedjob/*store/query_data.go`) | errore del driver DB/Kafka senza codice specifico, con l'errore originale in causa |

## 2. Esito del runner → lifecycle del WorkItem

`store.ApplyResult` (`store/task_runner.go:45`) classifica il valore ritornato da
`ITaskRunner.Run` in un `store.Outcome`. La catena è ispezionata con `errors.Is`/`errors.As`,
quindi **funziona anche se l'errore è avvolto** in un `*ApplicationError` (per questo
`ApplicationError.Unwrap` non ritorna mai nil).

| Ritorno del runner | `Outcome` | Azione del framework |
|---|---|---|
| `nil` | `OutcomeDone` | `MarkDone` |
| `store.ErrHandled` | `OutcomeHandled` | **nessun Mark\***: il runner ha già finalizzato il lifecycle da sé (es. MarkDone + insert dei figli nella stessa transazione, outbox) |
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
| `execution type not found: <TaskName>` | `worker/workers.go:144` | il worker pool ha ricevuto un WorkItem il cui `TaskName` non corrisponde a nessun runner registrato. Diventa `OutcomeFailed` → `MarkFailed` |
| `simplejob: job %q: nessun task %q registrato per il type %q` | `scheduler/simplejob/simplejob.go:163` | il job simplejob non trova il runner per il task risolto (property `task`, o dedotto dal `type` del job) |

## 4. Fail-fast all'avvio (panic — l'app non parte)

Sono errori di **configurazione o di wiring**, deliberatamente non recuperabili:

| Messaggio | Origine | Causa |
|---|---|---|
| `batch.Module: WithStore è obbligatorio` | `module.go:173` | manca il backend dello store (`storemongo.Module` / `storesql.Module`) |
| `batch.Module: WithLocker è obbligatorio` | `module.go:176` | manca il backend del lock distribuito (`mongolocker` / `sqllocker` / `redislocker`) |
| `batch: task <type> registrato fuori dalla funzione passata a batch.Module` | `task/task.go:108` | `runner.Register`/`simplejob.RegisterRunner` chiamata in un `init()`: lì la sezione `tasks:` non è ancora nota |
| `batch: la sezione tasks: richiede un name su ogni voce` | `task/task.go:167` | voce di `tasks:` senza `name`. Il nome è la **chiave di routing** (`WorkItem.TaskName`) e non ha fallback sul `type`: va scritto anche quando coincide |
| `batch: <problemi>` | `task/task.go:201` | riferimenti incoerenti: `jobs[].properties.task` o `workers[].tasks` che nominano un task non dichiarato, task type registrato senza voce in `tasks:` |

Il fail-fast è **gate-ato sui modes**: in un processo `MODE=API` né `register` né la
validazione girano (lo store resta wirato, così l'API può accodare WorkItem).
