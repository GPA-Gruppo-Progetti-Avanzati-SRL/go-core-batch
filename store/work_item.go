package store

import (
	"context"
	"time"
)

const (
	CollectionWorkItems = "work_items"
	TableWorkItems      = "work_items"
	StatusPending       = "PENDING"
	StatusInProgress    = "IN_PROGRESS"
	StatusDone          = "DONE"
	StatusFailed        = "FAILED"
)

// WorkItem is the generic unit-of-work document.
// Implements both mongo.ICollection and coresql.IRecord so either backend can persist it.
//
// I dati specifici del lavoro stanno in Payload. L'UNICA chiave di instradamento è TaskName: ci
// filtra il claiming, ci instrada il MuxRunner, ci si distingue una coda dall'altra e ci si
// deduplica insieme a ObjectId. Non ce ne sono altre — c'erano Destination e ObjectType, due
// filtri di claim facoltativi che nessun runner, dispatcher o worker leggeva mai: esistevano solo
// perché il job NotificationKafka sprecava il proprio TaskName su una costante, e sono spariti
// quando quel job ha cominciato a nominare il flusso che drena.
type WorkItem struct {
	Id string `bson:"_id"                       bun:"id,pk"`
	// TaskName è il nome della CODA a cui l'item appartiene, cioè ciò che un job claima. Per i job
	// che eseguono un runner è il NOME dell'istanza di task — la voce di `tasks:` referenziata dal
	// job (`properties.task`) o elencata da un worker pool (`workers[].tasks`) — e ci instrada il
	// MuxRunner via TaskRunner.TaskName; per NotificationKafka, che un runner non ce l'ha, è il
	// nome del flusso di notifiche (`properties.stream`).
	//
	// Non è un "tipo": ci filtra il claiming (ClaimPending/RecoverOrphans) e, con ObjectId, è la
	// chiave dell'indice unico che deduplica gli accodamenti.
	TaskName string `bson:"taskName"                  bun:"task_name"`
	// ObjectId identifica l'OGGETTO di dominio su cui si lavora, ed è la seconda metà della chiave
	// di deduplica: uk_workitem_active è unico su (task_name, object_id) per i soli stati attivi,
	// quindi InsertIfNotActive non accoda un secondo item finché il primo non è finito.
	ObjectId   string     `bson:"objectId"                  bun:"object_id"`
	Payload    any        `bson:"payload"                   bun:"payload,type:jsonb"`
	Status     string     `bson:"status"                    bun:"status"`
	CreateTime time.Time  `bson:"createTime"                bun:"create_time"`
	UpdateTime *time.Time `bson:"updateTime,omitempty"      bun:"update_time,nullzero"`
	// LockedAt è l'istante del claim, ed è IL SEGNALE che l'item è in carico a qualcuno: i Mark* e
	// Release lo azzerano, ed è l'unico dei tre campi di lock che azzerano. Un item è "sotto lock"
	// quando Status è IN_PROGRESS e LockedAt non è nullo — non quando LockedBy è valorizzato.
	LockedAt *time.Time `bson:"lockedAt,omitempty" bun:"locked_at,nullzero"`
	// LockToken è il fencing token: un valore unico rigenerato ad OGNI claim/recover.
	// I Mark* lo richiedono in WHERE, così un worker "stale" (il cui item è stato ri-claimato
	// da RecoverOrphans) non può più finalizzare l'item — il suo token non matcha più.
	//
	// SOPRAVVIVE alla finalizzazione: non viene azzerato, e il claim successivo lo sovrascrive.
	// È innocuo perché ogni Mark* pretende anche Status = IN_PROGRESS, quindi un token rimasto su
	// un item terminale o tornato PENDING non autorizza nulla.
	LockToken string `bson:"lockToken,omitempty" bun:"lock_token,nullzero"`
	// LockedBy è l'hostname di chi ha CLAIMATO l'item, cioè del processo che gira il tick del job
	// — solo osservabilità, NON partecipa al fencing. Sul percorso gRPC quello è lo SCHEDULER, che
	// l'item lo dispatcha e non lo esegue: chi l'ha eseguito è ExecutedBy.
	//
	// SOPRAVVIVE alla finalizzazione, come LockToken: su un item DONE resta leggibile chi l'aveva
	// preso in carico, che insieme a ExecutedBy è la coppia con cui si ricostruisce cos'è successo
	// senza dipendere da task_logs. Il rovescio è che su un item tornato PENDING è STALE — nomina
	// chi teneva il lease nel tentativo precedente — finché il claim successivo non lo riscrive:
	// per sapere se l'item è in carico ADESSO si guarda LockedAt, non questo campo.
	LockedBy string `bson:"lockedBy,omitempty"        bun:"locked_by,nullzero"`
	// ExecutedBy è l'hostname di chi ha ESEGUITO l'ultimo tentativo. Lo scrivono i Mark* che
	// finalizzano un'esecuzione — MarkDone, MarkFailed, MarkPending — e non il claim, perché sono
	// gli unici che girano nel processo che ha davvero eseguito il runner: il worker sul percorso
	// gRPC, lo scheduler su quello in-process.
	//
	// Esiste perché LockedBy non risponde alla domanda "chi l'ha fatto" in un deployment
	// distribuito, e l'unica altra traccia era la riga START/DONE di `task_logs` — che con
	// `task-log: errors` non c'è per gli esiti riusciti, con `off` non c'è affatto, e che
	// SingleTask non scrive mai.
	//
	// NON viene scritto da Release: lì il dispatch non è riuscito e nessuno ha eseguito nulla, che
	// è la ragione per cui Release non consuma nemmeno un ritentativo.
	ExecutedBy string     `bson:"executedBy,omitempty"      bun:"executed_by,nullzero"`
	NextRunAt  *time.Time `bson:"nextRunAt,omitempty"       bun:"next_run_at,nullzero"`
	Retry      int        `bson:"retry"                     bun:"retry"`
	Error      string     `bson:"error,omitempty"           bun:"error,nullzero"`
}

func (w WorkItem) GetCollectionName(ctx context.Context) string { return CollectionWorkItems }
func (w WorkItem) GetTableName(ctx context.Context) string      { return TableWorkItems }
