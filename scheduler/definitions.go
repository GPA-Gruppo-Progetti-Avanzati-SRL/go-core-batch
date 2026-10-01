package scheduler

// Il vocabolario della sezione `jobs:`: i `type` dei job della libreria e le chiavi delle loro
// `properties`. È pubblico perché è il contratto della config che l'app scrive e che può anche
// leggere — un'app che vuole sapere, per esempio, quali definizioni accoda un FeedTask scorre i suoi
// Config e confronta questi nomi. La macchina che li esegue sta in internal/, e li prende da qui:
// la fonte è una sola, e un rename rompe la compilazione di chi li usa invece di lasciare in giro
// una stringa che non corrisponde più a nulla.

// Job type della libreria: il `type` di una voce di `jobs:`.
const (
	// JobTypeSingleTask esegue in linea, dentro il tick, un work item del task nominato da `task`.
	JobTypeSingleTask = "SingleTask"
	// JobTypeDistribuiteTask reclama i work item del task nominato da `task` e li consegna al
	// dispatcher: in-process, o via gRPC se `grpc.client.url` è valorizzato.
	JobTypeDistribuiteTask = "DistribuiteTask"
	// JobTypeDistribuiteTaskByQuery è DistribuiteTask con un feed da una query (query store
	// mongo o sql, da passare a batch.WithModule).
	JobTypeDistribuiteTaskByQuery = "DistribuiteTaskByQuery"
	// JobTypeDistribuiteTaskByS3File è DistribuiteTask con un feed dai file di un bucket S3
	// (s3feed, da passare a batch.WithModule).
	JobTypeDistribuiteTaskByS3File = "DistribuiteTaskByS3File"
	// JobTypeFeedTask accoda un work item per tick sul task nominato da `task`; non esegue nulla.
	JobTypeFeedTask = "FeedTask"
	// JobTypePurgeWorkItems cancella i work item terminali più vecchi di `older-than`.
	JobTypePurgeWorkItems = "PurgeWorkItems"
	// JobTypeNotificationKafka pubblica su Kafka i work item della coda `stream` (kafkajob, da
	// passare a batch.WithModule).
	JobTypeNotificationKafka = "NotificationKafka"
)

// Chiavi comuni a più job type.
const (
	// PropTask nomina l'istanza di task su cui il job lavora: una voce di `tasks:`. È anche il
	// WorkItem.TaskName degli item che il job claima o accoda.
	PropTask = "task"
	// PropLimit è il tetto al lavoro che un tick prende in carico.
	PropLimit = "limit"
	// PropBacklogMetrics abilita le gauge di coda su un job claim-based. Di default sono spente:
	// sono una query in più per tick, e la paga chi la vuole.
	PropBacklogMetrics = "backlog-metrics"
)

// Chiavi di FeedTask.
const (
	// PropObjectId è il WorkItem.ObjectId: identifica COSA accodare ed è la chiave su cui
	// l'indice unico parziale impedisce il duplicato.
	PropObjectId = "objectId"
	// PropPayload è il payload applicativo del work item, facoltativo. Copiato così com'è.
	PropPayload = "payload"
)

// Chiavi di PurgeWorkItems.
const (
	// PropStatus è lo stato degli item da cancellare: DONE o FAILED. Obbligatoria.
	PropStatus = "status"
	// PropOlderThan è l'età minima (durata) oltre la quale un item è cancellabile, misurata
	// sull'update_time. Obbligatoria.
	PropOlderThan = "older-than"
	// PropTaskLogs, se true, cancella anche le righe di task_logs più vecchie di older-than.
	PropTaskLogs = "task-logs"
)

// Chiavi di NotificationKafka.
const (
	// PropStream nomina il flusso di notifiche che il job drena: è il WorkItem.TaskName della coda.
	// Non si chiama `task` di proposito: `task` è un riferimento a una voce di `tasks:`, e una
	// notifica un runner da configurare non ce l'ha.
	PropStream = "stream"
	// PropTopic è il topic di default, per i record che non ne portano uno proprio.
	PropTopic = "topic"
	// PropMaxRetry è il tetto ai ritentativi di un item: assente o -1 = illimitato.
	PropMaxRetry = "max-retry"
)
