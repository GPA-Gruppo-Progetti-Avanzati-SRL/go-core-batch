package batchmetrics

// Etichette pprof delle goroutine del framework. Stanno qui, con le label delle metriche, per la
// stessa ragione per cui ci stanno i collector: le applica sia il lato scheduler (il tick di un
// job, le task del dispatcher in-process) sia il lato worker, e ognuno dei due le dichiarava per
// conto proprio — "batch_task_name" esisteva in due costanti, in due package, con lo stesso valore.
//
// Sono a BASSA CARDINALITÀ di proposito (nome e tipo del job, nome del worker e del task; mai un
// jobId o un taskId, che contengono un timestamp): il profilo raggruppa per set di label, e da
// Go 1.27 le label compaiono anche nell'header dei traceback. Su un processo batch long-running
// sono ciò che rende leggibili /debug/pprof/goroutine e /debug/pprof/goroutineleak, dove
// altrimenti tutti i tick si presentano come goroutine gocron indistinguibili.
const (
	// LabelJob e LabelJobType etichettano la goroutine di un tick (scheduler.LabeledTask).
	LabelJob     = "batch_job"
	LabelJobType = "batch_job_type"
	// LabelWorker è il nome del pool che ha preso in carico la task.
	LabelWorker = "batch_worker"
	// LabelTaskName è il nome dell'istanza di task in esecuzione, sui due percorsi: la goroutine
	// del worker pool e quella del dispatcher in-process.
	LabelTaskName = "batch_task_name"
)
