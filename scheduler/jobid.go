package scheduler

import (
	"fmt"
	"uuid"
)

// NewJobID genera l'identificativo di UNA esecuzione di un job: il nome del job più un
// UUIDv7, che è la stessa convenzione degli id dei WorkItem (ordinato nel tempo, quindi
// leggibile come un timestamp senza esserne uno).
//
// Prima ogni famiglia di job aveva la sua copia della stessa funzione, che formattava il
// tempo al secondo: due tick nello stesso secondo — un cron sub-minuto, o un tick che
// ritarda — producevano lo STESSO jobId, e le righe di task_logs di esecuzioni diverse si
// confondevano. L'id di un'esecuzione deve essere unico per costruzione, non per fortuna.
//
// Non va MAI usato come label di una metrica: è ad alta cardinalità. Le metriche usano il
// nome del job (vedi batchmetrics).
func NewJobID(name string) string {
	return fmt.Sprintf("%s-%s", name, uuid.NewV7().String())
}
