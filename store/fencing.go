package store

import (
	"uuid"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// NewLockToken genera un fencing token unico per un claim/recover. Un nuovo token viene
// stampato su ogni item ad ogni (ri)claim: i Mark* lo richiedono in WHERE, così un worker
// stale (il cui item è stato ri-claimato da RecoverOrphans) non può più finalizzarlo.
func NewLockToken() string { return uuid.New().String() }

// Hostname ritorna l'hostname del processo. Usato come WorkItem.LockedBy/ExecutedBy per sapere
// quale replica ha in carico un item — solo osservabilità, non partecipa al fencing. Delega a
// core.GetHostname, la stessa fonte del campo hostname di task_logs: prima erano due helper che
// divergevano quando os.Hostname falliva ("unknown" qui, "" là).
func Hostname() string { return core.GetHostname() }
