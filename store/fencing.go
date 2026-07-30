package store

import (
	"os"
	"sync"

	"github.com/google/uuid"
)

// NewLockToken genera un fencing token unico per un claim/recover. Un nuovo token viene
// stampato su ogni item ad ogni (ri)claim: i Mark* lo richiedono in WHERE, così un worker
// stale (il cui item è stato ri-claimato da RecoverOrphans) non può più finalizzarlo.
func NewLockToken() string { return uuid.NewString() }

var (
	hostnameOnce sync.Once
	hostnameVal  string
)

// Hostname ritorna l'hostname del processo (cached). Usato come WorkItem.LockedBy per sapere
// quale replica ha in carico un item — solo osservabilità, non partecipa al fencing.
func Hostname() string {
	hostnameOnce.Do(func() {
		if h, err := os.Hostname(); err == nil && h != "" {
			hostnameVal = h
		} else {
			hostnameVal = "unknown"
		}
	})
	return hostnameVal
}
