package scheduler

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// JobIDTimeLayout è il formato del timestamp dentro il jobId: ordinabile lessicograficamente
// e a larghezza fissa, quindi due id si confrontano come stringhe.
const JobIDTimeLayout = "20060102150405"

// NewJobID genera l'identificativo di UNA esecuzione di un job: nome, timestamp al secondo
// e una coda casuale — "quadratura-job-20260928085730-a63fd0dc".
//
// Il timestamp c'è perché l'id lo si legge nei log e in task_logs, e un id opaco non dice
// quando quell'esecuzione è partita. La coda c'è perché il timestamp da solo NON è univoco,
// ed è il difetto che questa funzione ha già avuto una volta: le righe di task_logs di due
// esecuzioni diverse si confondevano.
//
// Che un cron non possa scattare due volte nello stesso secondo è vero della SCHEDULAZIONE,
// non dell'OROLOGIO da cui l'id è derivato:
//
//   - alla fine dell'ora legale un'ora di parete si ripete, quindi un cron in quell'ora
//     scatta due volte con lo stesso orario locale. Verificato su robfig/cron (il parser di
//     gocron) con `0 30 2 * * *` in Europe/Rome: 2026-10-25T02:30:00+02:00 e
//     2026-10-25T02:30:00+01:00 formattano entrambi "20261025023000";
//   - una correzione NTP all'indietro riporta l'orologio su secondi già usati;
//   - due repliche hanno due orologi. Il lock dello scheduler è dedup e non correttezza:
//     un tick che non trova pending items dura millisecondi e rilascia subito, quindi la
//     replica il cui orologio è indietro di un secondo trova il lock libero e scatta il
//     "suo" stesso secondo.
//
// Nove caratteri di coda comprano un'unicità che vale per costruzione invece che per un
// ragionamento su fuso orario, demone NTP e durata del lock.
//
// La coda è math/rand/v2 e non un generatore crittografico di proposito: deve solo NON
// COLLIDERE, non essere imprevedibile. Nessuno la confronta e nessuno le concede qualcosa —
// a differenza del fencing token dei WorkItem (store.NewLockToken), che è un'autorizzazione
// a finalizzare e per questo è crypto-random. 32 bit bastano: la collisione conta solo fra
// due esecuzioni nello STESSO secondo, perché fuori dal secondo distingue il timestamp.
//
// Non va MAI usato come label di una metrica: è ad alta cardinalità. Le metriche usano il
// nome del job (vedi batchmetrics).
func NewJobID(name string) string {
	return fmt.Sprintf("%s-%s-%08x", name, time.Now().Format(JobIDTimeLayout), rand.Uint32())
}
