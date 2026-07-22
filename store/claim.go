package store

import (
	"context"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/rs/zerolog/log"
)

// ClaimBatch è la parte comune del loop di claim condivisa da tutti i job (distributedjob,
// simplejob, kafkajob): recupera gli orfani (IN_PROGRESS scaduti) e poi claima i PENDING
// freschi fino a `limit`, restituendo il batch combinato (orfani + freschi) da processare.
//
//   - Gli errori di recupero orfani sono best-effort: loggati e trattati come "nessun orfano".
//   - Un errore di ClaimPending è restituito in claimErr; `batch` contiene comunque gli
//     eventuali orfani, così il chiamante applica la propria policy (abortire vs processare
//     gli orfani già recuperati).
//
// Restano al chiamante: la fase di feed (che precede), lo span/tracing, e la fase di process
// (dispatch gRPC / esecuzione in-process / publish Kafka) che segue.
func ClaimBatch(ctx context.Context, items IWorkItemStore, jobID, workType, destination, objectType string, orphanTimeout time.Duration, limit int) (batch []*WorkItem, orphans, fresh int, claimErr *core.ApplicationError) {
	orph, appErr := items.RecoverOrphans(ctx, workType, destination, objectType, orphanTimeout, limit)
	if appErr != nil {
		log.Warn().Err(appErr).Msgf("[%s] orphan recovery failed", jobID)
		orph = nil
	} else if len(orph) > 0 {
		log.Info().Msgf("[%s] re-claimed %d orphaned item(s) (timeout=%s)", jobID, len(orph), orphanTimeout)
	}

	batch = orph
	orphans = len(orph)

	if remaining := limit - orphans; remaining > 0 {
		fr, err := items.ClaimPending(ctx, workType, destination, objectType, remaining)
		if err != nil {
			return batch, orphans, 0, err
		}
		batch = append(batch, fr...)
		fresh = len(fr)
	}
	return batch, orphans, fresh, nil
}
