// Package lifecycle raccoglie i pezzi di arresto condivisi fra i due esecutori in-process di
// go-core-batch: il worker pool e il dispatcher locale.
package lifecycle

import (
	"context"
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/httpx"
	"github.com/rs/zerolog/log"
)

// Drain attende che le task IN VOLO finiscano, ma non oltre la deadline del context di stop di fx.
//
// Le due metà contano entrambe. Senza l'attesa, un SIGTERM tronca a metà le task già partite: i
// loro item restano IN_PROGRESS fino al recupero orfani, che oltre a farli aspettare gli consuma un
// ritentativo. Senza il limite, una task appesa terrebbe in piedi il processo per sempre — quindi
// oltre la deadline le residue si abbandonano, ed è corretto farlo: sono claimate, e RecoverOrphans
// le riprende.
//
// `chi` nomina il componente nei due log, che sono l'unico modo di sapere, dopo un rolling restart,
// se l'arresto è stato pulito.
func Drain(ctx context.Context, wg *sync.WaitGroup, chi string) {
	if httpx.WaitContext(ctx, wg) {
		log.Info().Msgf("%s: tutte le task in volo drenate", chi)
		return
	}
	log.Warn().Msgf("%s: drain scaduto, task residue abbandonate (saranno recuperate come orfani)", chi)
}
