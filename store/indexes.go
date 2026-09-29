package store

import (
	"slices"

	"github.com/rs/zerolog/log"
)

// Nomi degli indici su cui gira il sottosistema. Sono QUI perché hanno quattro lettori — le due
// EnsureIndexes che li creano e le due verifiche che ne segnalano l'assenza — e finora erano
// quattro liste separate: due `var indiciAttesi` identiche più i nomi ripetuti dentro il DDL dei
// due backend. Quattro copie di un elenco sono quattro posti da cui può sparire una voce.
const (
	// IndexWorkItemActive è l'unico PARZIALE su (task_name, object_id) per gli stati attivi:
	// senza, InsertIfNotActive non deduplica — non c'è duplicate-key da intercettare — e
	// nascono work item doppi, con rischio di doppia esecuzione.
	IndexWorkItemActive = "uk_workitem_active"
	// IndexWorkItemClaim serve la query di ClaimPending (filtro + ordinamento per scadenza).
	IndexWorkItemClaim = "ix_workitem_claim"
	// IndexWorkItemOrphan serve la query di RecoverOrphans.
	IndexWorkItemOrphan = "ix_workitem_orphan"
	// IndexWorkItemPurge serve la query del job PurgeWorkItems, che è l'OPPOSTO delle altre tre:
	// filtra e ordina per update_time sugli stati TERMINALI, cioè sulla parte grande della
	// collection — quella che il job esiste per tenere sotto controllo. Senza, la retention
	// scandisce a ogni tick tutto lo storico, anche quando non c'è nulla da cancellare.
	IndexWorkItemPurge = "ix_workitem_purge"
)

// ExpectedIndexes è l'elenco che le verifiche di avvio confrontano con ciò che esiste davvero.
// Gli indici NON vengono creati in automatico (gestione via EnsureIndexes o migration): il warning
// serve a rendere l'eventuale assenza una scelta consapevole, non una svista.
var ExpectedIndexes = []string{
	IndexWorkItemActive,
	IndexWorkItemClaim,
	IndexWorkItemOrphan,
	IndexWorkItemPurge,
}

// MissingIndexes ritorna gli indici attesi che non compaiono fra quelli presenti.
func MissingIndexes(presenti []string) []string {
	var mancanti []string
	for _, nome := range ExpectedIndexes {
		if !slices.Contains(presenti, nome) {
			mancanti = append(mancanti, nome)
		}
	}
	return mancanti
}

// WarnMissingIndexes segnala gli indici assenti. `comeCrearli` è il rimedio specifico del backend
// (es. "mongostore.EnsureIndexes"), che è l'unica parte del messaggio che dipende da chi chiama.
//
// L'indice unico ha una riga sua perché la sua assenza non è un problema di prestazioni ma di
// CORRETTEZZA: senza, la deduplica di InsertIfNotActive non avviene affatto.
func WarnMissingIndexes(presenti []string, comeCrearli string) {
	mancanti := MissingIndexes(presenti)
	if len(mancanti) == 0 {
		return
	}
	if slices.Contains(mancanti, IndexWorkItemActive) {
		log.Warn().Str("collection", CollectionWorkItems).Msgf(
			"go-core-batch: indice parziale unico %q ASSENTE — InsertIfNotActive NON deduplica (rischio work item duplicati / doppia esecuzione)",
			IndexWorkItemActive)
	}
	log.Warn().Str("collection", CollectionWorkItems).Strs("indici", mancanti).Msgf(
		"go-core-batch: indici ASSENTI su %s — il claim di ogni tick (e la retention) scandiscono l'intera collection invece del solo lavoro da fare. Crearli via %s o migration, oppure confermare che l'assenza è voluta.",
		CollectionWorkItems, comeCrearli)
}
