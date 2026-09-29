// Package kafka contiene la FORMA del payload di un WorkItem `NotificationKafka` — la struct che
// l'applicazione costruisce quando accoda una notifica, e le chiavi con cui il job
// scheduler/kafkajob la rilegge — più il costruttore del WorkItem che la trasporta.
//
// Non contiene alcun client Kafka: il producer è quello di go-core-kafka, wirato dall'app.
package kafka

import (
	"time"
	"uuid"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

// Chiavi del payload. Sono QUI, accanto ai tag della struct, perché il job che rilegge il payload
// (scheduler/kafkajob) lo trova come mappa — Mongo e SQL restituiscono un documento, non una
// Message — e deve indicizzarla per nome. Prima quei tre nomi erano stringhe letterali dentro il
// job: la struct e il suo unico lettore dichiaravano lo stesso contratto senza conoscersi, e un
// rename dei tag l'avrebbe rotto in silenzio. Che i tag e le costanti coincidano è verificato da
// un test (scheduler/kafkajob: TestChiaviDelPayload_CoincidonoCoiTagDiMessage).
const (
	KeyMessageKey     = "messageKey"
	KeyMessageValue   = "messageValue"
	KeyMessageHeaders = "messageHeaders"
	KeyTopic          = "topic"
)

type Message struct {
	MessageKey    any               `json:"messageKey" bson:"messageKey"`                   //DO NOT EDIT
	MessageValue  any               `json:"messageValue" bson:"messageValue"`               //DO NOT EDIT
	MessageHeader map[string]string `json:"messageHeaders" bson:"messageHeaders,omitempty"` //DO NOT EDIT
	// Topic è FACOLTATIVO: se valorizzato vince sul topic di default del job (`properties.topic`),
	// perché message.ProducerRecord porta già il proprio topic e ProduceTo stampa il default sui
	// soli record che non ne hanno uno. Serve a far drenare a un unico job un flusso che va a
	// topic diversi, invece di moltiplicare i job per moltiplicare le destinazioni.
	Topic string `json:"topic,omitempty" bson:"topic,omitempty"`
}

// NewWorkItem costruisce il WorkItem di una notifica: `stream` è il flusso che il job drena
// (la property `stream` della voce di `jobs:`, che finisce in WorkItem.TaskName), `objectId`
// l'oggetto di dominio a cui la notifica si riferisce.
//
// Esiste perché il contratto di accodamento era finora solo documentato, e l'applicazione doveva
// sapere da sé quali campi il claim pretende — `Status`, `CreateTime`, `NextRunAt`, e soprattutto
// che `TaskName` non è facoltativo: un item senza non viene claimato da nessuno, in silenzio.
//
// L'inserimento resta dell'applicazione, perché è lì che si decide la semantica:
// `Insert` dentro la transazione del dato di dominio (outbox vero e proprio), oppure
// `InsertIfNotActive` per non accodare una seconda notifica finché la prima non è partita —
// la deduplica è sulla coppia (stream, objectId) e richiede l'indice unico parziale.
func NewWorkItem(stream, objectId string, msg Message) *store.WorkItem {
	now := time.Now()
	return &store.WorkItem{
		Id:         uuid.NewV7().String(),
		TaskName:   stream,
		ObjectId:   objectId,
		Payload:    &msg,
		Status:     store.StatusPending,
		CreateTime: now,
		NextRunAt:  &now,
	}
}
