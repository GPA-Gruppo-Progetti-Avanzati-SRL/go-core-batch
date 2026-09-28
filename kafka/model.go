// Package kafka contiene la FORMA del payload di un WorkItem `NotificationKafka`: la struct che
// l'applicazione costruisce quando accoda una notifica, e le chiavi con cui il job
// scheduler/kafkajob la rilegge.
//
// Non contiene alcun client Kafka: il producer è quello di go-core-kafka, wirato dall'app.
package kafka

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
)

type Message struct {
	MessageKey    any               `json:"messageKey" bson:"messageKey"`                   //DO NOT EDIT
	MessageValue  any               `json:"messageValue" bson:"messageValue"`               //DO NOT EDIT
	MessageHeader map[string]string `json:"messageHeaders" bson:"messageHeaders,omitempty"` //DO NOT EDIT
}
