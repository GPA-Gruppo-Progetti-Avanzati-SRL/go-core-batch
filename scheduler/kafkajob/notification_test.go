package kafkajob

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestPrepareMessagesBsonPayload riproduce il payload di un WI NotificationKafka riletto da
// Mongo (Payload = bson.D con sottodocumenti bson.D per messageValue e messageHeaders) e
// verifica che prepareMessages: (1) non lo scarti come "invalid payload", (2) mappi gli header
// a map[string]string, (3) renda messageValue serializzabile come OGGETTO JSON (non array).
func TestPrepareMessagesBsonPayload(t *testing.T) {
	item := &store.WorkItem{
		Id: "BR-test-1",
		Payload: bson.D{
			{Key: "messageKey", Value: "BR-test-1"},
			{Key: "messageValue", Value: bson.D{
				{Key: "numeroOrdine", Value: "584"},
				{Key: "stato", Value: "KO"},
				{Key: "datiRicarica", Value: bson.D{{Key: "importoRicarica", Value: 1.23}}},
			}},
			{Key: "messageHeaders", Value: bson.D{
				{Key: "canale", Value: "APBP"},
				{Key: "stato-operazione", Value: "KO"},
			}},
		},
	}

	// Caso SQL: bun rilegge la colonna jsonb come map[string]interface{} (tipi già nativi).
	sqlItem := &store.WorkItem{
		Id: "BR-test-sql",
		Payload: map[string]interface{}{
			"messageKey":   "BR-test-sql",
			"messageValue": map[string]interface{}{"numeroOrdine": "584", "stato": "KO"},
			"messageHeaders": map[string]interface{}{
				"canale":           "APBP",
				"stato-operazione": "KO",
			},
		},
	}

	for _, item := range []*store.WorkItem{item, sqlItem} {
		ids, msgs := prepareMessages([]*store.WorkItem{item})
		if len(ids) != 1 || len(msgs) != 1 {
			t.Fatalf("[%s] atteso 1 messaggio, ottenuti ids=%d msgs=%d (payload scartato come invalid?)", item.Id, len(ids), len(msgs))
		}
		m := msgs[0]
		if m.MessageHeader["canale"] != "APBP" || m.MessageHeader["stato-operazione"] != "KO" {
			t.Fatalf("[%s] header non mappati correttamente: %#v", item.Id, m.MessageHeader)
		}
		b, err := json.Marshal(m.MessageValue)
		if err != nil {
			t.Fatalf("[%s] json.Marshal(messageValue) fallito: %v", item.Id, err)
		}
		if !strings.HasPrefix(strings.TrimSpace(string(b)), "{") {
			t.Fatalf("[%s] messageValue serializzato come NON-oggetto (regressione bson.D): %s", item.Id, string(b))
		}
		if !strings.Contains(string(b), `"numeroOrdine":"584"`) {
			t.Fatalf("[%s] messageValue serializzato in modo inatteso: %s", item.Id, string(b))
		}
	}
}
