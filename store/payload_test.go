package store

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Il payload riletto da Mongo arriva come bson.D — una lista ordinata di coppie — e non come
// mappa. È la forma che il convertitore di s3feed non gestiva: passava per json.Marshal, che su
// una lista produce un ARRAY, quindi la Unmarshal in una struct falliva e il job
// DistribuiteTaskByS3File non decodificava mai il proprio payload sul backend Mongo.
func TestPayloadMap_FormeDeiBackend(t *testing.T) {
	atteso := func(t *testing.T, m map[string]any, ok bool) {
		t.Helper()
		if !ok {
			t.Fatalf("payload non riconosciuto come documento")
		}
		if m["service"] != "s3" {
			t.Fatalf("service = %v, atteso s3", m["service"])
		}
	}

	t.Run("bson.D (Mongo)", func(t *testing.T) {
		m, ok := PayloadMap(bson.D{{Key: "service", Value: "s3"}, {Key: "key", Value: "f.csv"}})
		atteso(t, m, ok)
	})
	t.Run("bson.M (Mongo)", func(t *testing.T) {
		m, ok := PayloadMap(bson.M{"service": "s3"})
		atteso(t, m, ok)
	})
	t.Run("map (SQL jsonb)", func(t *testing.T) {
		m, ok := PayloadMap(map[string]any{"service": "s3"})
		atteso(t, m, ok)
	})
	t.Run("[]byte (SQL jsonb grezzo)", func(t *testing.T) {
		m, ok := PayloadMap([]byte(`{"service":"s3"}`))
		atteso(t, m, ok)
	})
	t.Run("string JSON", func(t *testing.T) {
		m, ok := PayloadMap(`{"service":"s3"}`)
		atteso(t, m, ok)
	})

	t.Run("sottodocumento annidato", func(t *testing.T) {
		m, ok := PayloadMap(bson.D{{Key: "hdr", Value: bson.D{{Key: "a", Value: "1"}}}})
		if !ok {
			t.Fatal("non riconosciuto")
		}
		inner, ok := m["hdr"].(map[string]any)
		if !ok || inner["a"] != "1" {
			t.Fatalf("sottodocumento non convertito: %#v", m["hdr"])
		}
	})
	t.Run("lista di documenti", func(t *testing.T) {
		m, _ := PayloadMap(bson.D{{Key: "l", Value: bson.A{bson.D{{Key: "a", Value: "1"}}}}})
		l, ok := m["l"].([]any)
		if !ok || len(l) != 1 {
			t.Fatalf("lista non convertita: %#v", m["l"])
		}
		if el, ok := l[0].(map[string]any); !ok || el["a"] != "1" {
			t.Fatalf("elemento non convertito: %#v", l[0])
		}
	})

	t.Run("non è un documento", func(t *testing.T) {
		for _, p := range []any{nil, 42, "non json", []any{1, 2}} {
			if _, ok := PayloadMap(p); ok {
				t.Fatalf("%#v non doveva essere riconosciuto come documento", p)
			}
		}
	})
}

// Gli scalari NON devono passare da JSON: un int64 che diventa float64 e torna indietro cambia
// cifre oltre i 2^53, e il payload di un WorkItem finisce su Kafka come chiave di partizionamento.
func TestPayloadMap_ConservaGliInteriGrandi(t *testing.T) {
	const grande = int64(1234567890123456789)
	m, ok := PayloadMap(bson.D{{Key: "id", Value: grande}})
	if !ok {
		t.Fatal("non riconosciuto")
	}
	if got, isInt := m["id"].(int64); !isInt || got != grande {
		t.Fatalf("id = %#v, atteso l'int64 %d invariato", m["id"], grande)
	}
}

type s3ish struct {
	Service string `json:"service"`
	Key     string `json:"key"`
	Dest    string `json:"destPath"`
}

func TestDecodePayload(t *testing.T) {
	verifica := func(t *testing.T, raw any) {
		t.Helper()
		var out s3ish
		if err := DecodePayload(raw, &out); err != nil {
			t.Fatalf("DecodePayload(%T): %v", raw, err)
		}
		if out.Service != "s3" || out.Key != "f.csv" {
			t.Fatalf("decodifica errata da %T: %+v", raw, out)
		}
	}

	// La regressione: prima questa riga ritornava un errore di unmarshal ("cannot unmarshal array").
	verifica(t, bson.D{{Key: "service", Value: "s3"}, {Key: "key", Value: "f.csv"}})
	verifica(t, bson.M{"service": "s3", "key": "f.csv"})
	verifica(t, map[string]any{"service": "s3", "key": "f.csv"})
	verifica(t, []byte(`{"service":"s3","key":"f.csv"}`))
	verifica(t, `{"service":"s3","key":"f.csv"}`)
	// Payload mai passato da un backend: assegnato direttamente, senza giro da JSON.
	verifica(t, s3ish{Service: "s3", Key: "f.csv"})
	verifica(t, &s3ish{Service: "s3", Key: "f.csv"})

	t.Run("destinazione non valida", func(t *testing.T) {
		var out s3ish
		if err := DecodePayload(map[string]any{}, out); err == nil {
			t.Fatal("un out non puntatore deve essere un errore")
		}
		if err := DecodePayload(nil, &out); err == nil {
			t.Fatal("un payload assente deve essere un errore")
		}
	})
}
