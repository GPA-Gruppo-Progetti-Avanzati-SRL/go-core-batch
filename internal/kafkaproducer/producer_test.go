package kafkaproducer

import (
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"go.uber.org/fx/fxtest"
)

func TestTransactionalID(t *testing.T) {
	cases := []struct {
		name   string
		field  string
		wantID string
		wantTx bool
	}{
		{"assente", "", "", false},
		{"valorizzato", "tx-1", "tx-1", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, tx := transactionalID(kafka.ProducerConfig{TransactionalID: c.field})
			if id != c.wantID || tx != c.wantTx {
				t.Fatalf("transactionalID = (%q, %v), want (%q, %v)", id, tx, c.wantID, c.wantTx)
			}
		})
	}
}

// TestNewProducerService_FailFast verifica che il costruttore fallisca (fail-fast) quando
// manca transactional.id, e abbia successo quando è presente.
func TestNewProducerService_FailFast(t *testing.T) {
	t.Run("senza transactional.id fallisce", func(t *testing.T) {
		// lifecycle nil è sicuro: la validazione ritorna prima di lc.Append.
		if _, err := NewProducerService(nil, &kafka.Config{}); err == nil {
			t.Fatal("atteso errore per transactional.id mancante, ottenuto nil")
		}
	})

	t.Run("con transactional.id ha successo", func(t *testing.T) {
		lc := fxtest.NewLifecycle(t)
		cfg := &kafka.Config{Producer: kafka.ProducerConfig{TransactionalID: "tx-ok"}}
		ks, err := NewProducerService(lc, cfg)
		if err != nil || ks == nil {
			t.Fatalf("atteso producer valido, ottenuto (%v, %v)", ks, err)
		}
	})
}
