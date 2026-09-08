package batchmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Questi due helper sostituiscono prometheus/testutil di proposito: testutil trascina
// github.com/kylelemons/godebug, e un helper di test non deve aggiungere un modulo al set di
// dipendenze della libreria. Sono una decina di righe contro una riga di go.mod.

// counterDelta cattura il valore corrente di una serie e ritorna la funzione che, chiamata dopo
// l'azione, ne dà l'INCREMENTO.
//
// Le asserzioni sui contatori devono essere sui delta, mai sui valori assoluti: i collector sono
// globali di processo, quindi con `go test -count=2` (o con un altro test che tocca le stesse
// label) il secondo giro li trova già valorizzati dal primo e un'asserzione su "== 1" fallisce
// contando 2.
func counterDelta(t *testing.T, c prometheus.Counter) func() float64 {
	t.Helper()
	before := counterValue(t, c)
	return func() float64 { return counterValue(t, c) - before }
}

// counterValue legge il valore corrente di una singola serie.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("lettura del counter fallita: %v", err)
	}
	return m.GetCounter().GetValue()
}

// seriesCount conta le serie attualmente esistenti in un collector. Serve dove l'asserzione è
// "la serie non deve nemmeno essere creata": WithLabelValues la creerebbe a zero, quindi
// leggerne il valore non distinguerebbe "non incrementata" da "mai toccata".
func seriesCount(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 1024)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	n := 0
	for range ch {
		n++
	}
	return n
}
