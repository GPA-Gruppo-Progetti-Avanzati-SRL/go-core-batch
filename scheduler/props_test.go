package scheduler

import (
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
)

func jobProps(p properties.Properties) Props {
	return JobProps("il-job", Config{Type: "IlType", Properties: p})
}

// Un errore di property deve nominare job, type e la property, e dire a cosa serviva: è ciò che
// distingue un messaggio su cui si agisce da «configurazione non valida». Prima ogni job type se
// lo scriveva da sé, e simplejob per esempio non allegava nemmeno il codice BATCH-JOB-PROPS.
func TestProps_MessaggiEObbligatorieta(t *testing.T) {
	j := jobProps(properties.Properties{"vuota": "", "task": "import", "limit": 10, "eta": "24h"})

	if _, err := j.RequiredString("task", "serve"); err != nil {
		t.Fatalf("una property valorizzata non deve dare errore: %v", err)
	}

	casi := map[string]error{
		"mancante": erroreDi(func() error { _, e := j.RequiredString("assente", "non si sa quale task eseguire"); return e }),
		"vuota":    erroreDi(func() error { _, e := j.RequiredString("vuota", "serve"); return e }),
		"int":      erroreDi(func() error { _, e := j.RequiredPositiveInt("assente", "serve"); return e }),
		"durata":   erroreDi(func() error { _, e := j.RequiredPositiveDuration("assente", "serve"); return e }),
	}
	for nome, err := range casi {
		t.Run(nome, func(t *testing.T) {
			if err == nil {
				t.Fatal("atteso un errore")
			}
			for _, atteso := range []string{`"il-job"`, `"IlType"`} {
				if !strings.Contains(err.Error(), atteso) {
					t.Errorf("il messaggio non nomina %s: %s", atteso, err)
				}
			}
		})
	}
	if e := casi["mancante"]; !strings.Contains(e.Error(), "non si sa quale task eseguire") {
		t.Errorf("il messaggio non riporta il perché: %s", e)
	}
}

// Una property scritta ma non convertibile NON ricade sul default: un refuso su `limit` cambia
// quanto lavoro fa un tick, e deve fermare l'avvio invece di degradare in silenzio.
func TestProps_ValoreInvalidoNonRicadeSulDefault(t *testing.T) {
	j := jobProps(properties.Properties{"limit": "molti", "eta": "tantissimo"})

	if n, err := j.PositiveInt("limit", 100); err == nil {
		t.Fatalf("un limit non numerico deve essere un errore, non il default: ho avuto %d", n)
	}
	if _, err := j.RequiredPositiveDuration("eta", "serve"); err == nil {
		t.Fatal("una durata non parsabile deve essere un errore")
	}
	// Property assente: lì il default è la condotta giusta.
	if n, err := j.PositiveInt("assente", 100); err != nil || n != 100 {
		t.Fatalf("PositiveInt su property assente = (%d, %v), atteso (100, nil)", n, err)
	}
}

func erroreDi(f func() error) error { return f() }
