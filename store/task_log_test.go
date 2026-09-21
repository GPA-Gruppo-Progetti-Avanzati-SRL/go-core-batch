package store

import "testing"

// Il livello decide quali righe di task_logs sopravvivono. Lo zero value deve valere "tutte":
// aggiornare la libreria senza toccare la propria config non deve far sparire dei dati.
func TestTaskLogLevel_Records(t *testing.T) {
	stati := []string{TaskLogStart, TaskLogDone, TaskLogError, TaskLogAssigned, TaskLogAssignedKO}

	for _, l := range []TaskLogLevel{"", TaskLogAll} {
		for _, s := range stati {
			if !l.Records(s) {
				t.Errorf("livello %q: lo stato %s doveva essere scritto", l, s)
			}
		}
	}

	scritti := map[string]bool{TaskLogError: true, TaskLogAssignedKO: true}
	for _, s := range stati {
		if got := TaskLogErrors.Records(s); got != scritti[s] {
			t.Errorf("livello errors: stato %s scritto = %v, atteso %v", s, got, scritti[s])
		}
		if TaskLogOff.Records(s) {
			t.Errorf("livello off: lo stato %s non doveva essere scritto", s)
		}
	}
}

func TestTaskLogLevel_Filter(t *testing.T) {
	logs := []*TaskLog{
		{Stato: TaskLogStart}, {Stato: TaskLogDone}, {Stato: TaskLogError}, {Stato: TaskLogAssignedKO},
	}
	if got := len(TaskLogAll.Filter(logs)); got != 4 {
		t.Errorf("all: %d righe, attese 4", got)
	}
	if got := len(TaskLogErrors.Filter(logs)); got != 2 {
		t.Errorf("errors: %d righe, attese 2", got)
	}
	if got := len(TaskLogOff.Filter(logs)); got != 0 {
		t.Errorf("off: %d righe, attese 0", got)
	}
	// Filter non deve consumare lo slice originale.
	if len(logs) != 4 {
		t.Errorf("lo slice di partenza è stato modificato: %d righe", len(logs))
	}
}

// Un valore non previsto è un ERRORE e non un ripiego silenzioso su "all": chi l'ha scritto
// intendeva qualcosa, e indovinare male significherebbe scrivere (o non scrivere) dati senza che
// nulla lo dica.
func TestParseTaskLogLevel(t *testing.T) {
	ok := map[string]TaskLogLevel{
		"":        TaskLogAll,
		"all":     TaskLogAll,
		"ALL":     TaskLogAll,
		" errors": TaskLogErrors,
		"off":     TaskLogOff,
	}
	for in, want := range ok {
		got, err := ParseTaskLogLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseTaskLogLevel(%q) = %q, %v; atteso %q, nil", in, got, err, want)
		}
	}
	for _, in := range []string{"none", "true", "tutti"} {
		if _, err := ParseTaskLogLevel(in); err == nil {
			t.Errorf("ParseTaskLogLevel(%q): atteso errore, ottenuto nil", in)
		}
	}
}
