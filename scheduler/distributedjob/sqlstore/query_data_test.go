package sqlstore

import "testing"

func TestValidateIdent(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		allowDot bool
		wantErr  bool
	}{
		{"tabella semplice", "users", true, false},
		{"schema-qualified", "public.users", true, false},
		{"underscore iniziale", "_tbl", true, false},
		{"con cifre", "col_1", false, false},
		{"colonna semplice", "created_at", false, false},
		{"colonna con dot non ammesso", "public.users", false, true},
		{"tre parti dotted", "a.b.c", true, true},
		{"injection", "users; DROP TABLE x", true, true},
		{"spazio", "col name", false, true},
		{"trattino", "col-1", false, true},
		{"cifra iniziale", "1col", false, true},
		{"vuoto", "", true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateIdent("x", c.s, c.allowDot)
			if (err != nil) != c.wantErr {
				t.Fatalf("validateIdent(%q, allowDot=%v) err=%v, wantErr=%v", c.s, c.allowDot, err, c.wantErr)
			}
		})
	}
}
