package store_test

import (
	"os/exec"
	"strings"
	"testing"
)

// Ogni backend dello store è un package che l'app sceglie con un import, e quell'import deve
// portare il SUO database e nessun altro: un'app Postgres non compila il codice Oracle né il driver
// Mongo, e viceversa. Le dipendenze si leggono dal grafo dei package (go list -deps), che è ciò
// che il compilatore costruisce davvero; un import aggiunto nel posto sbagliato fa fallire qui
// invece che passare inosservato.
func TestBackend_NonPortanoGliAltriDatabase(t *testing.T) {
	const m = "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/"
	cases := []struct {
		pkg       string
		forbidden []string
	}{
		{"store/sqlstore", []string{"/sqlbackend/oraclebackend", "mongo-driver"}},
		{"scheduler/distributedjob/sqlstore", []string{"/sqlbackend/oraclebackend", "mongo-driver"}},
		{"store/oraclestore", []string{"/sqlbackend/pgbackend", "mongo-driver"}},
		{"scheduler/distributedjob/oraclestore", []string{"/sqlbackend/pgbackend", "mongo-driver"}},
		{"store/mongostore", []string{"uptrace/bun"}},
		{"scheduler/distributedjob/mongostore", []string{"uptrace/bun"}},
	}
	for _, c := range cases {
		t.Run(c.pkg, func(t *testing.T) {
			out, err := exec.Command("go", "list", "-deps", m+c.pkg).Output()
			if err != nil {
				t.Fatalf("go list -deps %s: %v", c.pkg, err)
			}
			deps := strings.Fields(string(out))
			if len(deps) == 0 {
				t.Fatalf("go list -deps %s: nessun package, la verifica non verificherebbe nulla", c.pkg)
			}
			for _, dep := range deps {
				for _, f := range c.forbidden {
					if strings.Contains(dep, f) {
						t.Errorf("%s compila %s", c.pkg, dep)
					}
				}
			}
		})
	}
}
