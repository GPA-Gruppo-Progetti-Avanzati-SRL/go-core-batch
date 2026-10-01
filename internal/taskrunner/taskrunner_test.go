package taskrunner

import (
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/task"
)

// Stessa semantica sul contratto dei file runner: senza il campo, `max-retry` valeva per Register e
// non per RegisterFile — un knob che vale a metà è peggio di un knob che non vale.
func TestFileTaskRunner_PortaIlTetto(t *testing.T) {
	fr := NewFile("s3-in", nil)
	if got := fr.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("assenza: atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
	if got := fr.WithMaxRetry(5).ResolveMaxRetry(); got != 5 {
		t.Fatalf("atteso 5, ottenuto %d", got)
	}
	var nilFile *FileTaskRunner
	if got := nilFile.ResolveMaxRetry(); got != task.MaxRetryUnlimited {
		t.Fatalf("receiver nil: atteso %d, ottenuto %d", task.MaxRetryUnlimited, got)
	}
}
