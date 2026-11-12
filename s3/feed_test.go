package s3

import "testing"

func TestFeedResult_Outcome(t *testing.T) {
	casi := []struct {
		res  FeedResult
		want Outcome
	}{
		{FeedResult{}, NothingToDo},
		{FeedResult{Found: 3, Queued: 3}, Queued},
		{FeedResult{Found: 3, Queued: 1}, Queued},
		{FeedResult{Found: 3}, InProgress},
	}
	for _, c := range casi {
		if got := c.res.Outcome(); got != c.want {
			t.Errorf("%+v: got %q, want %q", c.res, got, c.want)
		}
	}
}

func TestSource(t *testing.T) {
	ok := Source{Service: "main", DestPath: "done", Pattern: "*.csv"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("sorgente valida rifiutata: %v", err)
	}
	if ok.ResolvedLimit() != DefaultSourceLimit {
		t.Errorf("limit assente: atteso %d", DefaultSourceLimit)
	}
	for nome, s := range map[string]Source{
		"senza service":   {DestPath: "done"},
		"senza dest-path": {Service: "main"},
		"limit negativo":  {Service: "main", DestPath: "done", Limit: -1},
		"pattern rotto":   {Service: "main", DestPath: "done", Pattern: "[a-"},
	} {
		if err := s.Validate(); err == nil {
			t.Errorf("%s: atteso errore", nome)
		}
	}
}
