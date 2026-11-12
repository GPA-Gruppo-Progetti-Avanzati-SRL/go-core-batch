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
