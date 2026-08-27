package store

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/page"
)

// markCall registra l'ultima transizione richiesta allo store.
type markCall struct {
	op    string
	id    string
	token string
	after time.Duration
}

type fakeStore struct{ last markCall }

func (f *fakeStore) MarkDone(_ context.Context, ids []string, token string) *core.ApplicationError {
	f.last = markCall{op: "done", id: ids[0], token: token}
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, id, token, _ string) *core.ApplicationError {
	f.last = markCall{op: "failed", id: id, token: token}
	return nil
}
func (f *fakeStore) MarkPending(_ context.Context, id, token string, after time.Duration) *core.ApplicationError {
	f.last = markCall{op: "pending", id: id, token: token, after: after}
	return nil
}

// Resto dell'interfaccia: non esercitato da ApplyResult.
func (f *fakeStore) FindPending(context.Context, string, string, string) ([]*WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) ClaimPending(context.Context, string, string, string, int) ([]*WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) RecoverOrphans(context.Context, string, string, string, time.Duration, int) ([]*WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) Insert(context.Context, []*WorkItem) *core.ApplicationError { return nil }
func (f *fakeStore) InsertIfNotActive(context.Context, []*WorkItem) (int, *core.ApplicationError) {
	return 0, nil
}
func (f *fakeStore) HasActive(context.Context, string, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (f *fakeStore) GetById(context.Context, string) (*WorkItem, *core.ApplicationError) {
	return nil, nil
}
func (f *fakeStore) DeleteIfPending(context.Context, string) (bool, *core.ApplicationError) {
	return false, nil
}
func (f *fakeStore) List(context.Context, string, string, *page.Paging, page.SortRequest) ([]*WorkItem, *core.ApplicationError) {
	return nil, nil
}

func TestApplyResult(t *testing.T) {
	transient := errors.New("sink irraggiungibile")

	tests := []struct {
		name    string
		runErr  error
		outcome Outcome
		op      string
		after   time.Duration
	}{
		{name: "nil → done", runErr: nil, outcome: OutcomeDone, op: "done"},
		{name: "ErrHandled → nessun Mark*", runErr: ErrHandled, outcome: OutcomeHandled, op: ""},
		{name: "RetryError → pending", runErr: Retry(5 * time.Minute), outcome: OutcomeRetry, op: "pending", after: 5 * time.Minute},
		{name: "errore generico → failed", runErr: transient, outcome: OutcomeFailed, op: "failed"},

		// I tre casi sotto sono quelli abilitati da ApplicationError.Unwrap: prima
		// finivano tutti su OutcomeFailed, perché la catena si interrompeva
		// sull'ApplicationError e né errors.Is né errors.AsType la attraversavano.
		{
			name:    "ApplicationError che avvolge RetryError → pending",
			runErr:  core.TechnicalErrorWithError(RetryWithCause(90*time.Second, transient)),
			outcome: OutcomeRetry, op: "pending", after: 90 * time.Second,
		},
		{
			name:    "ApplicationError che avvolge ErrHandled → nessun Mark*",
			runErr:  core.TechnicalErrorWithError(ErrHandled),
			outcome: OutcomeHandled, op: "",
		},
		{
			name:    "ApplicationError con WithCause(RetryError) → pending",
			runErr:  core.TechnicalErrorWithCodeAndMessage("SINK-KO", "sink non raggiungibile").WithCause(Retry(0)),
			outcome: OutcomeRetry, op: "pending", after: 0,
		},

		// Un ApplicationError la cui causa non è né RetryError né ErrHandled resta un
		// errore normale: la causa sintetica di *WithCodeAndMessage non deve dirottare
		// la classificazione.
		{
			name:    "ApplicationError con sola causa sintetica → failed",
			runErr:  core.TechnicalErrorWithCodeAndMessage("TECH500", "boom"),
			outcome: OutcomeFailed, op: "failed",
		},
		{
			name:    "ApplicationError che avvolge un errore estraneo → failed",
			runErr:  core.TechnicalErrorWithError(transient),
			outcome: OutcomeFailed, op: "failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{}
			got, appErr := ApplyResult(context.Background(), fs, "item-1", "tok-1", tc.runErr)
			if appErr != nil {
				t.Fatalf("ApplyResult ha ritornato un errore: %v", appErr)
			}
			if got != tc.outcome {
				t.Errorf("Outcome = %v, atteso %v", got, tc.outcome)
			}
			if fs.last.op != tc.op {
				t.Fatalf("Mark* invocato = %q, atteso %q", fs.last.op, tc.op)
			}
			if tc.op == "" {
				return
			}
			if fs.last.id != "item-1" || fs.last.token != "tok-1" {
				t.Errorf("id/token = %q/%q, attesi item-1/tok-1", fs.last.id, fs.last.token)
			}
			if tc.op == "pending" && fs.last.after != tc.after {
				t.Errorf("retryDelay = %v, atteso %v", fs.last.after, tc.after)
			}
		})
	}
}
