package gocronlock

import (
	"context"
	"errors"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/lock"
)

type fakeLocker struct {
	acquireErr error
}

type fakeHandle struct {
	released bool
}

func (f *fakeLocker) Acquire(ctx context.Context, key string, opts ...lock.AcquireOption) (lock.Handle, error) {
	if f.acquireErr != nil {
		return nil, f.acquireErr
	}
	return &fakeHandle{}, nil
}

func (h *fakeHandle) Release(ctx context.Context) error {
	h.released = true
	return nil
}

func (h *fakeHandle) Extend(ctx context.Context) error {
	return nil
}

// TestAdaptLockUnlock verifies the happy path: Lock acquires the neutral handle
// and Unlock releases it.
func TestAdaptLockUnlock(t *testing.T) {
	g := Adapt(&fakeLocker{})
	l, err := g.Lock(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("Lock returned unexpected error: %v", err)
	}
	if err := l.Unlock(context.Background()); err != nil {
		t.Fatalf("Unlock returned unexpected error: %v", err)
	}
}

// TestAdaptContentionPropagates verifies that a failed Acquire (contention or
// backend error) surfaces as a Lock error, so gocron skips the run this tick.
func TestAdaptContentionPropagates(t *testing.T) {
	g := Adapt(&fakeLocker{acquireErr: lock.ErrNotAcquired})
	if _, err := g.Lock(context.Background(), "job-1"); !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired to propagate, got %v", err)
	}
}
