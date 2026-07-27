// Package gocronlock adapts the neutral go-core-app/lock.Locker primitive to the
// gocron.Locker interface. It is the ONLY place in the batch subsystem tied to
// gocron's locking API: the backend lockers (Redis/Mongo/SQL) implement the
// neutral lock.Locker and stay gocron-agnostic.
package gocronlock

import (
	"context"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/lock"
	gocron "github.com/go-co-op/gocron/v2"
)

// Adapt wraps a neutral lock.Locker as a gocron.Locker. When Acquire returns an
// error (contention via lock.ErrNotAcquired, or a backend failure) gocron skips
// the run this tick — exactly the dispatch-dedup behaviour we want.
func Adapt(l lock.Locker) gocron.Locker {
	return gocronLocker{l: l}
}

type gocronLocker struct{ l lock.Locker }

func (g gocronLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	h, err := g.l.Acquire(ctx, key)
	if err != nil {
		return nil, err
	}
	return gocronLock{h: h}, nil
}

type gocronLock struct{ h lock.Handle }

func (g gocronLock) Unlock(ctx context.Context) error {
	return g.h.Release(ctx)
}
