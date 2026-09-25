// Package gocronlock adatta il corelock.Locker di go-core-locker all'interfaccia gocron.Locker.
//
// È l'UNICO punto del sottosistema batch legato all'API di locking di gocron: lo scheduler riceve
// da fx un corelock.Locker — quale backend lo serva è una scelta dell'applicazione, fatta in
// corelock.Module — e tutto il resto di batch resta agnostico rispetto a gocron.
package gocronlock

import (
	"context"

	corelock "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-locker"
	gocron "github.com/go-co-op/gocron/v2"
)

// Adapt wraps a neutral corelock.Locker as a gocron.Locker. When Acquire returns an
// error (contention via lock.ErrNotAcquired, or a backend failure) gocron skips
// the run this tick — exactly the dispatch-dedup behaviour we want.
func Adapt(l corelock.Locker) gocron.Locker {
	return gocronLocker{l: l}
}

type gocronLocker struct{ l corelock.Locker }

func (g gocronLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	h, err := g.l.Acquire(ctx, key)
	if err != nil {
		return nil, err
	}
	return gocronLock{h: h}, nil
}

type gocronLock struct{ h corelock.Handle }

func (g gocronLock) Unlock(ctx context.Context) error {
	return g.h.Release(ctx)
}
