package scheduler

import (
	"testing"
	"time"
)

func TestResolveTimeouts(t *testing.T) {
	t.Run("LockTimeout non impostato → default separati", func(t *testing.T) {
		run, orphan := Config{}.ResolveTimeouts()
		if run != DefaultRunTimeout {
			t.Fatalf("run = %v, want %v", run, DefaultRunTimeout)
		}
		if orphan != DefaultOrphanTimeout {
			t.Fatalf("orphan = %v, want %v", orphan, DefaultOrphanTimeout)
		}
	})

	t.Run("LockTimeout impostato → governa entrambi", func(t *testing.T) {
		run, orphan := Config{LockTimeout: 2 * time.Minute}.ResolveTimeouts()
		if run != 2*time.Minute || orphan != 2*time.Minute {
			t.Fatalf("(run, orphan) = (%v, %v), want (2m, 2m)", run, orphan)
		}
	})
}
