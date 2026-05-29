package scheduler

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"go.uber.org/fx"
)

// JobFactory is the function type used to create a gocron.Task for a registered job type.
// The full Config is passed so factories can access both Properties and typed fields (e.g. LockTimeout).
type JobFactory = func(name string, services *Services, config Config) gocron.Task

// Jobs is the global registry of available job types.
// Register job types before the scheduler is initialized (e.g. via fx.Invoke).
// Built-in optional packages: distributedjob, kafkajob.
var Jobs = map[string]JobFactory{}

type Services struct {
	fx.In
	Data store.IData
}
