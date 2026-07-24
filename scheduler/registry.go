package scheduler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	gocron "github.com/go-co-op/gocron/v2"
	"go.uber.org/fx"
)

// JobFactory is the function type used to create a gocron.Task for a registered job type.
// The full Config is passed so factories can access both Properties and typed fields (e.g. LockTimeout).
type JobFactory = func(name string, services *Services, config Config) gocron.Task

// JobGroup is the fx value group into which all job type registrations are collected.
// newScheduler consumes the whole group and builds its lookup map from it, so the
// registration order no longer matters: fx resolves every group contributor before
// constructing the scheduler (this replaces the old global Jobs map read at build time).
const JobGroup = "batch_jobs"

// JobRegistration binds a job type to its factory. Job packages provide it into the
// batch_jobs fx group via ProvideJob; newScheduler builds its factory map from the group.
type JobRegistration struct {
	Type    string
	Factory JobFactory
}

// ProvideJob registra un costruttore che ritorna una JobRegistration nel value group
// batch_jobs, eliminando la vecchia scrittura della mappa globale via side-effect. Il
// costruttore può dichiarare qualunque dipendenza fx-iniettabile. Per un costruttore che
// ritorna []JobRegistration usare il tag group con ",flatten" direttamente (vedi simplejob).
// modes opzionale, coerente con gli altri Provide.
func ProvideJob(constructor any, modes ...string) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+JobGroup+`"`)), modes...)
}

// Services is passed to each JobFactory. È costruita da newScheduler a partire dallo
// store.IData iniettato — non è più una struct fx.In iniettata direttamente.
type Services struct {
	Data store.IData
}
