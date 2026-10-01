package scheduler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"

	gocron "github.com/go-co-op/gocron/v2"
	"go.uber.org/fx"
)

// JobFactory is the function type used to create a gocron.Task for a registered job type.
// The full Config is passed so factories can access both Properties and typed fields (e.g. LockTimeout).
//
// Non riceve altro: c'era un terzo parametro *Services — una struct con il solo store.IData — che
// newScheduler costruiva e passava a ogni factory, e che NESSUNA delle cinque usava (tre lo
// ricevevano già come `_`). Era una dipendenza che il contratto pubblico imponeva di dichiarare a
// chi scrive un job type, e che nessuno poteva consumare: i job che di IData hanno bisogno — il
// dispatch di distributedjob, la retention di purgejob — se lo fanno iniettare da fx nel proprio
// costruttore, come ogni altra dipendenza.
type JobFactory = func(name string, config Config) gocron.Task

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
	// Check, se valorizzato, verifica al boot che la voce di `jobs:` sia eseguibile IN QUESTO
	// PROCESSO: newScheduler la chiama per ogni job attivo del tipo prima di costruirlo, e un
	// errore fa fallire l'avvio. Serve ai job che eseguono in linea (SingleTask, DistribuiteTask*
	// col localdispatcher): lì un task referenziato ma senza runner — escluso dai modes di
	// runner.Register, o col type non registrato in questo binario — è una configurazione che non
	// può funzionare, e senza il controllo si manifestava solo a runtime, item per item
	// (MarkFailed + "no runner registered for task name"). nil = nessun controllo, ed è il caso
	// dei job il cui esecutore non è noto qui (dispatch gRPC verso un worker in un altro processo).
	Check func(name string, config Config) error
}

// ProvideJob registra un costruttore che ritorna una JobRegistration nel value group
// batch_jobs, eliminando la vecchia scrittura della mappa globale via side-effect. Il
// costruttore può dichiarare qualunque dipendenza fx-iniettabile. Per un costruttore che
// ritorna []JobRegistration usare il tag group con ",flatten" direttamente (vedi simplejob).
// modes opzionale, coerente con gli altri Provide.
func ProvideJob(constructor any, modes ...string) {
	core.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"`+JobGroup+`"`)), modes...)
}
