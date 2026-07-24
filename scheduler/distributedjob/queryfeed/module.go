// Package queryfeed provides the Fx module for the DistribuiteTaskByQuery job type.
// It wires an IQueryStore-based feed into the distributedjob claiming pipeline.
//
// Usage:
//
//	func init() { queryfeed.Module() }
package queryfeed

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
)

// Module registers the DistribuiteTaskByQuery job type. It requires an
// ITaskDispatcher, IWorkItemStore, IQueryStore, and IData to be provided in the
// fx container (iniettati da fx nel costruttore RegisterByQuery, il cui risultato
// confluisce nel value group batch_jobs). Se modes è vuoto registra sempre;
// altrimenti solo quando core.Mode è tra i modes indicati (per binari multi-mode).
func Module(modes ...string) {
	scheduler.ProvideJob(distributedjob.RegisterByQuery, modes...)
}
