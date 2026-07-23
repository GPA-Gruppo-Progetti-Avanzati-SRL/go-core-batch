// Package queryfeed provides the Fx module for the DistribuiteTaskByQuery job type.
// It wires an IQueryStore-based feed into the distributedjob claiming pipeline.
//
// Usage:
//
//	func init() { queryfeed.Module() }
package queryfeed

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler/distributedjob"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

func registerByQuery(d distributedjob.ITaskDispatcher, items store.IWorkItemStore, qs distributedjob.IQueryStore, data store.IData) {
	distributedjob.RegisterByQuery(d, items, qs, data)
}

// Module registers the DistribuiteTaskByQuery job type. It requires an
// ITaskDispatcher, IWorkItemStore, IQueryStore, and IData to be provided in the
// fx container. Se modes è vuoto registra sempre; altrimenti solo quando core.Mode
// è tra i modes indicati (per binari multi-mode).
func Module(modes ...string) {
	core.Invoke(registerByQuery, modes...)
}
