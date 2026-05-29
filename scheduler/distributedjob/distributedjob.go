// Package distributedjob provides a job that atomically claims WorkItems and dispatches them.
// Claiming is always active: items are marked IN_PROGRESS before dispatch, orphaned items
// are re-claimed at each run, and workers close the lifecycle with MarkDone/MarkFailed.
//
// Deployment modes for ITaskDispatcher:
//
//   - In-process: use localdispatcher.New — runs the task inline, no gRPC needed.
//   - Distributed gRPC: import distributedjob/grpcdispatcher and use GrpcModule().
//
// Usage — manual registration:
//
//	distributedjob.Register(dispatcher, workItemStore, data)
//
// Config example:
//
//	scheduler:
//	  - name: "my-job"
//	    type: "DistribuiteTask"
//	    cron: "0 * * * *"
//	    lock-timeout: 15m   # how long before an IN_PROGRESS item is considered orphaned
//	    properties:
//	      task:  "myTaskType"
//	      limit: "100"
package distributedjob

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

const JobType = "DistribuiteTask"

// Register adds the DistribuiteTask job to the scheduler registry.
// Workitems are populated externally (via API, another process, or manually).
// Must be called before NewScheduler.
func Register(dispatcher ITaskDispatcher, items store.IWorkItemStore, data store.IData) {
	scheduler.Jobs[JobType] = makeClaimingFactory(dispatcher, items, nil, data)
}

// RegisterWithFeed adds the DistribuiteTask job with an external query source.
// On each run, qs is polled for object IDs; new workitems are created for IDs
// that have no active (PENDING or IN_PROGRESS) entry, then claiming proceeds as normal.
// Use this when the trigger source is an external table or collection.
func RegisterWithFeed(dispatcher ITaskDispatcher, items store.IWorkItemStore, qs IQueryStore, data store.IData) {
	scheduler.Jobs[JobType] = makeClaimingFactory(dispatcher, items, qs, data)
}
