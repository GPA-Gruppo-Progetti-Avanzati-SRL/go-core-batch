// Package distributedjob provides a job that atomically claims WorkItems and dispatches them.
// Claiming is always active: items are marked IN_PROGRESS before dispatch, orphaned items
// are re-claimed at each run, and workers close the lifecycle with MarkDone/MarkFailed.
//
// Three job types are available:
//
//   - DistribuiteTask: workitems are populated externally (API, another process, manually).
//   - DistribuiteTaskByQuery: workitems are fed from a DB query via IQueryStore each tick.
//   - DistribuiteTaskByS3File: workitems are fed from S3 file listings each tick.
//
// Deployment modes for ITaskDispatcher:
//
//   - In-process: use localdispatcher.New — runs the task inline, no gRPC needed.
//   - Distributed gRPC: import distributedjob/grpcdispatcher and use Module().
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

const (
	JobType         = "DistribuiteTask"
	JobTypeByQuery  = "DistribuiteTaskByQuery"
	JobTypeByS3File = "DistribuiteTaskByS3File"
)

// Register adds the DistribuiteTask job to the scheduler registry.
// Workitems are populated externally (via API, another process, or manually).
// Must be called before NewScheduler.
func Register(dispatcher ITaskDispatcher, items store.IWorkItemStore, data store.IData) {
	scheduler.Jobs[JobType] = makeClaimingFactory(dispatcher, items, nil, data)
}

// RegisterByQuery adds the DistribuiteTaskByQuery job to the scheduler registry.
// On each run, qs is polled for object IDs via IQueryStore; new workitems are created
// for IDs that have no active (PENDING or IN_PROGRESS) entry, then claiming proceeds.
func RegisterByQuery(dispatcher ITaskDispatcher, items store.IWorkItemStore, qs IQueryStore, data store.IData) {
	scheduler.Jobs[JobTypeByQuery] = makeClaimingFactory(dispatcher, items, NewQueryFeed(qs), data)
}

// RegisterByS3File adds the DistribuiteTaskByS3File job to the scheduler registry.
// On each run, the provided IFeedSource lists S3 objects matching a pattern;
// new workitems are created for keys not already active, then claiming proceeds.
func RegisterByS3File(dispatcher ITaskDispatcher, items store.IWorkItemStore, feed IFeedSource, data store.IData) {
	scheduler.Jobs[JobTypeByS3File] = makeClaimingFactory(dispatcher, items, feed, data)
}
