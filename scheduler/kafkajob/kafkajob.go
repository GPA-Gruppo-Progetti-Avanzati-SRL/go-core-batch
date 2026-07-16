// Package kafkajob reads pending WorkItems from the store and sends them to Kafka.
// It is intentionally separated from the scheduler package so that applications
// that do not need Kafka do not pull franz-go into their dependency graph.
//
// The store.IWorkItemStore must be provided separately by the application:
//
//	// MongoDB backend:
//	fx.Options(kafkajob.Module(), fx.Provide(mongostore.NewWorkItemData))
//
//	// SQL backend:
//	fx.Options(kafkajob.Module(), fx.Provide(sqlstore.NewWorkItemDataSQL))
//
// Usage — manual registration:
//
//	kafkajob.Register(producer, workItemStore)
package kafkajob

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"

	"go.uber.org/fx"
)

const JobType = "NotificationKafka"

// Register adds the NotificationKafka job type to the scheduler registry.
func Register(producer *kafka.ProducerService, items store.IWorkItemStore) {
	scheduler.Jobs[JobType] = makeNotificationJobFactory(producer, items)
}

// Module wires the Kafka producer and registers the job (stile fx.Options, per composizione con fx).
// The app must separately provide store.IWorkItemStore (mongostore or sqlstore).
func Module() fx.Option {
	return fx.Options(
		fx.Provide(kafka.NewProducerService),
		fx.Invoke(Register),
	)
}

// ModuleCore è la variante core-style di Module: provvede il ProducerService e registra il job
// nel container go-core-app (Provides + Invoke), incondizionatamente.
// L'app deve fornire *kafka.Config e store.IWorkItemStore.
func ModuleCore() {
	core.Provides(kafka.NewProducerService)
	core.Invoke(Register)
}

// ModuleIf è come ModuleCore ma attivo solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvidesIf(kafka.NewProducerService, modes...)
	core.InvokeIf(Register, modes...)
}
