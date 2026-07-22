// Package kafkajob reads pending WorkItems from the store and sends them to Kafka.
// It is intentionally separated from the scheduler package so that applications
// that do not need Kafka do not pull franz-go into their dependency graph.
//
// Wiring (stile go-core-app): chiamare kafkajob.Module() o kafkajob.ModuleIf(modes...) in un
// init(). L'app deve fornire *kafka.Config (core.Supply) e store.IWorkItemStore via core.Provides
// (mongostore.NewWorkItemData oppure sqlstore.NewWorkItemDataSQL).
//
// Il ProducerService Kafka vive nel package internal/kafkaproducer: NON è importabile dalle
// applicazioni, quindi non è iniettabile in un runner. Per inviare una notifica si crea un
// WorkItem di tipo "NotificationKafka" (outbox) — non si pubblica inline.
package kafkajob

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/kafkaproducer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
)

const JobType = "NotificationKafka"

// Register registra il job NotificationKafka nella registry dello scheduler. Consuma il
// ProducerService (dal package internal kafkaproducer, non importabile dalle app) e lo store.
// lc e config vivono nel costruttore kafkaproducer.NewProducerService, non qui.
func Register(producer *kafkaproducer.ProducerService, items store.IWorkItemStore) {
	scheduler.Jobs[JobType] = makeNotificationJobFactory(producer, items)
}

// Module registra il job NotificationKafka nel container go-core-app (Provides del producer +
// Invoke di Register), incondizionatamente. Il producer è del package internal kafkaproducer,
// quindi non iniettabile fuori dalla libreria.
func Module() {
	core.Provides(kafkaproducer.NewProducerService)
	core.Invoke(Register)
}

// ModuleIf è come Module ma attiva solo quando core.Mode è tra i modes indicati.
func ModuleIf(modes ...string) {
	core.ProvidesIf(kafkaproducer.NewProducerService, modes...)
	core.InvokeIf(Register, modes...)
}
