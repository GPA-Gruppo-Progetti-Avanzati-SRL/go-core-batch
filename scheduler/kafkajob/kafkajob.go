// Package kafkajob reads pending WorkItems from the store and sends them to Kafka.
// It is intentionally separated from the scheduler package so that applications
// that do not need Kafka do not pull franz-go into their dependency graph.
//
// Wiring (stile go-core-app): chiamare kafkajob.Module(cfg, modes...) in un
// init(). La *kafka.Config viene passata come parametro e fornita a fx dal Module stesso (core.Supply
// interno): l'app non deve più fare core.Supply. L'app fornisce store.IWorkItemStore chiamando
// mongostore.Module() oppure sqlstore.Module().
//
// Il ProducerService Kafka vive nel package internal/kafkaproducer: NON è importabile dalle
// applicazioni, quindi non è iniettabile in un runner. Per inviare una notifica si crea un
// WorkItem di tipo "NotificationKafka" (outbox) — non si pubblica inline.
package kafkajob

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/kafkaproducer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/kafka"
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

// Module registra il job NotificationKafka nel container go-core-app (Provide del producer +
// Invoke di Register). Il producer è del package internal kafkaproducer, quindi non iniettabile
// fuori dalla libreria. Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è
// tra i modes indicati.
func Module(cfg *kafka.Config, modes ...string) {
	core.Supply(cfg, modes...)
	core.Provide(kafkaproducer.NewProducerService, modes...)
	core.Invoke(Register, modes...)
}
