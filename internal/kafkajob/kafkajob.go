// Package kafkajob è l'implementazione del job NotificationKafka: reclama i work item della coda
// nominata da `properties.stream` e li pubblica col producer di go-core-kafka (producer.IProducer),
// che l'app wira con corekafka.ProducerModule. Il guscio pubblico che l'app passa a
// batch.WithModule è scheduler/kafkajob.
package kafkajob

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
)

const jobType = "NotificationKafka"

// register costruisce la JobRegistration del job NotificationKafka. Consuma il producer di
// go-core-kafka (il seam producer.IProducer, wirato dall'app) e lo store.
// È un costruttore fx: il risultato confluisce nel value group batch_jobs via scheduler.ProvideJob.
func register(p producer.IProducer, items store.IWorkItemStore) scheduler.JobRegistration {
	return scheduler.JobRegistration{Type: jobType, Factory: makeNotificationJobFactory(p, items)}
}

// Module registra il job NotificationKafka. Se modes è vuoto registra sempre; altrimenti solo quando
// core.Mode è tra i modes indicati.
//
// Registra SOLO il job: il producer lo fornisce l'app con corekafka.ProducerModule (vedi il doc del
// package). Prima lo costruiva qui un ProducerService interno, che era un secondo client Kafka con la
// sua config, il suo TLS/SASL scritti a mano e nessuna astrazione driver.
func Module(modes ...string) {
	scheduler.ProvideJob(register, modes...)
}
