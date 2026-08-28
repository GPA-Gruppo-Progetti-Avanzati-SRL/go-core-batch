// Package kafkajob reads pending WorkItems from the store and sends them to Kafka.
// It is intentionally separated from the scheduler package so that applications
// that do not need Kafka do not pull the Kafka client into their dependency graph.
//
// Wiring (stile go-core-app): chiamare kafkajob.Module(modes...) in un init() oppure passarlo a
// batch.WithModule. È modes-only e NON registra più alcun producer: il producer è quello di
// go-core-kafka, che l'APP wira dalla composition root — insieme al driver che ha scelto:
//
//	corekafka.ProducerModule(&svc.Kafka,
//	    corekafka.WithDriver(franzdriver.Driver),   // o driver/confluent
//	    corekafka.WithModes(engine.Scheduler))
//
//	batch.Module(&svc.Batch, Register, ..., batch.WithModule(kafkajob.Module))
//
// Se il producer non è wirato, fx fallisce all'avvio con un "missing type": è il fail-fast previsto,
// non un nil silenzioso. È anche il motivo per cui il client Kafka non è una dipendenza di
// go-core-batch: qui si nomina soltanto il seam (producer.IProducer), e quale client giri lo decide
// l'import dell'app.
//
// La transazionalità è una scelta della config del producer
// (`server.producer.transactional-id`): con l'id, i messaggi di un tick diventano visibili ai
// consumer read_committed tutti o nessuno; senza, il producer è idempotente e un tick parzialmente
// prodotto è possibile — gli item non confermati tornano PENDING e il tick successivo li ripubblica
// (at-least-once, che è il contratto del framework in entrambi i casi: MarkDone non è nella
// transazione).
//
// Il producer NON è iniettabile in un task runner: per mandare una notifica si crea un WorkItem di
// tipo "NotificationKafka" (outbox), che questo job drena — non si pubblica inline.
package kafkajob

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-kafka/producer"
)

const JobType = "NotificationKafka"

// Register costruisce la JobRegistration del job NotificationKafka. Consuma il producer di
// go-core-kafka (il seam producer.IProducer, wirato dall'app) e lo store.
// È un costruttore fx: il risultato confluisce nel value group batch_jobs via scheduler.ProvideJob.
func Register(p producer.IProducer, items store.IWorkItemStore) scheduler.JobRegistration {
	return scheduler.JobRegistration{Type: JobType, Factory: makeNotificationJobFactory(p, items)}
}

// Module registra il job NotificationKafka. Se modes è vuoto registra sempre; altrimenti solo quando
// core.Mode è tra i modes indicati.
//
// Registra SOLO il job: il producer lo fornisce l'app con corekafka.ProducerModule (vedi il doc del
// package). Prima lo costruiva qui un ProducerService interno, che era un secondo client Kafka con la
// sua config, il suo TLS/SASL scritti a mano e nessuna astrazione driver.
func Module(modes ...string) {
	scheduler.ProvideJob(Register, modes...)
}
