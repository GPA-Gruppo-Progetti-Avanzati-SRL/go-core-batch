// Package kafkajob reads pending WorkItems from the store and sends them to Kafka.
// È un package a parte, e non un componente che batch.Module wira da sé, perché nomina
// go-core-kafka: un'app che non lo importa non se lo trascina nel go.mod. L'implementazione sta
// in internal/kafkajob; qui c'è soltanto la registrazione.
//
// Si passa a batch.WithModule. NON registra alcun producer: il producer è quello di
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

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/kafkajob"

// Module registra il job NotificationKafka. Si passa a batch.WithModule, che lo gate-a sui
// scheduler modes; il producer.IProducer va wirato dall'app (corekafka.ProducerModule).
func Module(modes ...string) { kafkajob.Module(modes...) }
