package scheduler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Module wires the batch scheduler into the fx application. La []Config è passata
// come parametro e fornita a fx dal Module stesso (core.Supply interno): l'app non
// deve più fare core.Supply. Il costruttore concreto (newScheduler) non è esportato:
// l'unico entry-point è Module().
//
// Dipendenze risolte da fx (fornite altrove): *redis.Client (lock distribuito) e
// store.IData (via Services). Registrare i job type (distributedjob/kafkajob/simplejob)
// e lo store PRIMA di chiamare Module().
//
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(config []Config, modes ...string) {
	core.Supply(config, modes...)
	core.Provide(newScheduler, modes...)
	core.Invoke(func(*Scheduler) {}, modes...)
}
