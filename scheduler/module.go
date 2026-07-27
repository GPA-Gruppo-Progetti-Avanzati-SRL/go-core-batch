package scheduler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Module wires the batch scheduler into the fx application. La []Config è passata
// come parametro e fornita a fx dal Module stesso (core.Supply interno): l'app non
// deve più fare core.Supply. Il costruttore concreto (newScheduler) non è esportato:
// l'unico entry-point è Module().
//
// Dipendenze risolte da fx (fornite altrove): lock.Locker (lock distribuito, backend
// iniettato via batch.WithLocker: redis/mongo/sql) e store.IData (via Services).
// Registrare i job type (distributedjob/kafkajob/simplejob) e lo store PRIMA di Module().
//
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(config []Config, modes ...string) {
	core.Supply(config, modes...)
	core.Provide(newScheduler, modes...)
	core.Invoke(func(*Scheduler) {}, modes...)
}
