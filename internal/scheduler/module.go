package scheduler

import (
	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// Module wires the batch scheduler into the fx application. La []Config è passata
// come parametro e fornita a fx dal Module stesso (core.Supply interno): l'app non
// deve più fare core.Supply. Il costruttore concreto (newScheduler) non è esportato:
// l'unico entry-point è Module().
//
// Dipendenza risolta da fx (fornita altrove): corelock.Locker, il lock distribuito che
// l'applicazione wira con corelock.Module. Lo store non serve allo Scheduler: lo consumano i
// costruttori dei job, che se lo fanno iniettare.
//
// L'ordine di registrazione dei job type è indifferente: confluiscono nel value group batch_jobs,
// che fx risolve per intero prima di costruire lo Scheduler.
//
// Se modes è vuoto registra sempre; altrimenti solo quando core.Mode è tra i modes indicati.
func Module(config []Config, modes ...string) {
	core.Supply(config, modes...)
	core.Provide(newScheduler, modes...)
	core.Invoke(func(*Scheduler) {}, modes...)
}
