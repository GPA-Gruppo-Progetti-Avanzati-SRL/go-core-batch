package worker

import pub "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/worker"

// Config è un worker pool (voce di `workers:`). Il tipo è pubblico perché sta in batch.Config; qui
// è un alias, così il worker pool lo nomina come prima.
type Config = pub.Config
