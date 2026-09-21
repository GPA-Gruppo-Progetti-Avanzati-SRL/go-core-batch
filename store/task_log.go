package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

const TableTaskLogs = "task_logs"

// Stati di una riga di task log.
const (
	TaskLogStart      = "START"
	TaskLogDone       = "DONE"
	TaskLogError      = "ERROR"
	TaskLogAssigned   = "ASSIGNED"
	TaskLogAssignedKO = "ASSIGNEDKO"
)

// TaskLog records a single lifecycle event for a distributed task.
// Implements both mongo.ICollection (GetCollectionName) and coresql.IRecord (GetTableName).
type TaskLog struct {
	TaskID   string    `json:"taskId"            bson:"taskId"            bun:"task_id"`
	JobID    string    `json:"jobId"             bson:"jobId"             bun:"job_id"`
	TaskName string    `json:"taskName"          bson:"taskName"          bun:"task_name"`
	Stato    string    `json:"stato"             bson:"stato"             bun:"stato"`
	Hostname string    `json:"hostname"          bson:"hostname"          bun:"hostname"`
	Logdate  time.Time `json:"logdate"           bson:"logdate"           bun:"logdate"`
	Objectid string    `json:"oggetto,omitempty" bson:"oggetto,omitempty" bun:"oggetto,nullzero"`
	Error    string    `json:"errore,omitempty"  bson:"errore,omitempty"  bun:"errore,nullzero"`
}

func (t TaskLog) GetCollectionName(ctx context.Context) string { return "tasks" }
func (t TaskLog) GetTableName(ctx context.Context) string      { return TableTaskLogs }

// NewTaskLog costruisce una riga di task log riempiendo i campi di contesto (hostname, istante).
// Sta qui e non nelle due implementazioni perché era la stessa funzione copiata in entrambe.
func NewTaskLog(taskId, jobId, taskName, objectId, stato, errMsg string) *TaskLog {
	return &TaskLog{
		TaskID:   taskId,
		JobID:    jobId,
		TaskName: taskName,
		Stato:    stato,
		Hostname: core.GetHostname(),
		Logdate:  time.Now(),
		Objectid: objectId,
		Error:    errMsg,
	}
}

// TaskLogLevel dice quali righe di task_logs scrivere. Esiste perché il livello di dettaglio
// storico è una scelta di esercizio, non una costante della libreria: sul percorso distributedjob
// si scrivono TRE righe per item (ASSIGNED dal job, START e DONE/ERROR dal worker), e in molti
// deploy quelle di successo non vengono mai lette — restano un costo di scrittura e una
// collection che cresce.
//
// Lo zero value è la condotta storica (tutte le righe): aggiornare la libreria senza toccare la
// propria config non deve far sparire dei dati.
type TaskLogLevel string

const (
	// TaskLogAll scrive ogni riga. È il default (anche come stringa vuota).
	TaskLogAll TaskLogLevel = "all"
	// TaskLogErrors scrive solo gli esiti negativi (ERROR, ASSIGNEDKO): tiene la diagnosi di ciò
	// che è andato storto e butta la traccia di ciò che è andato bene, che è quasi tutto.
	TaskLogErrors TaskLogLevel = "errors"
	// TaskLogOff non scrive nulla: lo stato vive sul WorkItem e l'andamento sulle metriche.
	TaskLogOff TaskLogLevel = "off"
)

// Records dice se una riga con quello stato va scritta al livello corrente.
func (l TaskLogLevel) Records(stato string) bool {
	switch l {
	case TaskLogOff:
		return false
	case TaskLogErrors:
		return stato == TaskLogError || stato == TaskLogAssignedKO
	default: // TaskLogAll, e lo zero value
		return true
	}
}

// Filter tiene le sole righe che il livello corrente prevede.
func (l TaskLogLevel) Filter(logs []*TaskLog) []*TaskLog {
	if l == TaskLogAll || l == "" {
		return logs
	}
	out := logs[:0:0]
	for _, tl := range logs {
		if l.Records(tl.Stato) {
			out = append(out, tl)
		}
	}
	return out
}

// ParseTaskLogLevel valida il valore letto da config. Un valore non previsto è un errore e non
// un ripiego silenzioso su "all": chi l'ha scritto intendeva qualcosa, e indovinare male
// significherebbe scrivere (o non scrivere) dati senza che nulla lo dica.
func ParseTaskLogLevel(s string) (TaskLogLevel, error) {
	switch l := TaskLogLevel(strings.ToLower(strings.TrimSpace(s))); l {
	case "", TaskLogAll:
		return TaskLogAll, nil
	case TaskLogErrors, TaskLogOff:
		return l, nil
	default:
		return "", fmt.Errorf("task-log %q non valido: ammessi %q, %q, %q",
			s, TaskLogAll, TaskLogErrors, TaskLogOff)
	}
}
