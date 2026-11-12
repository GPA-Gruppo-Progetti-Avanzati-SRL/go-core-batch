// Package task contiene la configurazione applicativa dei task batch: la sezione `tasks:`.
//
// Il registro che lega i task type registrati alle istanze configurate (Apply/Instances) è un
// ingranaggio di batch.Module e di runner.Register, e sta in internal/taskreg: l'app di questo
// package nomina solo il tipo della sezione.
//
// Il modello:
//
//	tasks:                     # istanze di task, con la loro configurazione APPLICATIVA
//	  - name: import-in        # identificativo dell'istanza, referenziato dai job/worker
//	    type: IMPORT           # task type registrato da runner.Register[importRunner]("IMPORT")
//	    properties:
//	      folder: /data/in
//	  - name: import-bulk      # stesso type, properties diverse → istanza distinta
//	    type: IMPORT
//	    properties:
//	      folder: /data/bulk
//
// Da non confondere col blocco `properties:` di un JOB, che è INFRASTRUTTURALE (configura il job
// type: `task`, `limit`, `collection`, `topic`, …) e resta letto dal framework.
//
// La dichiarazione è OBBLIGATORIA: ogni task type registrato deve avere almeno una voce in `tasks:`,
// ogni voce deve portare il suo `name` (nessuna fallback sul `type`), ogni task referenziato da
// jobs:/workers: deve esistere e ogni task eseguito in questo processo deve avere un runner. Le
// incoerenze fanno fallire l'avvio.
//
// Un'istanza diventa runner solo se è ESEGUITA in questo processo (ActiveSet.Executed): nominata da
// un job che la esegue in linea o servita da un worker pool di qui. Un task citato da un FeedTask,
// che accoda e non esegue, o consegnato via gRPC a un worker remoto, non porta le sue dipendenze
// nel grafo fx.
package task

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/s3"
)

// Config è una voce della sezione `tasks:`: un'istanza di task type, con la sua configurazione
// applicativa. Le Properties sono mappate sui campi `prop:` della struct del runner (properties.BindProps).
type Config struct {
	// Name identifica l'istanza ed è ciò che job e worker referenziano; è anche il WorkItem.TaskName
	// usato da claiming e instradamento. È OBBLIGATORIO e va scritto anche quando coincide col
	// Type: è la chiave di routing, e due istanze dello stesso Type si distinguono solo per Name.
	Name string `yaml:"name" mapstructure:"name" json:"name"`
	Type string `yaml:"type" mapstructure:"type" json:"type" validate:"required"`
	// MaxRetry è il numero massimo di RITENTATIVI di un work item: con `max-retry: 3` l'item
	// viene eseguito fino a 4 volte in tutto. -1 = illimitato.
	//
	// Il tetto sta sul TASK e non sul job perché il ciclo di vita del work item è per task —
	// ClaimPending e RecoverOrphans filtrano per task name — e lo stesso task può essere servito
	// da più job o da un worker pool: item identici devono avere lo stesso limite.
	//
	// È un puntatore perché l'ASSENZA vale -1 (illimitato), che è la condotta storica. Lo
	// zero-value di un int è 0, cioè "nessun ritentativo": come default silenzioso sarebbe il
	// peggiore possibile per chi aggiorna la libreria senza toccare la propria config.
	//
	// ATTENZIONE: il contatore su cui si misura è WorkItem.Retry, che RecoverOrphans incrementa
	// insieme a MarkPending. Il recupero di un item orfano — tipicamente il riavvio di un pod —
	// consuma quindi un tentativo, anche se il runner non ha mai fallito.
	MaxRetry   *int                  `yaml:"max-retry" mapstructure:"max-retry" json:"max-retry"`
	Properties properties.Properties `yaml:"properties" mapstructure:"properties" json:"properties"`
	// S3 è la sorgente S3 del task: con questo blocco il task è alimentato dal feed S3 (job FeedS3Task
	// o Feeder su richiesta), che accoda un work item per file. Il runner è un runner.RegisterFile.
	// Sta accanto a Properties e non dentro perché è INFRASTRUTTURALE — la legge il framework, non il
	// runner — e un campo tipizzato si valida da sé invece di finire fra le chiavi non reclamate.
	S3 *s3.Source `yaml:"s3" mapstructure:"s3" json:"s3,omitempty"`
}

// ResolveMaxRetry applica la convenzione dell'assenza: nessun valore configurato significa
// illimitato, come si comportava il framework prima che il tetto esistesse.
func (c Config) ResolveMaxRetry() int {
	if c.MaxRetry == nil {
		return MaxRetryUnlimited
	}
	return *c.MaxRetry
}

// MaxRetryUnlimited è il valore che disattiva il tetto ai ritentativi.
const MaxRetryUnlimited = -1
