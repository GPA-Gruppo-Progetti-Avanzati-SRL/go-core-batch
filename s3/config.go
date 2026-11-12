package s3

import (
	"errors"
	"fmt"
	"path"
)

// ServiceConfig holds the configuration for a single S3-compatible service.
type ServiceConfig struct {
	Endpoint     string `yaml:"endpoint" mapstructure:"endpoint" json:"endpoint"`
	Region       string `yaml:"region" mapstructure:"region" json:"region"`
	AccessKey    string `yaml:"access-key" mapstructure:"access-key" json:"access-key"`
	SecretKey    string `yaml:"secret-key" mapstructure:"secret-key" json:"secret-key"`
	Bucket       string `yaml:"bucket" mapstructure:"bucket" json:"bucket"`
	UsePathStyle bool   `yaml:"use-path-style" mapstructure:"use-path-style" json:"use-path-style"`
}

// Config holds the configuration for all S3 services, keyed by logical name.
type Config struct {
	Services map[string]*ServiceConfig `yaml:"services" mapstructure:"services" json:"services"`
}

// DefaultSourceLimit è il tetto ai file accodati per giro del feed quando `limit` non è scritto.
const DefaultSourceLimit = 100

// Source è la sorgente S3 di un task: il blocco `s3:` di una voce di `tasks:`. Un task che lo porta
// è alimentato dal feed S3 — dal job FeedS3Task a cron o dal Feeder su richiesta (go-core-trigger) —
// che elenca Path e accoda un work item per ogni file il cui nome corrisponde a Pattern; il runner
// (runner.RegisterFile) riceve il file, e dopo un'elaborazione riuscita la libreria lo sposta in
// DestPath.
//
//	tasks:
//	  - name: import
//	    type: IMPORT
//	    s3: { service: s3, path: "inbox/", pattern: "*", dest-path: "archive" }
type Source struct {
	// Service è il servizio S3 logico: una chiave di `s3.services`.
	Service string `yaml:"service" mapstructure:"service" json:"service"`
	// Path è il prefisso elencato. L'elenco è per prefisso e non per cartella: `in/` comprende
	// anche `in/sub/x.csv`.
	Path string `yaml:"path" mapstructure:"path" json:"path"`
	// Pattern è il glob confrontato col nome del file (path.Base della chiave); vuoto = tutti.
	Pattern string `yaml:"pattern" mapstructure:"pattern" json:"pattern"`
	// DestPath è dove il file viene spostato dopo un'elaborazione riuscita. È obbligatorio: è lo
	// spostamento a togliere dal prefisso un file elaborato, e senza il giro successivo lo
	// riaccoderebbe.
	DestPath string `yaml:"dest-path" mapstructure:"dest-path" json:"dest-path"`
	// Limit è il tetto ai file accodati per giro (default DefaultSourceLimit).
	Limit int `yaml:"limit" mapstructure:"limit" json:"limit"`
}

// Validate controlla i campi obbligatori e il pattern. Non verifica che Service sia dichiarato in
// `s3.services`: lo fa chi possiede il Registry, alla costruzione del Feeder.
func (s *Source) Validate() error {
	switch {
	case s.Service == "":
		return errors.New("s3.service mancante: è il servizio S3 da elencare (una chiave di s3.services)")
	case s.DestPath == "":
		return errors.New("s3.dest-path mancante: è dove il file viene spostato dopo l'elaborazione, e senza un file già elaborato verrebbe riaccodato al giro successivo")
	case s.Limit < 0:
		return fmt.Errorf("s3.limit non può essere negativo: %d", s.Limit)
	}
	if s.Pattern != "" {
		if _, err := path.Match(s.Pattern, ""); err != nil {
			return fmt.Errorf("s3.pattern %q non valido: %w", s.Pattern, err)
		}
	}
	return nil
}

// ResolvedLimit applica il default di Limit.
func (s *Source) ResolvedLimit() int {
	if s.Limit <= 0 {
		return DefaultSourceLimit
	}
	return s.Limit
}
