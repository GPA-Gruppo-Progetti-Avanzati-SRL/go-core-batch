package worker

// Config è un worker pool: dimensione e task che serve.
type Config struct {
	Name string `yaml:"name" mapstructure:"name" json:"name"`
	Size int    `yaml:"size" mapstructure:"size" json:"size"`
	// Tasks elenca i task NAME serviti dal pool — i nomi delle istanze dichiarate nella sezione
	// `tasks:` (senza quella sezione il nome coincide col task type). È anche l'insieme che dice a
	// batch.Module quali runner istanziare in questo processo.
	Tasks []string `yaml:"tasks" mapstructure:"tasks" json:"tasks"`
}
