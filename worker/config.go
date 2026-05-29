package worker

type Config struct {
	Name  string   `yaml:"name" mapstructure:"name" json:"name"`
	Size  int      `yaml:"size" mapstructure:"size" json:"size"`
	Tasks []string `yaml:"tasks" mapstructure:"tasks" json:"tasks"`
}
