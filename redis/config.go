package redis

type Config struct {
	Enable   bool   `yaml:"enable" mapstructure:"enable" json:"enable"`
	Address  string `yaml:"address" mapstructure:"address" json:"address"`
	Port     int    `yaml:"port" mapstructure:"port" json:"port"`
	Password string `yaml:"password" mapstructure:"password" json:"password"`
}
