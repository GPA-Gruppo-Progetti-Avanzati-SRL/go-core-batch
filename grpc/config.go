package grpc

type ClientConfig struct {
	Url string `mapstructure:"url"`
}

type ServerConfig struct {
	Hostname string `mapstructure:"hostname"`
	Port     int    `mapstructure:"port"`
}

type Config struct {
	Client ClientConfig `yaml:"client" mapstructure:"client" json:"client"`
	Server ServerConfig `yaml:"server" mapstructure:"server" json:"server"`
}
