package s3

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
