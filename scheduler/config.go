package scheduler

import "time"

type Config struct {
	Name          string `mapstructure:"name"`
	Type          string `mapstructure:"type"`
	ScheduledCron string `mapstructure:"cron"`
	Disabled      bool   `mapstructure:"disabled"`
	SingletonMode bool   `mapstructure:"singleton"`
	// LockTimeout is the maximum time an item can stay IN_PROGRESS before being
	// considered orphaned and re-claimed by RecoverOrphans. Configurable per job.
	LockTimeout time.Duration     `mapstructure:"lock-timeout"`
	Properties  map[string]string `mapstructure:"properties"`
}
