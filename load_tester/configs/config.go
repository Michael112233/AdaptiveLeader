package configs

type Config struct {
	DurationSeconds int `mapstructure:"duration_seconds"`
	Throughput      int `mapstructure:"throughput_per_second"`
	NumClients      int `mapstructure:"num_clients"`

	Scenario string   `mapstructure:"scenario"`
	Attacks  *Attacks `mapstructure:"attacks"`
}

type Attacks struct {
	DelayedProposal DelayedProposal `mapstructure:"delayed_proposal"`
	PeriodicFailure PeriodicFailure `mapstructure:"periodic_failure"`
}

type DelayedProposal struct {
	Enabled        bool    `mapstructure:"enabled"`
	AffectedNode   string  `mapstructure:"affected_node"`
	WaitTimeFactor float32 `mapstructure:"wait_time_factor"`
}

type PeriodicFailure struct {
	IntervalSeconds int `mapstructure:"interval_seconds"`
}
