package configs

type Config struct {
	Id           string              `mapstructure:"id"`
	Address      *Address            `mapstructure:"address"`
	PeersAddress map[string]*Address `mapstructure:"peers_address"`
	Grpc         *Grpc               `mapstructure:"grpc"`
	Timers       *Timers             `mapstructure:"timers"`
	General      *General            `mapstructure:"general"`
	Attacks      *Attacks            `mapstructure:"attacks"`
}

type Address struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type Grpc struct {
	SendTimeoutMs        int `mapstructure:"send_timeout_ms"`
	MaxRetries           int `mapstructure:"max_retries"`
	MaxConcurrentStreams int `mapstructure:"max_concurrent_streams"`
}

type Timers struct {
	ViewChangeTimeoutMs int `mapstructure:"view_change_timeout_ms"`
	RequestTimeoutMs    int `mapstructure:"request_timeout_ms"`
}

type General struct {
	EnabledByDefault       bool `mapstructure:"enabled_by_default"`
	MaxOutstandingRequests int  `mapstructure:"max_outstanding_requests"`
	CheckpointInterval     int  `mapstructure:"checkpoint_interval"`
	WaterMarkInterval      int  `mapstructure:"water_mark_interval"`
}

type Attacks struct {
	DelayedProposal DelayedProposal `mapstructure:"delayed_proposal"`
}

type DelayedProposal struct {
	Enabled        bool    `mapstructure:"enabled"`
	WaitTimeFactor float32 `mapstructure:"wait_time_factor"`
}

func (c *Config) F() int {
	return (len(c.PeersAddress) - 1) / 3
}

func (c *Config) GetAddress(id string) *Address {
	if id == c.Id {
		return c.Address
	}
	return c.PeersAddress[id]
}

func (c *Config) ReplicaIds() []string {
	ids := make([]string, 0, len(c.PeersAddress)+1)
	ids = append(ids, c.Id)
	for id := range c.PeersAddress {
		if id != c.Id {
			ids = append(ids, id)
		}
	}
	return ids
}
