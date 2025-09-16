package scenarios

import (
	"github.com/Arman17Babaei/pbft/load_tester/configs"
	config2 "github.com/Arman17Babaei/pbft/pbft/configs"
)

type Scenario interface {
	PrepareScenario(loadTestConfig *configs.Config, pbftConfig *config2.Config)
	Run(stopCh <-chan any)
}

var Scenarios = map[string]Scenario{
	"no_failure":       &NoFailure{},
	"periodic_failure": &PeriodicFailure{},
	"delayed_proposal": &DelayedProposal{},
}
