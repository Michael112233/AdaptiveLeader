package carousal

import (
	"sync"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"google.golang.org/protobuf/proto"
)

type Node interface {
	GetCurrentView() int64
	GetCurrentBlock() *pb.Block
}

type CarousalElection struct {
	mu sync.RWMutex

	node	Node
	peerIds	[]string
}

// It creates a new Carousal election instance.
func NewCarousalElection(config *configs.Config, node Node) *CarousalElection {
	leaderIds := make([]string, 0)
	for id := range config.PeersAddress {
		leaderIds = append(leaderIds, id)
	}
	slices.Sort(leaderIds)
	
	return &CarousalElection{
		config: config,
		node:   node,
		peerIds: leaderIds,
	}
}

// To get the leader for the current view
func (c *CarousalElection) GetLeader() string {
	currentView := c.node.GetCurrentView()
	currentBlock := c.node.GetCurrentBlock()

	// An honest party hasn't committed a block with round r-1.
	// TODO: Specify the block
	if currentBlock.round_number != currentView - 1 {
		return c.peerIds[currentView % int64(len(c.peerIds))]
	} 

	// Get the set of active peers
	block := currentBlock
	lastAuthors := make(map[string]int64)
	for i:=0; i < c.config.F(); i++ {
		lastAuthors = append(lastAuthors, block.author)
		block = block.parentBlock
	}

	candidate := make(map[string]int64)
	for endorser := range block.endorsers {
		if endorser not in lastAuthors {
			candidate = append(candidate, endorser)
		}
	}

	return random.Choice(candidate)
}

// For testing
func (c *CarousalElection) FindLeaderForView(viewId int64, callbackCh chan string) {

}

func (c *CarousalElection) Start() error {
	return nil;
}

func (c *CarousalElection) Stop() error {
	return nil;
}

func (c *CarousalElection) HandleMessage(msg proto.Message) error {
	return nil;
}

