package roundrobin

import (
	"slices"

	pb "github.com/Arman17Babaei/pbft/proto"

	"github.com/Arman17Babaei/pbft/pbft/configs"
)

type RoundRobin struct {
	peerIds     []string
	ViewChanges map[int64]map[string]*pb.ViewChangeRequest
}

func NewLeaderElection(config *configs.Config) *RoundRobin {
	leaderIds := make([]string, 0)
	for id := range config.PeersAddress {
		leaderIds = append(leaderIds, id)
	}
	slices.Sort(leaderIds)

	return &RoundRobin{
		peerIds: leaderIds,
	}
}

func (r *RoundRobin) GetLeader(viewId int64) string {
	return r.peerIds[viewId%int64(len(r.peerIds))]
}

// For testing
func (r *RoundRobin) FindLeaderForView(viewId int64, callbackCh chan string) {
	callbackCh <- r.peerIds[viewId%int64(len(r.peerIds))]
}

func (r *RoundRobin) Serve() error {
	return nil
}

func (r *RoundRobin) Stop() error {
	return nil
}

func (r *RoundRobin) Start() error {
	return nil
}
