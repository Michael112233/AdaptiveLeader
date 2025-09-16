package scenarios

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"

	"github.com/Arman17Babaei/pbft/load_tester/configs"
	pbftconfig "github.com/Arman17Babaei/pbft/pbft/configs"
	pb "github.com/Arman17Babaei/pbft/proto"
)

type DelayedProposal struct {
	peersAddress   map[string]*pbftconfig.Address
	nodes          map[string]pb.PbftClient
	affectedNode   string
	waitTimeFactor float32
}

func (dp *DelayedProposal) PrepareScenario(loadTestConfig *configs.Config, pbftConfig *pbftconfig.Config) {
	dp.peersAddress = pbftConfig.PeersAddress
	dp.waitTimeFactor = loadTestConfig.Attacks.DelayedProposal.WaitTimeFactor
	dp.affectedNode = loadTestConfig.Attacks.DelayedProposal.AffectedNode
	dp.nodes = make(map[string]pb.PbftClient)

	for id, address := range dp.peersAddress {
		target := fmt.Sprintf("%s:%d", address.Host, address.Port)
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		dp.nodes[id] = pb.NewPbftClient(conn)
	}
}

func (dp *DelayedProposal) Run(stopCh <-chan any) {
	affectedNode := dp.nodes[dp.affectedNode]
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := affectedNode.EnableDelayedProposal(ctx, &pb.DelayedProposalRequest{
		Enabled:        true,
		WaitTimeFactor: dp.waitTimeFactor,
	})
	if err != nil {
		log.WithError(err).Error("failed to enable node")
	}
}
