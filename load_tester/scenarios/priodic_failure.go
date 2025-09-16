package scenarios

import (
	"context"
	"fmt"
	"github.com/Arman17Babaei/pbft/load_tester/configs"
	pbftconfig "github.com/Arman17Babaei/pbft/pbft/configs"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sort"
	"time"
)

type PeriodicFailure struct {
	peersAddress map[string]*pbftconfig.Address
	nodes        map[string]pb.PbftClient
	interval     time.Duration
}

func (p *PeriodicFailure) PrepareScenario(loadTestConfig *configs.Config, pbftConfig *pbftconfig.Config) {
	p.peersAddress = pbftConfig.PeersAddress
	p.interval = time.Duration(loadTestConfig.Attacks.PeriodicFailure.IntervalSeconds) * time.Second
	p.nodes = make(map[string]pb.PbftClient)
	for id, address := range p.peersAddress {
		target := fmt.Sprintf("%s:%d", address.Host, address.Port)
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		p.nodes[id] = pb.NewPbftClient(conn)
	}
}

func (p *PeriodicFailure) Run(stopCh <-chan any) {
	nodeIds := make([]string, 0, len(p.nodes))
	for id := range p.nodes {
		nodeIds = append(nodeIds, id)
	}
	sort.Strings(nodeIds)

	toggleTicker := time.NewTicker(p.interval)
	curIdx := len(nodeIds) - 1
	isEnableTurn := false
	for {
		select {
		case <-stopCh:
			return
		case <-toggleTicker.C:
			if isEnableTurn {
				log.WithField("node id", nodeIds[curIdx]).Error("Enabling")
				sendEnable(p.nodes[nodeIds[curIdx]])
			} else {
				curIdx = (curIdx + 1) % len(nodeIds)
				log.WithField("node id", nodeIds[curIdx]).Error("Disabling")
				sendDisable(p.nodes[nodeIds[curIdx]])
			}
			isEnableTurn = !isEnableTurn
		}
	}
}

func sendEnable(node pb.PbftClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := node.Enable(ctx, &pb.Empty{})
	if err != nil {
		log.WithError(err).Error("failed to enable node")
	}
}

func sendDisable(node pb.PbftClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := node.Disable(ctx, &pb.Empty{})
	if err != nil {
		log.WithError(err).Error("failed to enable node")
	}
}
