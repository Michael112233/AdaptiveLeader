package paxos

import (
	"fmt"
	"testing"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestPaxos_SuccessfulElection(t *testing.T) {
	const nodeCount = 4
	const viewId = int64(3)

	// Set up
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nodeIds := make([]string, 0)
	configStructs := make([]*configs.Config, 0)
	paxosElections := make([]*PaxosElection, 0)
	nodes := make([]*MockNode, 0)
	senders := make([]*Sender, 0)
	services := make([]*Service, 0)
	paxosChs := make([]chan proto.Message, 0)
	for i := range nodeCount {
		nodeIds = append(nodeIds, fmt.Sprintf("node_%d", i+1))
		configStructs = append(configStructs, &configs.Config{
			Id: nodeIds[i],
			Address: &configs.Address{
				Host: "localhost",
				Port: 1001 + i,
			},
			PeersAddress: map[string]*configs.Address{
				"node_1": {
					Host: "localhost",
					Port: 1001,
				},
				"node_2": {
					Host: "localhost",
					Port: 1002,
				},
				"node_3": {
					Host: "localhost",
					Port: 1003,
				},
				"node_4": {
					Host: "localhost",
					Port: 1004,
				},
			},
			Timers: &configs.Timers{
				ViewChangeTimeoutMs: 10_000,
			},
			General: &configs.General{
				EnabledByDefault: true,
			},
		})
		senders = append(senders, NewSender(configStructs[i]))
		nodes = append(nodes, NewMockNode(ctrl))
		nodes[i].EXPECT().GetCurrentView().Return(int64(1)).AnyTimes()
		nodes[i].EXPECT().GetCurrentViewLeader().Return("node_1").AnyTimes()

		// 创建消息通道和gRPC服务
		paxosCh := make(chan proto.Message, 100)
		paxosChs = append(paxosChs, paxosCh)
		services = append(services, NewService(paxosCh, configStructs[i]))

		paxosElections = append(paxosElections, NewPaxosElection(configStructs[i], nodes[i], senders[i], paxosCh))
		err := paxosElections[i].Start()
		assert.NoError(t, err)

		// 启动gRPC服务器
		go services[i].Serve()
	}

	// Act
	resultChannels := make([]chan string, 0)
	for i, election := range paxosElections {
		resultChannels = append(resultChannels, make(chan string, 1))
		election.FindLeaderForView(viewId, resultChannels[i])
		time.Sleep(100 * time.Millisecond)
	}

	leaders := make([]string, 0)
	timeout := time.After(15 * time.Second)

	for i, ch := range resultChannels {
		select {
		case leader := <-ch:
			leaders = append(leaders, leader)
			t.Logf("Node %d elected leader: %s", i+1, leader)
		case <-timeout:
			t.Fatalf("Election timeout for node %d", i+1)
		}
	}

	// Assert
	assert.NotEmpty(t, leaders[0], "No leader was elected")
	assert.Contains(t, nodeIds, leaders[0], "Elected leader is not in the node list")

	// 所有节点应该选举出同一个leader
	for i, leader := range leaders {
		assert.Equal(t, leaders[0], leader, "Node %d elected different leader: %s vs %s", i+1, leader, leaders[0])
	}

	fmt.Printf("Elected %s as leader\n", leaders[0])

	// Clean Up
	for i, election := range paxosElections {
		err := election.Stop()
		assert.NoError(t, err)
		services[i].Stop()
		// 不要关闭已经关闭的channel
		// close(paxosChs[i])
	}
}
