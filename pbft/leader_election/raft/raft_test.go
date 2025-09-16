package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestRaft_SuccessfulElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock nodes
	mockNodes := make([]*MockNode, 4)
	for i := 0; i < 4; i++ {
		mockNodes[i] = NewMockNode(ctrl)
		mockNodes[i].EXPECT().GetCurrentView().Return(int64(1)).AnyTimes()
		mockNodes[i].EXPECT().GetCurrentViewLeader().Return("").AnyTimes()
	}

	// Create configurations for 4 nodes
	configStructs := make([]*configs.Config, 4)
	services := make([]*Service, 4)
	raftChs := make([]chan proto.Message, 4)
	senders := make([]*Sender, 4)

	for i := 0; i < 4; i++ {
		configStructs[i] = &configs.Config{
			Id:      fmt.Sprintf("node_%d", i+1),
			Address: &configs.Address{Host: "localhost", Port: 1001 + i},
			PeersAddress: map[string]*configs.Address{
				"node_1": {Host: "localhost", Port: 1001},
				"node_2": {Host: "localhost", Port: 1002},
				"node_3": {Host: "localhost", Port: 1003},
				"node_4": {Host: "localhost", Port: 1004},
			},
			General: &configs.General{
				EnabledByDefault: true,
			},
			Timers: &configs.Timers{
				ViewChangeTimeoutMs: 10000,
			},
		}
	}

	// Create raft elections
	raftElections := make([]*RaftElection, 4)
	for i := 0; i < 4; i++ {
		raftChs[i] = make(chan proto.Message, 100)
		senders[i] = NewSender(configStructs[i])
		services[i] = NewService(raftChs[i], configStructs[i])
		raftElections[i] = NewRaftElection(configStructs[i], mockNodes[i], senders[i], raftChs[i])
	}

	// Start gRPC servers
	for i := 0; i < 4; i++ {
		go services[i].Serve()
	}

	// Start raft elections
	for i := 0; i < 4; i++ {
		go raftElections[i].Start()
	}

	// Wait for election to complete
	time.Sleep(30 * time.Second)

	// Check results
	leaders := make([]string, 4)
	for i := 0; i < 4; i++ {
		leaders[i] = raftElections[i].GetCurrentLeader()
		t.Logf("Node %d elected leader: %s", i+1, leaders[i])
	}

	// Verify that a leader was elected
	assert.NotEmpty(t, leaders[0], "No leader was elected")
	assert.Contains(t, []string{"node_1", "node_2", "node_3", "node_4"}, leaders[0], "Elected leader is not in the node list")

	// Verify all nodes agree on the same leader
	for i := 1; i < 4; i++ {
		assert.Equal(t, leaders[0], leaders[i], "Nodes disagree on leader")
	}

	// Cleanup
	for i := 0; i < 4; i++ {
		raftElections[i].Stop()
		services[i].Stop()
	}
}
