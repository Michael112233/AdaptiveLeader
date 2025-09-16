package raft

//go:generate mockgen -source=raft.go -destination=raft_mock.go -package=raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// Node interface defines the methods that Raft election needs from the main node
type Node interface {
	GetCurrentView() int64
	GetCurrentViewLeader() string
}

// RaftElection implements the Raft consensus algorithm for leader election
// It manages the Prepare, Accept, and Learn phases of the Raft protocol
type RaftElection struct {
	mu sync.RWMutex

	config *configs.Config
	node   Node
	sender *Sender

	// Raft state variables
	currentView        int64
	currentViewLeader  string
	currentTerm        int64
	maxProposalId      int64
	proposalId         int64
	proposedValue      string
	acceptedProposalId int64
	acceptedValue      string

	// Election state
	NewLeader   string
	isLeader    bool
	isCandidate bool
	NewLeaderCh chan string

	// Message handling
	leaderElectionCh chan struct{}
	raftCh           <-chan proto.Message
	stopCh           chan struct{}

	// Timeout control
	electionTimeout time.Duration
	electionTimer   *time.Timer

	// Response tracking
	prepareRequests  map[int64]map[string]*pb.RaftPrepareRequest
	promiseResponses map[int64]map[string]*pb.RaftPromiseRequest
	acceptRequests   map[int64]map[string]*pb.RaftAcceptRequest
	successRequests  map[int64]map[string]*pb.RaftSuccessRequest
}

// NewRaftElection creates a new Raft election instance
func NewRaftElection(config *configs.Config, node Node, sender *Sender) *RaftElection {
	electionTimeout := time.Duration(config.Timers.ViewChangeTimeoutMs) * time.Millisecond

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	return &RaftElection{
		config: config,
		node:   node,
		sender: sender,

		currentView:        node.GetCurrentView(),
		currentViewLeader:  node.GetCurrentViewLeader(),
		currentTerm:        0,
		maxProposalId:      0,
		proposalId:         0,
		proposedValue:      "",
		acceptedValue:      "",
		acceptedProposalId: 0,

		NewLeader:        "",
		isLeader:         false,
		isCandidate:      false,
		NewLeaderCh:      make(chan string, 10),
		leaderElectionCh: make(chan struct{}, 10),
		stopCh:           make(chan struct{}),

		electionTimeout: electionTimeout,
		electionTimer:   time.NewTimer(electionTimeout),

		prepareRequests:  make(map[int64]map[string]*pb.RaftPrepareRequest),
		promiseResponses: make(map[int64]map[string]*pb.RaftPromiseRequest),
		acceptRequests:   make(map[int64]map[string]*pb.RaftAcceptRequest),
		successRequests:  make(map[int64]map[string]*pb.RaftSuccessRequest),
	}
}

func (p *RaftElection) UpdateRaftElection(node Node) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.currentView = node.GetCurrentView()
	p.currentViewLeader = node.GetCurrentViewLeader()
}

func (p *RaftElection) StartElection() {
	p.leaderElectionCh <- struct{}{}
}

// runMessageHandler processes incoming Raft messages
func (p *RaftElection) runMessageHandler() {
	for {
		select {
		case msg := <-p.raftCh:
			p.handleMessage(msg)
		case <-p.stopCh:
			return
		}
	}
}

// handleMessage routes different types of Raft messages to appropriate handlers
func (p *RaftElection) handleMessage(msg proto.Message) {
	// 1. Recording the time of the message
	startTime := time.Now()
	timer := monitoring.ResponseTimeSummary.WithLabelValues(p.config.Id, "raft-message")
	defer timer.Observe(time.Since(startTime).Seconds())

	// 2. Routing the message to the appropriate handler
	switch m := msg.(type) {
	case *pb.RaftPrepareRequest:
		p.handleRaftPrepareRequest(m)
	case *pb.RaftPromiseRequest:
		p.handleRaftPromiseResponse(m)
	case *pb.RaftAcceptRequest:
		p.handleRaftAcceptRequest(m)
	case *pb.RaftSuccessRequest:
		p.handleRaftSuccessResponse(m)
	default:
		log.WithField("message-type", fmt.Sprintf("%T", msg)).Warn("Unknown raft message type")
	}
}

// handlePrepareRequest processes incoming Prepare phase requests
func (p *RaftElection) handleRaftPrepareRequest(req *pb.RaftPrepareRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Debug("Handling raft-prepare request")

	// Update term if request has higher term
	if req.Term > p.currentTerm {
		p.currentTerm = req.Term
		p.isLeader = false
		p.isCandidate = false
	} else if req.Term < p.currentTerm {
		// Reject prepare
		response := &pb.RaftPromiseRequest{
			Term:                   p.currentTerm,
			Promised:               false,
			AcceptorId:             p.config.Id,
			ViewId:                 p.currentView,
			LastAcceptedProposalId: p.acceptedProposalId,
			LastAcceptedValue:      p.acceptedValue,
			Timestamp:              time.Now().Unix(),
		}
		p.sender.SendRPCToPeer(req.ProposerId, "RaftPromise", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Wrong Raft-Prepare, Rejected")
		return
	}

	// Accept prepare if proposal ID is higher
	if req.ProposalId > p.maxProposalId {
		p.maxProposalId = req.ProposalId

		p.isLeader = false
		p.isCandidate = false

		// Send prepare response
		response := &pb.RaftPromiseRequest{
			Term:                   p.currentTerm,
			Promised:               true,
			AcceptorId:             p.config.Id,
			ViewId:                 p.currentView,
			LastAcceptedProposalId: p.acceptedProposalId,
			LastAcceptedValue:      p.acceptedValue,
			Timestamp:              time.Now().Unix(),
		}

		if p.prepareRequests[p.currentTerm] == nil {
			p.prepareRequests[p.currentTerm] = make(map[string]*pb.RaftPrepareRequest)
		}
		p.prepareRequests[p.currentTerm][req.ProposerId] = req
		p.sender.SendRPCToPeer(req.ProposerId, "RaftPromise", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Sending Raft-Promise")
	} else {
		// Reject prepare
		response := &pb.RaftPromiseRequest{
			Term:                   p.currentTerm,
			Promised:               false,
			AcceptorId:             p.config.Id,
			ViewId:                 p.currentView,
			LastAcceptedProposalId: p.acceptedProposalId,
			LastAcceptedValue:      p.acceptedValue,
			Timestamp:              time.Now().Unix(),
		}

		p.sender.SendRPCToPeer(req.ProposerId, "RaftPromise", response)
		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Sending Reject Raft-Prepare")
	}
}

// handlePrepareResponse processes Prepare phase responses
func (p *RaftElection) handleRaftPromiseResponse(resp *pb.RaftPromiseRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", resp.Term).
		Debug("Handling raft-promise response")

	// if node is not trying to be a leader or request is not for the current term
	// skip the response
	if p.currentTerm < resp.Term {
		p.prepareRequests[p.currentTerm] = nil
		p.currentTerm = resp.Term
		p.isLeader = false
		p.isCandidate = false

		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Raft-Promise Response (Term is lower)")
		return
	}

	if !p.isCandidate || p.currentTerm != resp.Term || !resp.Promised {
		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Raft-Promise Response")
		return
	}

	// Recording the response
	if p.promiseResponses[p.currentTerm] == nil {
		p.promiseResponses[p.currentTerm] = make(map[string]*pb.RaftPromiseRequest)
	}

	p.promiseResponses[p.currentTerm][resp.AcceptorId] = resp

	// if got majority of promise, start the accept phase
	if len(p.promiseResponses[p.currentTerm]) >= p.getMajority() {
		p.acceptedProposalId = p.maxProposalId
		p.acceptedValue = p.config.Id

		p.sender.Broadcast("raft-accept", &pb.RaftAcceptRequest{
			Term:          p.currentTerm,
			ProposalId:    p.maxProposalId,
			ProposerId:    p.config.Id,
			ProposedValue: p.config.Id,
			ViewId:        p.currentView,
			Timestamp:     time.Now().Unix(),
		})

		log.WithField("node-id", p.config.Id).
			WithField("term", p.currentTerm).
			WithField("proposal-id", p.maxProposalId).
			Debug("Broadcasting Raft-Accept Request")
	}
}

// handleAcceptRequest processes incoming Accept phase requests
func (p *RaftElection) handleRaftAcceptRequest(req *pb.RaftAcceptRequest) {
	p.mu.Lock()

	log.WithField("node-id", p.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Debug("Handling accept request")

	// 1. higer term request change the node to acceptor
	if req.Term > p.currentTerm {
		p.isCandidate = false
		p.isLeader = false
	}

	// 2. if term is not the same, skip the request (As it does not go through the prepare-promise phase)
	if req.Term != p.currentTerm {
		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Wrong Accept Request, Rejected")
		p.mu.Unlock()
		return
	}

	// Accept if proposal ID >= accepted proposal ID
	if req.ProposalId >= p.acceptedProposalId || req.ProposalId == p.maxProposalId {
		p.acceptedProposalId = req.ProposalId
		p.acceptedValue = req.ProposedValue

		// Send success response
		response := &pb.RaftSuccessRequest{
			Term:       p.currentTerm,
			Success:    true,
			AcceptorId: p.config.Id,
			ViewId:     p.currentView,
			Timestamp:  time.Now().Unix(),
		}

		p.sender.SendRPCToPeer(req.ProposerId, "RaftAccept", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Sending Raft-Success Response")

		// Set the new leader
		// p.NewLeader = req.ProposerId
		p.mu.Unlock()

		// // Wait and check whether the node is leader (Maybe other candidate also trying)
		// time.Sleep(1 * time.Second)
		// p.NewLeaderCh <- p.NewLeader
	} else {
		// Reject accept
		response := &pb.RaftSuccessRequest{
			Term:       p.currentTerm,
			Success:    false,
			AcceptorId: p.config.Id,
			ViewId:     p.currentView,
			Timestamp:  time.Now().Unix(),
		}
		p.sender.SendRPCToPeer(req.ProposerId, "RaftAccept", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Rejecting Raft-Accept Request")
		p.mu.Unlock()
	}
}

// handleAcceptResponse processes Accept phase responses
func (p *RaftElection) handleRaftSuccessResponse(resp *pb.RaftSuccessRequest) {
	p.mu.Lock()

	log.WithField("node-id", p.config.Id).
		WithField("term", resp.Term).
		Debug("Handling raft-success request")

	// if term is lower, skip the response and become acceptor
	if p.currentTerm < resp.Term {
		p.successRequests[p.currentTerm] = nil
		p.currentTerm = resp.Term
		p.isLeader = false
		p.isCandidate = false

		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Raft-Success Response (Term is lower)")
		p.mu.Unlock()
		return
	}

	// skip wrong response
	if !p.isCandidate || p.currentTerm != resp.Term || !resp.Success {
		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Raft-Success Response")
		p.mu.Unlock()
		return
	}
	p.successRequests[p.currentTerm][resp.AcceptorId] = resp
	p.mu.Unlock()

	// Check if we have majority support

	if p.isCandidate && len(p.successRequests[p.currentTerm]) >= p.getMajority() {
		waitTime := time.Duration(rand.Intn(3000-1000+1)+1000) * time.Millisecond
		log.WithField("node-id", p.config.Id).
			WithField("wait-time", waitTime).
			Info("Waiting before becoming leader")

		// Random wait 1~3 seconds before becoming leader
		// Wait for other candidates to finish the election race
		time.Sleep(waitTime)
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.isCandidate && len(p.successRequests[p.currentTerm]) >= p.getMajority() {
			log.WithField("node-id", p.config.Id).
				WithField("term", resp.Term).
				Debug("Becoming leader")
			p.NewLeader = p.config.Id
			p.isLeader = true
			p.isCandidate = false
			p.NewLeaderCh <- p.config.Id
		} else {
			log.WithField("node-id", p.config.Id).
				WithField("term", resp.Term).
				Debug("Not a new leader")
			p.isCandidate = false
			p.isLeader = false
		}
	}
}

// // handleStatusRequest processes election status requests
// func (p *RaftElection) handleStatusRequest(req *pb.ElectionStatusRequest) {
// 	response := &pb.ElectionStatusResponse{
// 		CurrentTerm:   p.currentTerm,
// 		CurrentLeader: p.currentLeader,
// 		NodeState:     p.getNodeState(),
// 		ViewId:        p.currentTerm, // Use term as view ID
// 		LastHeartbeat: time.Now().Unix(),
// 	}

// 	p.sender.SendRPCToPeer(req.NodeId, "GetElectionStatus", response)
// }

// startElection initiates a new election round
func (p *RaftElection) startElection() {
	p.mu.Lock()
	p.currentTerm++
	p.isLeader = false
	p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", p.currentTerm).
		WithField("proposal-id", p.proposalId).
		Info("Starting new election")

	// Send Prepare requests
	p.startRaftPreparePhase()
}

// startPreparePhase initiates the Prepare phase
func (p *RaftElection) startRaftPreparePhase() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isCandidate = true
	p.isLeader = false
	p.proposalId = p.generateProposalId()

	request := &pb.RaftPrepareRequest{
		Term:       p.currentTerm,
		ProposalId: p.proposalId,
		ProposerId: p.config.Id,
		ViewId:     p.currentView,
		Timestamp:  time.Now().Unix(),
	}

	p.sender.Broadcast("raft-prepare", request)
}

// runElectionTimer handles election timeouts
func (p *RaftElection) runElectionManager() {
	for {
		select {
		case <-p.NewLeaderCh:
			p.startElection()
		case <-p.electionTimer.C:
			p.startElection()
			p.electionTimer.Reset(p.electionTimeout)
		case <-p.stopCh:
			return
		}
	}
}

// generateProposalId creates a unique proposal ID
func (p *RaftElection) generateProposalId() int64 {
	return time.Now().UnixNano() + int64(len(p.config.PeersAddress))
}

// getMajority returns the number of nodes needed for majority
func (p *RaftElection) getMajority() int {
	return len(p.config.PeersAddress)/2 + 1
}

// getNodeState returns the current state of this node
func (p *RaftElection) getNodeState() string {
	if p.isLeader {
		return "leader"
	}
	return "follower"
}

func (p *RaftElection) FindLeaderForView(viewId int64, callbackCh chan string) {
	// If we already know the leader for this view, return immediately
	if p.currentViewLeader != "" && p.currentView == viewId {
		callbackCh <- p.currentViewLeader
		return
	}

	// Otherwise start a new election
	go func() {
		p.startElection()
		// Wait for election result
		select {
		case leader := <-p.NewLeaderCh:
			callbackCh <- leader
		case <-time.After(p.electionTimeout):
			callbackCh <- ""
		}
	}()
}

func (p *RaftElection) GetCurrentLeader() string {
	return p.currentViewLeader
}

func (p *RaftElection) IsLeader() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isLeader
}

func (p *RaftElection) HandleMessage(msg proto.Message) error {
	// This method is called by external components to handle messages
	p.handleMessage(msg)
	return nil
}

func (p *RaftElection) Serve() error {
	return p.Start()
}

// Start begins the Raft election process
func (p *RaftElection) Start() error {
	log.WithField("node-id", p.config.Id).Info("Starting Raft election")

	// Start message handler loop
	go p.runMessageHandler()

	// Start election timeout handler
	go p.runElectionManager()

	return nil
}

// Stop halts the Raft election process
func (p *RaftElection) Stop() error {
	log.WithField("node-id", p.config.Id).Info("Stopping Raft election")
	p.stopCh <- struct{}{}
	return nil
}
