package paxos

//go:generate mockgen -source=paxos.go -destination=paxos_mock.go -package=paxos

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

// Node interface defines the methods that Paxos election needs from the main node
type Node interface {
	GetCurrentView() int64
	GetCurrentViewLeader() string
}

// PaxosElection implements the Paxos consensus algorithm for leader election
// It manages the Prepare, Accept, and Learn phases of the Paxos protocol
type PaxosElection struct {
	mu sync.RWMutex

	config *configs.Config
	node   Node
	sender *Sender

	// Paxos state variables
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
	paxosCh          <-chan proto.Message
	stopCh           chan struct{}

	// Timeout control
	electionTimeout time.Duration
	electionTimer   *time.Timer

	// Response tracking
	prepareRequests  map[int64]map[string]*pb.PaxosPrepareRequest
	promiseResponses map[int64]map[string]*pb.PaxosPromiseRequest
	acceptRequests   map[int64]map[string]*pb.PaxosAcceptRequest
	successRequests  map[int64]map[string]*pb.PaxosSuccessRequest
}

// NewPaxosElection creates a new Paxos election instance
func NewPaxosElection(config *configs.Config, node Node, sender *Sender, paxosCh <-chan proto.Message) *PaxosElection {
	electionTimeout := time.Duration(config.Timers.ViewChangeTimeoutMs) * time.Millisecond

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	return &PaxosElection{
		config:  config,
		node:    node,
		sender:  sender,
		paxosCh: paxosCh,

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

		prepareRequests:  make(map[int64]map[string]*pb.PaxosPrepareRequest),
		promiseResponses: make(map[int64]map[string]*pb.PaxosPromiseRequest),
		acceptRequests:   make(map[int64]map[string]*pb.PaxosAcceptRequest),
		successRequests:  make(map[int64]map[string]*pb.PaxosSuccessRequest),
	}
}

func (p *PaxosElection) UpdatePaxosElection(node Node) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.currentView = node.GetCurrentView()
	p.currentViewLeader = node.GetCurrentViewLeader()
}

func (p *PaxosElection) StartElection() {
	p.leaderElectionCh <- struct{}{}
}

// runMessageHandler processes incoming Paxos messages
func (p *PaxosElection) runMessageHandler() {
	for {
		select {
		case msg := <-p.paxosCh:
			p.handleMessage(msg)
		case <-p.stopCh:
			return
		}
	}
}

// handleMessage routes different types of Paxos messages to appropriate handlers
func (p *PaxosElection) handleMessage(msg proto.Message) {
	// 1. Recording the time of the message
	startTime := time.Now()
	timer := monitoring.ResponseTimeSummary.WithLabelValues(p.config.Id, "paxos-message")
	defer timer.Observe(time.Since(startTime).Seconds())

	// 2. Routing the message to the appropriate handler
	switch m := msg.(type) {
	case *pb.PaxosPrepareRequest:
		p.handlePrepareRequest(m)
	case *pb.PaxosPromiseRequest:
		p.handlePromiseResponse(m)
	case *pb.PaxosAcceptRequest:
		p.handleAcceptRequest(m)
	case *pb.PaxosSuccessRequest:
		p.handleSuccessResponse(m)
	default:
		log.WithField("message-type", fmt.Sprintf("%T", msg)).Warn("Unknown paxos message type")
	}
}

// handlePrepareRequest processes incoming Prepare phase requests
func (p *PaxosElection) handlePrepareRequest(req *pb.PaxosPrepareRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Info("Handling paxos-prepare request")

	// Update term if request has higher term
	if req.Term > p.currentTerm {
		p.currentTerm = req.Term
		p.currentViewLeader = "" // Clear current leader when term changes
		p.isLeader = false
		p.isCandidate = false
	} else if req.Term < p.currentTerm {
		// Reject prepare
		response := &pb.PaxosPromiseRequest{
			Term:                   p.currentTerm,
			Promised:               false,
			AcceptorId:             p.config.Id,
			ViewId:                 p.currentView,
			LastAcceptedProposalId: p.acceptedProposalId,
			LastAcceptedValue:      p.acceptedValue,
			Timestamp:              time.Now().Unix(),
		}
		p.sender.SendRPCToPeer(req.ProposerId, "paxos-promise", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Wrong Paxos-Prepare, Rejected")
		return
	}

	// Accept prepare if proposal ID is higher
	if req.ProposalId > p.maxProposalId {
		p.maxProposalId = req.ProposalId

		p.isLeader = false
		p.isCandidate = false

		// Send prepare response
		response := &pb.PaxosPromiseRequest{
			Term:                   p.currentTerm,
			Promised:               true,
			AcceptorId:             p.config.Id,
			ViewId:                 p.currentView,
			LastAcceptedProposalId: p.acceptedProposalId,
			LastAcceptedValue:      p.acceptedValue,
			Timestamp:              time.Now().Unix(),
		}

		if p.prepareRequests[p.currentTerm] == nil {
			p.prepareRequests[p.currentTerm] = make(map[string]*pb.PaxosPrepareRequest)
		}
		p.prepareRequests[p.currentTerm][req.ProposerId] = req
		p.sender.SendRPCToPeer(req.ProposerId, "paxos-promise", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Sending Paxos-Promise")
	} else {
		// Reject prepare
		response := &pb.PaxosPromiseRequest{
			Term:                   p.currentTerm,
			Promised:               false,
			AcceptorId:             p.config.Id,
			ViewId:                 p.currentView,
			LastAcceptedProposalId: p.acceptedProposalId,
			LastAcceptedValue:      p.acceptedValue,
			Timestamp:              time.Now().Unix(),
		}

		p.sender.SendRPCToPeer(req.ProposerId, "paxos-promise", response)
		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Sending Reject Paxos-Prepare")
	}
}

// handlePrepareResponse processes Prepare phase responses
func (p *PaxosElection) handlePromiseResponse(resp *pb.PaxosPromiseRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", resp.Term).
		Info("Handling paxos-promise response")

	// if node is not trying to be a leader or request is not for the current term
	// skip the response
	if p.currentTerm < resp.Term {
		p.prepareRequests[p.currentTerm] = nil
		p.currentTerm = resp.Term
		p.isLeader = false
		p.isCandidate = false

		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Paxos-Promise Response (Term is lower)")
		return
	}

	if !p.isCandidate || p.currentTerm != resp.Term || !resp.Promised {
		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Paxos-Promise Response")
		return
	}

	// Recording the response
	if p.promiseResponses[p.currentTerm] == nil {
		p.promiseResponses[p.currentTerm] = make(map[string]*pb.PaxosPromiseRequest)
	}

	p.promiseResponses[p.currentTerm][resp.AcceptorId] = resp

	// if got majority of promise, start the accept phase
	if len(p.promiseResponses[p.currentTerm]) >= p.getMajority() {
		p.acceptedProposalId = p.maxProposalId
		p.acceptedValue = p.config.Id

		p.sender.Broadcast("paxos-accept", &pb.PaxosAcceptRequest{
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
			Debug("Broadcasting Paxos-Accept Request")
	}
}

// handleAcceptRequest processes incoming Accept phase requests
func (p *PaxosElection) handleAcceptRequest(req *pb.PaxosAcceptRequest) {
	p.mu.Lock()

	log.WithField("node-id", p.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		Debug("Handling accept request")

	// 1. higer term request change the node to acceptor
	if req.Term > p.currentTerm {
		p.currentTerm = req.Term
		p.currentViewLeader = "" // Clear current leader when term changes
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
		response := &pb.PaxosSuccessRequest{
			Term:       p.currentTerm,
			Success:    true,
			AcceptorId: p.config.Id,
			ViewId:     p.currentView,
			Timestamp:  time.Now().Unix(),
		}

		p.sender.SendRPCToPeer(req.ProposerId, "paxos-success", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Sending Paxos-Success Response")

		// Set the new leader
		// p.NewLeader = req.ProposerId
		p.mu.Unlock()

		// // Wait and check whether the node is leader (Maybe other candidate also trying)
		// time.Sleep(1 * time.Second)
		// p.NewLeaderCh <- p.NewLeader
	} else {
		// Reject accept
		response := &pb.PaxosSuccessRequest{
			Term:       p.currentTerm,
			Success:    false,
			AcceptorId: p.config.Id,
			ViewId:     p.currentView,
			Timestamp:  time.Now().Unix(),
		}
		p.sender.SendRPCToPeer(req.ProposerId, "paxos-success", response)

		log.WithField("node-id", p.config.Id).
			WithField("term", req.Term).
			WithField("proposal-id", req.ProposalId).
			Debug("Rejecting Accept Request")
		p.mu.Unlock()
	}
}

// handleAcceptResponse processes Accept phase responses
func (p *PaxosElection) handleSuccessResponse(resp *pb.PaxosSuccessRequest) {
	p.mu.Lock()

	log.WithField("node-id", p.config.Id).
		WithField("term", resp.Term).
		Debug("Handling accept request")

	// if term is lower, skip the response and become acceptor
	if p.currentTerm < resp.Term {
		p.successRequests[p.currentTerm] = nil
		p.currentTerm = resp.Term
		p.currentViewLeader = "" // Clear current leader when term changes
		p.isLeader = false
		p.isCandidate = false

		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Paxos-Success Response (Term is lower)")
		p.mu.Unlock()
		return
	}

	// skip wrong response
	if !p.isCandidate || p.currentTerm != resp.Term || !resp.Success {
		log.WithField("node-id", p.config.Id).
			WithField("term", resp.Term).
			Debug("Skip Wrong Paxos-Success Response")
		p.mu.Unlock()
		return
	}
	if p.successRequests[p.currentTerm] == nil {
		p.successRequests[p.currentTerm] = make(map[string]*pb.PaxosSuccessRequest)
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
				Info("Becoming leader")
			p.NewLeader = p.config.Id
			p.currentViewLeader = p.config.Id
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
// func (p *PaxosElection) handleStatusRequest(req *pb.ElectionStatusRequest) {
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
func (p *PaxosElection) startElection() {
	p.mu.Lock()
	p.currentTerm++
	p.isLeader = false
	p.mu.Unlock()

	log.WithField("node-id", p.config.Id).
		WithField("term", p.currentTerm).
		WithField("proposal-id", p.proposalId).
		Info("Starting new election")

	// Send Prepare requests
	p.startPreparePhase()
}

// startPreparePhase initiates the Prepare phase
func (p *PaxosElection) startPreparePhase() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isCandidate = true
	p.isLeader = false
	p.proposalId = p.generateProposalId()

	request := &pb.PaxosPrepareRequest{
		Term:       p.currentTerm,
		ProposalId: p.proposalId,
		ProposerId: p.config.Id,
		ViewId:     p.currentView,
		Timestamp:  time.Now().Unix(),
	}

	p.sender.Broadcast("paxos-prepare", request)
}

// runElectionTimer handles election timeouts
func (p *PaxosElection) runElectionManager() {
	for {
		select {
		case <-p.NewLeaderCh:
			// Leader elected, reset timer for next election
			p.electionTimer.Reset(p.electionTimeout)
		case <-p.electionTimer.C:
			p.startElection()
			p.electionTimer.Reset(p.electionTimeout)
		case <-p.stopCh:
			return
		}
	}
}

// generateProposalId creates a unique proposal ID
func (p *PaxosElection) generateProposalId() int64 {
	return time.Now().UnixNano() + int64(len(p.config.PeersAddress))
}

// getMajority returns the number of nodes needed for majority
func (p *PaxosElection) getMajority() int {
	return len(p.config.PeersAddress)/2 + 1
}

// getNodeState returns the current state of this node
func (p *PaxosElection) getNodeState() string {
	if p.isLeader {
		return "leader"
	}
	return "follower"
}

func (p *PaxosElection) FindLeaderForView(viewId int64, callbackCh chan string) {
	// If we already know the leader for this view, return immediately
	if p.currentViewLeader != "" && p.currentView == viewId {
		callbackCh <- p.currentViewLeader
		return
	}

	// Otherwise start a new election
	go func() {
		p.startElection()

		// Wait for election result with a longer timeout
		// Use a separate channel to avoid competition
		waitCh := make(chan string, 1)

		// Start a goroutine to wait for leader election
		go func() {
			select {
			case leader := <-p.NewLeaderCh:
				waitCh <- leader
			case <-time.After(p.electionTimeout * 2): // Double the timeout
				waitCh <- "timeout"
			}
		}()

		// Wait for the result
		select {
		case leader := <-waitCh:
			callbackCh <- leader
		case <-time.After(p.electionTimeout * 3): // Triple timeout as fallback
			// If timeout, check if we know any leader
			if p.currentViewLeader != "" {
				callbackCh <- p.currentViewLeader
			} else {
				callbackCh <- "timeout"
			}
		}
	}()
}

func (p *PaxosElection) GetCurrentLeader() string {
	return p.currentViewLeader
}

func (p *PaxosElection) IsLeader() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isLeader
}

func (p *PaxosElection) HandleMessage(msg proto.Message) error {
	// This method is called by external components to handle messages
	p.handleMessage(msg)
	return nil
}

func (p *PaxosElection) Serve() error {
	return p.Start()
}

// Start begins the Paxos election process
func (p *PaxosElection) Start() error {
	log.WithField("node-id", p.config.Id).Info("Starting Paxos election")

	// Start message handler loop
	go p.runMessageHandler()

	// Start election timeout handler
	go p.runElectionManager()

	return nil
}

// Stop halts the Paxos election process
func (p *PaxosElection) Stop() error {
	log.WithField("node-id", p.config.Id).Info("Stopping Paxos election")
	p.stopCh <- struct{}{}
	return nil
}
