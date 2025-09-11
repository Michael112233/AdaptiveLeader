package pbft

//go:generate mockgen -source=node.go -destination=node_mock.go -package=pbft

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/leader_election"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ISender interface {
	SendRPCToClient(clientAddress, method string, message proto.Message)
	SendRPCToPeer(peerID string, method string, message proto.Message)
	Broadcast(method string, message proto.Message)
}

type ViewChanger interface {
	RequestExecuted(viewId int64)
}

type LeaderElection interface {
	GetLeader(view int64) string
}

type ViewData struct {
	LeaderId       string
	ViewId         int64
	IsInViewChange bool

	InProgressRequests map[int64]any
	LastSequenceNumber int64

	TransactionStates map[int64]*TransactionState
}

type Node struct {
	mu        sync.RWMutex
	config    *configs.Config
	sender    ISender
	Store     *Store
	InputCh   <-chan proto.Message
	RequestCh <-chan *pb.ClientRequest

	LeaderElection LeaderElection
	viewChanger    ViewChanger

	CurrentViewId   int64
	Views           map[int64]*ViewData
	PendingRequests []*pb.ClientRequest

	Enabled   bool
	EnableCh  <-chan any
	DisableCh <-chan any
	StopCh    chan any
}

func NewNode(
	config *configs.Config,
	sender ISender,
	inputCh <-chan proto.Message,
	requestCh <-chan *pb.ClientRequest,
	enableCh <-chan any,
	disableCh <-chan any,
	store *Store,
) *Node {
	leaderElection := leader_election.NewRoundRobinLeaderElection(config)
	return &Node{
		config:    config,
		sender:    sender,
		Store:     store,
		InputCh:   inputCh,
		RequestCh: requestCh,

		LeaderElection: leaderElection,

		CurrentViewId: 0,
		Views: map[int64]*ViewData{
			0: NewViewData(0, 0, leaderElection.GetLeader(0)),
		},

		PendingRequests: []*pb.ClientRequest{},

		Enabled:   config.General.EnabledByDefault,
		EnableCh:  enableCh,
		DisableCh: disableCh,
		StopCh:    make(chan any),
	}
}

func NewViewData(viewId, initialSequenceNumber int64, leaderId string) *ViewData {
	return &ViewData{
		IsInViewChange: false,

		TransactionStates: make(map[int64]*TransactionState),

		InProgressRequests: make(map[int64]any),

		ViewId:             viewId,
		LeaderId:           leaderId,
		LastSequenceNumber: initialSequenceNumber,
	}
}

func (n *Node) SetViewChanger(viewChanger ViewChanger) {
	n.viewChanger = viewChanger
}

func (n *Node) Run() {
	n.sender.Broadcast("GetStatus", &pb.StatusRequest{ReplicaId: n.config.Id})
	for {
		monitoring.LeaderCounter.WithLabelValues(n.config.Id, n.Views[n.CurrentViewId].LeaderId).Inc()
		if !n.Enabled {
			<-n.EnableCh
			n.Enabled = true
			exhaustChannel(n.DisableCh)
			n.sender.Broadcast("GetStatus", &pb.StatusRequest{ReplicaId: n.config.Id})
		}

		select {
		case request := <-n.RequestCh:
			if len(n.Views[n.CurrentViewId].InProgressRequests) >= n.config.General.MaxOutstandingRequests {
				monitoring.ClientRequestStatusCounter.WithLabelValues("dropped-exceeding-in-progress").Inc()
				continue
			}
			n.handleClientRequest(request)
		case input := <-n.InputCh:
			n.handleInput(input)
		case <-n.DisableCh:
			n.Enabled = false
			exhaustChannel(n.EnableCh)
		case <-n.StopCh:
			return
		}
	}
}

func exhaustChannel[T any](channel <-chan T) {
	for {
		select {
		case <-channel:
			continue
		default:
			return
		}
	}
}

func (n *Node) Stop() {
	close(n.StopCh)
}
func (n *Node) isPrimary() bool {
	return n.config.Id == n.Views[n.CurrentViewId].LeaderId
}

func (n *Node) handleInput(input proto.Message) {
	timer := prometheus.NewTimer(monitoring.ResponseTimeSummary.WithLabelValues(n.config.Id, string(input.ProtoReflect().Descriptor().Name())))
	defer timer.ObserveDuration()

	switch msg := input.(type) {
	case *pb.PiggyBackedPrePareRequest:
		n.handlePrePrepareRequest(msg)
	case *pb.PrepareRequest:
		n.handlePrepareRequest(msg)
	case *pb.CommitRequest:
		n.handleCommitRequest(msg)
	case *pb.CheckpointRequest:
		n.handleCheckpointRequest(msg)
	case *pb.StatusRequest:
		n.handleStatusRequest(msg)
	case *pb.StatusResponse:
		n.handleStatusResponse(msg)
	}
}

func (n *Node) handleClientRequest(msg *pb.ClientRequest) {
	timer := prometheus.NewTimer(monitoring.ResponseTimeSummary.WithLabelValues(n.config.Id, "client-request"))
	defer timer.ObserveDuration()

	if !n.isPrimary() {
		log.WithField("request", msg.String()).Info("Received client request but not primary")
		log.WithField("my-id", n.config.Id).WithField("leader", n.Views[n.CurrentViewId].LeaderId).Info("Forwarding request to leader")
		n.sender.SendRPCToPeer(n.Views[n.CurrentViewId].LeaderId, "Request", msg)
		monitoring.ClientRequestStatusCounter.WithLabelValues("forward-to-leader").Inc()
		return
	}

	if n.Views[n.CurrentViewId].IsInViewChange {
		log.Warn("Dismissing request because in view change")
		monitoring.ClientRequestStatusCounter.WithLabelValues("in-view-change").Inc()
		return
	}

	log.WithField("request", msg.String()).Info("Received client request")

	if len(n.Views[n.CurrentViewId].InProgressRequests) >= n.config.General.MaxOutstandingRequests {
		log.Warn("Too many outstanding requests, putting request in pending queue")
		n.PendingRequests = append(n.PendingRequests, msg)
		monitoring.ClientRequestStatusCounter.WithLabelValues("too-many-outstanding").Inc()
		return
	}

	// --- MUTEX
	n.mu.Lock()
	n.Views[n.CurrentViewId].LastSequenceNumber++
	n.Views[n.CurrentViewId].InProgressRequests[n.Views[n.CurrentViewId].LastSequenceNumber] = struct{}{}
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.Views[n.CurrentViewId].InProgressRequests)))

	prepreareMessage := &pb.PiggyBackedPrePareRequest{
		PrePrepareRequest: &pb.PrePrepareRequest{
			ViewId:         n.Views[n.CurrentViewId].ViewId,
			SequenceNumber: n.Views[n.CurrentViewId].LastSequenceNumber,
			RequestDigest:  strconv.FormatInt(msg.TimestampNs, 10),
		},
		Requests: []*pb.ClientRequest{msg},
	}
	txnState := NewTransactionState(n.config, n.handlePreparedTxn, func(seqNo int64) { n.handleCommittedTxn(n.Views[n.CurrentViewId].ViewId, seqNo) })
	n.Views[n.CurrentViewId].TransactionStates[n.Views[n.CurrentViewId].LastSequenceNumber] = txnState
	n.mu.Unlock()
	// --- MUTEX

	go txnState.AddPrePrepare(prepreareMessage.PrePrepareRequest).
		AddToStore(n.Store.AddRequests, []*pb.ClientRequest{msg})

	n.sender.Broadcast("PrePrepare", prepreareMessage)
	monitoring.ClientRequestStatusCounter.WithLabelValues("success").Inc()
}

func (n *Node) handlePrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) {
	log.WithField("id", msg.PrePrepareRequest.SequenceNumber).WithField("my-id", n.config.Id).Info("PrePrepare received")
	if n.isPrimary() {
		log.WithField("request", msg.String()).WithField("my-id", n.config.Id).Error("Received pre-prepare request but is primary")
		return
	}
	viewId := msg.PrePrepareRequest.ViewId

	if n.Views[viewId].IsInViewChange {
		monitoring.ErrorCounter.WithLabelValues("pbft_node", "handlePrePrepareRequest", "in_view_change").Inc()
		return
	}

	log.WithField("request", msg.String()).Info("Received pre-prepare request")

	if !n.verifyPrePrepareRequest(msg) {
		monitoring.ErrorCounter.WithLabelValues("pbft_node", "handlePrePrepareRequest", "verification_failed").Inc()
		return
	}

	sequenceNumber := msg.PrePrepareRequest.SequenceNumber

	// --- MUTEX
	n.mu.Lock()
	n.Views[viewId].InProgressRequests[sequenceNumber] = struct{}{}
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.Views[viewId].InProgressRequests)))
	txnState, exists := n.Views[viewId].TransactionStates[sequenceNumber]
	if !exists {
		txnState = NewTransactionState(n.config, n.handlePreparedTxn, func(seqNo int64) { n.handleCommittedTxn(viewId, seqNo) })
		n.Views[viewId].TransactionStates[sequenceNumber] = txnState
	}
	n.mu.Unlock()
	// --- MUTEX

	go txnState.AddPrePrepare(msg.PrePrepareRequest)

	prepareMessage := &pb.PrepareRequest{
		ViewId:         msg.PrePrepareRequest.ViewId,
		SequenceNumber: sequenceNumber,
		RequestDigest:  msg.PrePrepareRequest.RequestDigest,
		ReplicaId:      n.config.Id,
	}

	go txnState.AddPrepare(prepareMessage)
	n.Store.AddRequests(msg.PrePrepareRequest.SequenceNumber, msg.Requests)
	n.sender.Broadcast("Prepare", prepareMessage)
}

func (n *Node) handlePrepareRequest(msg *pb.PrepareRequest) {
	log.WithField("request", msg.String()).Info("Received prepare request")

	viewId := msg.ViewId
	if n.Views[viewId].IsInViewChange {
		log.Warn("Dismissing prepare because in view change")
		return
	}

	if !n.verifyPrepareRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify prepare request")
		return
	}

	sequenceNumber := msg.SequenceNumber

	// --- MUTEX
	n.mu.Lock()
	txnState, exists := n.Views[viewId].TransactionStates[sequenceNumber]
	if !exists {
		txnState = NewTransactionState(n.config, n.handlePreparedTxn, func(seqNo int64) { n.handleCommittedTxn(viewId, seqNo) })
		n.Views[viewId].TransactionStates[sequenceNumber] = txnState
	}
	n.mu.Unlock()
	// --- MUTEX

	go txnState.AddPrepare(msg)
}

func (n *Node) handlePreparedTxn(commitMessage *pb.CommitRequest) {
	go n.handleCommitRequest(commitMessage)
	n.sender.Broadcast("Commit", commitMessage)
}

func (n *Node) handleCommitRequest(msg *pb.CommitRequest) {
	log.WithField("request", msg.String()).Info("Received commit request")

	viewId := msg.ViewId
	if n.Views[viewId].IsInViewChange {
		log.Warn("Dismissing commit because in view change")
		return
	}

	if !n.verifyCommitRequest(msg) {
		log.WithField("request", msg.String()).Warn("Failed to verify commit request")
		return
	}

	sequenceNumber := msg.SequenceNumber

	// --- MUTEX
	n.mu.Lock()
	txnState, exists := n.Views[viewId].TransactionStates[sequenceNumber]
	if !exists {
		txnState = NewTransactionState(n.config, n.handlePreparedTxn, func(seqNo int64) { n.handleCommittedTxn(viewId, seqNo) })
		n.Views[viewId].TransactionStates[sequenceNumber] = txnState
	}
	n.mu.Unlock()
	// --- MUTEX

	go txnState.AddCommit(msg)
}

func (n *Node) handleCommittedTxn(viewId, sequenceNumber int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reqs, resps, checkpoints := n.Store.Commit(sequenceNumber)

	delete(n.Views[viewId].InProgressRequests, sequenceNumber)
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.Views[viewId].InProgressRequests)))

	for _, checkpoint := range checkpoints {
		checkpoint.ViewId = viewId
		go n.handleCheckpointRequest(checkpoint)
		n.sender.Broadcast("Checkpoint", checkpoint)
	}

	for i, req := range reqs {
		reply := &pb.ClientResponse{
			ViewId:      viewId,
			TimestampNs: req.TimestampNs,
			ClientId:    req.ClientId,
			ReplicaId:   n.config.Id,
			Result:      resps[i],
		}
		n.sender.SendRPCToClient(req.Callback, "Response", reply)
		n.viewChanger.RequestExecuted(viewId)
	}

	if len(n.Views[viewId].InProgressRequests) < n.config.General.MaxOutstandingRequests && len(n.PendingRequests) > 0 {
		pendings := n.PendingRequests
		n.PendingRequests = []*pb.ClientRequest{}
		for _, pending := range pendings {
			go n.handleClientRequest(pending)
		}
	}
}

func (n *Node) handleCheckpointRequest(msg *pb.CheckpointRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.WithField("request", msg.String()).Info("Received checkpoint request")

	stableSequenceNumber := n.Store.AddCheckpointRequest(msg)
	if stableSequenceNumber == nil {
		return
	}

	viewId := msg.ViewId
	for seqNo := range n.Views[viewId].TransactionStates {
		if seqNo <= *stableSequenceNumber {
			delete(n.Views[viewId].TransactionStates, seqNo)
		}
	}

	for req := range n.Views[viewId].InProgressRequests {
		if req <= *stableSequenceNumber {
			delete(n.Views[viewId].InProgressRequests, req)
		}
	}
	monitoring.InProgressRequestsGauge.WithLabelValues(n.config.Id).Set(float64(len(n.Views[viewId].InProgressRequests)))
}

func (n *Node) GetCurrentPreparedRequests() []*pb.ViewChangePreparedMessage {
	n.mu.RLock()
	defer n.mu.RUnlock()

	prepreparedProof := make([]*pb.ViewChangePreparedMessage, 0)
	for _, txnState := range n.Views[n.CurrentViewId].TransactionStates {
		if !txnState.IsPrepared() {
			continue
		}

		prepreparedProof = append(prepreparedProof, &pb.ViewChangePreparedMessage{
			PrePrepareRequest: txnState.GetPreprepare(),
			PreparedMessages:  txnState.GetPrepares(),
		})
	}
	return prepreparedProof
}

func (n *Node) GoToViewChange() {
	n.Views[n.CurrentViewId].IsInViewChange = true
}

func (n *Node) HandleNewViewRequest(msg *pb.NewViewRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.WithField("my-id", n.config.Id).Info("Received new view request")

	if msg.NewViewId < n.CurrentViewId {
		monitoring.ErrorCounter.WithLabelValues("pbft_node", "HandleNewViewRequest", "old_view").Inc()
		log.WithField("request", msg.String()).WithField("current-view", n.CurrentViewId).Error("Received new view request with old view")
		return
	}

	minSeqNo := int64(math.MaxInt64)
	for _, preprepare := range msg.Preprepares {
		minSeqNo = min(minSeqNo, preprepare.SequenceNumber)
	}

	if minSeqNo == int64(math.MaxInt64) {
		for _, viewChangeProof := range msg.ViewChangeProof {
			minSeqNo = min(minSeqNo, viewChangeProof.LastStableSequenceNumber)
		}
	}
	if minSeqNo == int64(math.MaxInt64) {
		log.WithFields(log.Fields{
			"request":      msg.String(),
			"current-view": n.CurrentViewId,
			"min-seq-no":   minSeqNo,
		}).Fatal("No valid sequence number found in new view request")
	}

	for viewId := range n.Views {
		if viewId <= msg.NewViewId {
			delete(n.Views, viewId)
		}
	}

	n.CurrentViewId = msg.NewViewId
	n.Views[msg.NewViewId] = NewViewData(msg.NewViewId, minSeqNo-1, msg.ReplicaId)
	n.Store.Rollback(minSeqNo - 1)
	log.WithField("my-id", n.config.Id).WithField("leader-id", msg.ReplicaId).Error("entered new view")
	time.Sleep(1 * time.Second)
	for _, preprepare := range msg.Preprepares {
		n.Views[msg.NewViewId].TransactionStates[preprepare.SequenceNumber] = NewTransactionState(n.config, n.handlePreparedTxn, func(seqNo int64) { n.handleCommittedTxn(msg.NewViewId, seqNo) }).AddPrePrepare(preprepare)
		prepareMessage := &pb.PrepareRequest{
			ViewId:         preprepare.ViewId,
			SequenceNumber: preprepare.SequenceNumber,
			RequestDigest:  preprepare.RequestDigest,
			ReplicaId:      n.config.Id,
		}
		n.Views[msg.NewViewId].TransactionStates[preprepare.SequenceNumber].AddPrepare(prepareMessage)
		n.sender.Broadcast("Prepare", prepareMessage)
	}
}

func (n *Node) handleStatusRequest(msg *pb.StatusRequest) {
	log.WithField("request", msg.String()).Info("Received status request")

	statusResponse := &pb.StatusResponse{
		LastStableSequenceNumber: n.Store.GetLastStableCheckpoint().GetSequenceNumber(),
		CheckpointProof:          n.Store.GetLastStableCheckpoint().GetProof(),
	}

	n.sender.SendRPCToPeer(msg.ReplicaId, "Status", statusResponse)
}

func (n *Node) handleStatusResponse(msg *pb.StatusResponse) {
	log.WithField("request", msg.String()).Info("Received status response")

	if !n.verifyStatusResponse(msg) {
		log.WithField("request", msg.String()).Error("Failed to verify status response")
		return
	}

	if msg.LastStableSequenceNumber > n.Store.GetLastStableCheckpoint().GetSequenceNumber() {
		n.Store.UpdateLastStableCheckpoint(msg.CheckpointProof)
	}

	maxView := int64(0)
	for _, p := range msg.CheckpointProof {
		if p.ViewId > maxView {
			maxView = p.ViewId
		}
	}

	n.Views = map[int64]*ViewData{maxView: NewViewData(maxView, msg.LastStableSequenceNumber+1, n.LeaderElection.GetLeader(maxView))}
	// TODO: set n.Store
}

func (n *Node) verifyPrePrepareRequest(msg *pb.PiggyBackedPrePareRequest) bool {
	// TODO: check signature
	viewId := msg.PrePrepareRequest.ViewId
	if msg.PrePrepareRequest.ViewId != n.Views[viewId].ViewId {
		log.WithField("preprepare", msg.String()).WithField("my-view", n.Views[viewId].ViewId).Error("preprepare view mismatch")
		return false
	}

	sequenceNumber := msg.PrePrepareRequest.SequenceNumber
	return n.sequenceInWaterMark(sequenceNumber)
}

func (n *Node) verifyPrepareRequest(msg *pb.PrepareRequest) bool {
	// TODO: check signature
	if msg.ViewId < n.CurrentViewId {
		log.WithField("prepare", msg.String()).WithField("msg-view", msg.ViewId).Warn("prepare view mismatch")
		return false
	}

	sequenceNumber := msg.SequenceNumber
	return n.sequenceInWaterMark(sequenceNumber)
}

func (n *Node) verifyCommitRequest(msg *pb.CommitRequest) bool {
	// TODO: check signature
	if msg.ViewId < n.CurrentViewId {
		log.WithField("commit", msg.String()).WithField("msg-view", msg.ViewId).Warn("commit view mismatch")
		return false
	}

	sequenceNumber := msg.SequenceNumber
	return n.sequenceInWaterMark(sequenceNumber)
}

func (n *Node) verifyStatusResponse(msg *pb.StatusResponse) bool {
	// TODO: complete verification
	if msg.LastStableSequenceNumber < n.Store.GetLastStableCheckpoint().GetSequenceNumber() {
		return false
	}
	return len(msg.CheckpointProof) > 0
}

func (n *Node) sequenceInWaterMark(sequenceNumber int64) bool {
	lowWaterMark := n.Store.GetLastStableCheckpoint().GetSequenceNumber() + 1
	highWaterMark := lowWaterMark + int64(n.config.General.WaterMarkInterval)
	if sequenceNumber < lowWaterMark || sequenceNumber >= highWaterMark {
		log.WithField("sequence-number", sequenceNumber).
			WithField("low-water-mark", lowWaterMark).
			WithField("high-water-mark", highWaterMark).
			Warn("watermark mismatch")
		return false
	}

	return true
}
