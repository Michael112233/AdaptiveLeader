package pbft

import (
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	"slices"
	"strconv"
	"sync"
	"time"

	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
)

type CheckpointProof struct {
	mu    sync.RWMutex
	proof []*pb.CheckpointRequest
}

func (cpp *CheckpointProof) GetSequenceNumber() int64 {
	if cpp == nil {
		return 0
	}

	return cpp.proof[0].SequenceNumber
}

func (cpp *CheckpointProof) GetProof() []*pb.CheckpointRequest {
	if cpp == nil {
		return []*pb.CheckpointRequest{}
	}

	return cpp.proof
}

type CheckpointId struct {
	sequenceNumber int64
	digest         string
}

type State struct {
	value map[string]int
}

type Store struct {
	mu                           sync.RWMutex
	config                       *configs.Config
	unstableCheckpoints          map[CheckpointId]*CheckpointProof
	lastStableCheckpoint         *CheckpointProof
	state                        *State
	requests                     map[int64][]*pb.ClientRequest
	lastAppliedSequenceNumber    int64
	lastSentResultSequenceNumber int64
	committedRequests            map[int64][]*pb.ClientRequest
}

func NewStore(config *configs.Config) *Store {
	return &Store{
		config:                    config,
		unstableCheckpoints:       make(map[CheckpointId]*CheckpointProof),
		lastStableCheckpoint:      nil,
		lastAppliedSequenceNumber: 0,
		committedRequests:         make(map[int64][]*pb.ClientRequest),
		state:                     &State{value: make(map[string]int)},
		requests:                  make(map[int64][]*pb.ClientRequest),
	}
}

func (s *Store) GetLastStableCheckpoint() *CheckpointProof {
	return s.lastStableCheckpoint
}

func (s *Store) UpdateLastStableCheckpoint(checkpointProof []*pb.CheckpointRequest) {
	if s.lastStableCheckpoint == nil || s.lastStableCheckpoint.GetSequenceNumber() < checkpointProof[0].SequenceNumber {
		s.lastStableCheckpoint = &CheckpointProof{proof: checkpointProof}
	}
	if s.lastAppliedSequenceNumber < checkpointProof[0].SequenceNumber {
		s.lastAppliedSequenceNumber = checkpointProof[0].SequenceNumber
	}
}

func (s *Store) AddCheckpointRequest(checkpoint *pb.CheckpointRequest) *int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.WithField("checkpoint", checkpoint.String()).Debug("adding checkpoint request to store")

	if checkpoint.SequenceNumber < s.GetLastStableCheckpoint().GetSequenceNumber() {
		log.WithField("checkpoint", checkpoint.String()).Warn("stale checkpoint")
		return nil
	}

	id := newCheckpointId(checkpoint)
	if _, ok := s.unstableCheckpoints[id]; !ok {
		s.unstableCheckpoints[id] = &CheckpointProof{proof: []*pb.CheckpointRequest{}}
	}

	s.unstableCheckpoints[id].proof = append(s.unstableCheckpoints[id].proof, checkpoint)
	if len(s.unstableCheckpoints[id].proof) == 2*s.config.F()+1 {
		sequenceNumber := &s.unstableCheckpoints[id].proof[0].SequenceNumber
		s.stabilizeCheckpoint(s.unstableCheckpoints[id])
		return sequenceNumber
	}

	return nil
}

func (s *Store) AddRequests(sequenceNumber int64, reqs []*pb.ClientRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.WithField("sequence-number", sequenceNumber).Debug("adding requests to store")
	s.requests[sequenceNumber] = reqs
}

func (s *Store) Commit(seqNo int64) ([]*pb.ClientRequest, []*pb.OperationResult, []*pb.CheckpointRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.committedRequests[seqNo] = s.requests[seqNo]

	if s.lastAppliedSequenceNumber > seqNo {
		monitoring.ErrorCounter.WithLabelValues("store", "commit", "skipped-seq-no").Inc()
	}

	if s.lastAppliedSequenceNumber < seqNo-100 {
		log.WithField("seq-no", seqNo).WithField("last-applied", s.lastAppliedSequenceNumber).Error("committing a sequence number far ahead of last applied")
	}

	var reqs []*pb.ClientRequest
	var results []*pb.OperationResult
	var checkpoints []*pb.CheckpointRequest

	for ; s.committedRequests[s.lastAppliedSequenceNumber+1] != nil; s.lastAppliedSequenceNumber++ {
		curSeqNo := s.lastAppliedSequenceNumber + 1
		monitoring.ExecutedRequestsGauge.WithLabelValues(s.config.Id).Set(float64(curSeqNo))
		requests := s.requests[curSeqNo]

		if curSeqNo > s.lastSentResultSequenceNumber {
			reqs = append(reqs, requests...)
		}

		for _, req := range requests {
			monitoring.ClientRequestLatencySummary.WithLabelValues(s.config.Id).Observe(time.Since(time.Unix(0, req.GetTimestampNs())).Seconds())
			operationResult := s.state.apply(req.Operation)
			if curSeqNo > s.lastSentResultSequenceNumber {
				results = append(results, operationResult)
			}
		}

		s.lastSentResultSequenceNumber = max(s.lastSentResultSequenceNumber, curSeqNo)

		if int(curSeqNo)%s.config.General.CheckpointInterval == 0 {
			log.WithField("seq-no", curSeqNo).WithField("replica", s.config.Id).Info("created checkpoint for seq-no")
			checkpoints = append(checkpoints, &pb.CheckpointRequest{
				SequenceNumber: curSeqNo,
				StateDigest:    []byte(s.state.digest()),
				ReplicaId:      s.config.Id,
			})
		}
	}

	return reqs, results, checkpoints
}

func (s *Store) Rollback(toSeqNo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if toSeqNo >= s.lastSentResultSequenceNumber {
		// This needs to get information from other replicas to get state
		s.lastSentResultSequenceNumber = toSeqNo
	}

	// This needs to rollback state as well
	s.lastAppliedSequenceNumber = toSeqNo
}

func (s *State) digest() string {
	return strconv.Itoa(s.value[""])
}

func (s *Store) stabilizeCheckpoint(checkpoint *CheckpointProof) {
	if s.lastStableCheckpoint.GetSequenceNumber() >= checkpoint.proof[0].SequenceNumber {
		log.WithField("checkpoint", checkpoint.proof[0].SequenceNumber).Debug("not stabilizing checkpoint")
		return
	}

	log.WithField("checkpoint", checkpoint.proof[0].SequenceNumber).Debug("stabilizing checkpoint")
	s.lastStableCheckpoint = checkpoint

	// remove stale checkpoints
	for id, cp := range s.unstableCheckpoints {
		if cp.proof[0].SequenceNumber <= checkpoint.proof[0].SequenceNumber {
			delete(s.unstableCheckpoints, id)
		}
	}

	// remove stale requests
	for seqNo := range s.requests {
		if seqNo <= checkpoint.proof[0].SequenceNumber {
			delete(s.requests, seqNo)
		}
	}

	// remove unapplied requests
	haveStaleState := false
	for seqNo := range s.committedRequests {
		if seqNo <= checkpoint.proof[0].SequenceNumber {
			delete(s.committedRequests, seqNo)
			haveStaleState = true
		}
	}

	if haveStaleState {
		s.state = &State{value: make(map[string]int)}
		// In reality this should be a call to other services to update missing requests
		// Since it can be done outside the critical path, we can ignore it here
		s.state.apply(&pb.Operation{Type: pb.Operation_ADD, Key: "", Value: string(checkpoint.proof[0].StateDigest)})
	}

	if s.lastAppliedSequenceNumber < checkpoint.proof[0].SequenceNumber {
		monitoring.ErrorCounter.WithLabelValues("store", "stabilize-checkpoint", "skipped-seq-no").Add(float64(checkpoint.proof[0].SequenceNumber - s.lastAppliedSequenceNumber))
		s.lastAppliedSequenceNumber = checkpoint.proof[0].SequenceNumber
	}
}

func (s *State) apply(operation *pb.Operation) *pb.OperationResult {
	value := 0
	if slices.Contains([]pb.Operation_Type{pb.Operation_ADD, pb.Operation_SUB}, operation.Type) {
		var err error
		value, err = strconv.Atoi(operation.Value)

		if err != nil {
			log.WithField("request", operation.String()).Error("failed to convert value to int")
			return &pb.OperationResult{
				Value: strconv.Itoa(s.value[operation.Key]),
			}
		}
	}

	switch operation.Type {
	case pb.Operation_GET:
		// do nothing
	case pb.Operation_ADD:
		s.value[operation.Key] += value
	case pb.Operation_SUB:
		s.value[operation.Key] -= value
	default:
		log.WithField("operation", operation.String()).Error("unknown operation type")
	}

	return &pb.OperationResult{
		Value: strconv.Itoa(s.value[operation.Key]),
	}
}

func newCheckpointId(checkpoint *pb.CheckpointRequest) CheckpointId {
	return CheckpointId{
		sequenceNumber: checkpoint.SequenceNumber,
		digest:         string(checkpoint.StateDigest),
	}
}
