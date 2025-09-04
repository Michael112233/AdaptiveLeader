package pbft

import (
	"github.com/Arman17Babaei/pbft/pbft/configs"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"maps"
	"slices"
	"sync"
)

type TransactionState struct {
	config *configs.Config
	mu     sync.RWMutex

	preparedCallback  func(*pb.CommitRequest)
	committedCallback func(seqNo int64)

	digest     string
	preprepare *pb.PrePrepareRequest
	prepares   map[string]*pb.PrepareRequest
	commits    map[string]*pb.CommitRequest

	prepared  bool
	committed bool
}

func NewTransactionState(config *configs.Config, preparedCallback func(*pb.CommitRequest), committedCallback func(seqNo int64)) *TransactionState {
	return &TransactionState{
		config:            config,
		preparedCallback:  preparedCallback,
		committedCallback: committedCallback,
		prepares:          make(map[string]*pb.PrepareRequest),
		commits:           make(map[string]*pb.CommitRequest),
	}
}

func (t *TransactionState) AddPrePrepare(preprepare *pb.PrePrepareRequest) *TransactionState {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.digest != preprepare.RequestDigest {
		log.WithFields(log.Fields{"current": t.digest, "message": preprepare.RequestDigest}).Error("preprepare digest mismatch")
		t.digest = preprepare.RequestDigest
		for k, v := range t.prepares {
			if t.digest != v.RequestDigest {
				delete(t.prepares, k)
			}
		}
		for k, v := range t.commits {
			if t.digest != v.RequestDigest {
				delete(t.commits, k)
			}
		}
		if len(t.prepares) < 2*t.config.F() {
			t.prepared = false
		}
		if len(t.commits) <= 2*t.config.F() {
			t.committed = false
		}
	}

	t.preprepare = preprepare
	go t.checkCallbacks()
	return t
}

func (t *TransactionState) AddToStore(storeCallback func(int64, []*pb.ClientRequest), msgs []*pb.ClientRequest) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	storeCallback(t.preprepare.SequenceNumber, msgs)
}

func (t *TransactionState) AddPrepare(prepare *pb.PrepareRequest) *TransactionState {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.digest != "" && t.digest != prepare.RequestDigest {
		log.WithFields(log.Fields{"current": t.digest, "message": prepare.RequestDigest}).Error("prepare digest mismatch")
		return t
	}

	t.prepares[prepare.ReplicaId] = prepare

	if len(t.prepares) == 2*t.config.F() {
		t.prepared = true
	}

	go t.checkCallbacks()
	return t
}

func (t *TransactionState) AddCommit(commit *pb.CommitRequest) *TransactionState {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.digest != "" && t.digest != commit.RequestDigest {
		log.WithFields(log.Fields{"current": t.digest, "message": commit.RequestDigest}).Error("commit digest mismatch")
		return t
	}

	t.commits[commit.ReplicaId] = commit

	if len(t.commits) == 2*t.config.F()+1 {
		t.committed = true
	}

	go t.checkCallbacks()
	return t
}

func (t *TransactionState) IsPrepared() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.prepares) >= 2*t.config.F() && t.preprepare != nil
}

func (t *TransactionState) GetPreprepare() *pb.PrePrepareRequest {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.preprepare
}

func (t *TransactionState) GetPrepares() []*pb.PrepareRequest {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return slices.Collect(maps.Values(t.prepares))
}

func (t *TransactionState) checkCallbacks() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.prepared && t.preprepare != nil {
		t.prepared = false
		go t.preparedCallback(&pb.CommitRequest{
			ViewId:         t.preprepare.ViewId,
			SequenceNumber: t.preprepare.SequenceNumber,
			RequestDigest:  t.preprepare.RequestDigest,
			ReplicaId:      t.config.Id,
		})
	}
	if t.committed && t.preprepare != nil {
		t.committed = false
		go t.committedCallback(t.preprepare.SequenceNumber)
	}
}
