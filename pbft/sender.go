package pbft

import (
	"context"
	"fmt"
	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"

	pb "github.com/Arman17Babaei/pbft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Sender struct {
	mu          *sync.RWMutex
	config      *configs.Config
	sendTimeout time.Duration
	maxRetries  int
	clients     map[string]pb.ClientClient
	otherNodes  map[string]pb.PbftClient
	pool        *ants.Pool
}

func NewSender(config *configs.Config) *Sender {
	pool, err := ants.NewPool(config.Grpc.MaxConcurrentStreams, ants.WithPreAlloc(true))
	if err != nil {
		log.WithError(err).Fatal("failed to create pool")
	}
	pbftClients := make(map[string]pb.PbftClient)
	for id, addr := range config.PeersAddress {
		if id == config.Id {
			continue
		}

		c, err := grpc.NewClient(
			fmt.Sprintf("%s:%d", addr.Host, addr.Port),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.WithError(err).Error("failed to create client")
		}

		pbftClients[id] = pb.NewPbftClient(c)
	}

	return &Sender{
		mu:          &sync.RWMutex{},
		config:      config,
		sendTimeout: time.Duration(config.Grpc.SendTimeoutMs) * time.Millisecond,
		maxRetries:  config.Grpc.MaxRetries,
		clients:     make(map[string]pb.ClientClient),
		otherNodes:  pbftClients,
		pool:        pool,
	}
}

func (s *Sender) Broadcast(method string, message proto.Message) {
	log.WithField("method", method).Debug("broadcast message")
	for id := range s.otherNodes {
		s.SendRPCToPeer(id, method, message)
	}
}

func (s *Sender) SendRPCToPeer(peerID string, method string, message proto.Message) {
	go func() {
		for i := 0; i < s.maxRetries; i++ {
			if _, exists := s.otherNodes[peerID]; !exists {
				log.WithField("peer", peerID).Error("unknown peer")
				panic("unknown peer")
				return
			}
			if err := s.sendRPCToPeer(s.otherNodes[peerID], method, message); err == nil {
				log.WithField("method", method).WithField("peer", peerID).Debug("message sent")
				monitoring.MessageStatusCounter.WithLabelValues(s.config.Id, peerID, method, "success").Inc()
				return
			} else {
				monitoring.MessageStatusCounter.WithLabelValues(s.config.Id, peerID, method, err.Error()).Inc()
			}
		}
	}()
}

func (s *Sender) SendRPCToClient(clientAddress, method string, message proto.Message) {
	_ = s.pool.Submit(func() {
		s.sendRPCToClient(clientAddress, method, message)
	})
}

func (s *Sender) sendRPCToClient(clientAddress, method string, message proto.Message) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.clients[clientAddress]; !ok {
		var err error
		c, err := grpc.NewClient(
			clientAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.WithError(err).Error("failed to create client")
			return
		}

		s.mu.RUnlock()
		s.mu.Lock()
		s.clients[clientAddress] = pb.NewClientClient(c)
		s.mu.Unlock()
		s.mu.RLock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "Response":
		if _, err := s.clients[clientAddress].Response(ctx, message.(*pb.ClientResponse)); err != nil {
			log.WithError(err).Warn("failed to send Reply")
		}
	default:
		log.Error("unknown method")
	}
}

func (s *Sender) sendRPCToPeer(client pb.PbftClient, method string, message proto.Message) error {
	if client == nil {
		log.Error("peer address is nil")
		return fmt.Errorf("nil address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.sendTimeout)
	defer cancel()

	switch method {
	case "Request":
		if _, err := client.Request(ctx, message.(*pb.ClientRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-Request", err.Error()).Inc()
			return err
		}
	case "PrePrepare":
		if _, err := client.PrePrepare(ctx, message.(*pb.PiggyBackedPrePareRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-PrePrepare", err.Error()).Inc()
			return err
		}
	case "Prepare":
		if _, err := client.Prepare(ctx, message.(*pb.PrepareRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-Prepare", err.Error()).Inc()
			return err
		}
	case "Commit":
		if _, err := client.Commit(ctx, message.(*pb.CommitRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-Commit", err.Error()).Inc()
			return err
		}
	case "Checkpoint":
		if _, err := client.Checkpoint(ctx, message.(*pb.CheckpointRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-Checkpoint", err.Error()).Inc()
			return err
		}
	case "GetStatus":
		if _, err := client.GetStatus(ctx, message.(*pb.StatusRequest)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-GetStatus", err.Error()).Inc()
			return err
		}
	case "Status":
		if _, err := client.Status(ctx, message.(*pb.StatusResponse)); err != nil {
			monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-Status", err.Error()).Inc()
			return err
		}
	default:
		monitoring.ErrorCounter.WithLabelValues("pbft_sender", "SendRPCToPeer-UnknownMethod", method).Inc()
		return fmt.Errorf("unknown method %s", method)
	}

	return nil
}
