package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Sender struct {
	config *configs.Config
	conns  map[string]*grpc.ClientConn
	mu     sync.RWMutex
}

// proto:
// RaftElectionClient
// NewRaftElectionClient

func NewSender(config *configs.Config) *Sender {
	return &Sender{
		config: config,
		conns:  make(map[string]*grpc.ClientConn),
		mu:     sync.RWMutex{}, // Non-Necessary, 0-value is fine
	}
}

func (s *Sender) getClient(id string) (pb.RaftElectionClient, error) {
	// 1 Check Whether Connection Exists, if so, return the client
	s.mu.RLock()
	conn, ok := s.conns[id]
	s.mu.RUnlock()
	if ok {
		return pb.NewRaftElectionClient(conn), nil
	}

	// 2 If Not, Establish Connection
	s.mu.Lock()
	defer s.mu.Unlock()

	// 2.1 Double Check Whether Connection Exists
	// As in Execution Interval, Other Threads might have established the connection, so we need to check again
	if conn, ok = s.conns[id]; ok {
		return pb.NewRaftElectionClient(conn), nil
	}

	// 2.2 Establish Connection
	address := s.config.GetAddress(id)
	if address == nil {
		return nil, fmt.Errorf("no address found for replica %s", id)
	}
	// Specific to Leader Election Port (Original Port + 2000)
	address.Port += 2000

	target := fmt.Sprintf("%s:%d", address.Host, address.Port)
	log.WithField("from", s.config.Id).
		WithField("to", id).
		WithField("target", target).
		Info("Attempting to connect to peer")

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.WithError(err).
			WithField("from", s.config.Id).
			WithField("to", id).
			WithField("target", target).
			Error("Failed to connect to peer")
		return nil, fmt.Errorf("failed to connect to replica %s: %v", id, err)
	}

	log.WithField("from", s.config.Id).
		WithField("to", id).
		WithField("target", target).
		Info("Successfully connected to peer")

	s.conns[id] = conn
	return pb.NewRaftElectionClient(conn), nil
}

func (s *Sender) SendRaftPrepare(targetId string, req *pb.RaftPrepareRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.RaftPrepare(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send raft-prepare request")
		monitoring.ErrorCounter.WithLabelValues("raft-prepare", "SendRaftPrepare", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendRaftAccept(targetId string, req *pb.RaftAcceptRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.RaftAccept(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send raft-accept request")
		monitoring.ErrorCounter.WithLabelValues("raft-accept", "SendRaftAccept", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendRaftSuccess(targetId string, req *pb.RaftSuccessRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.RaftSuccess(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send raft-success request")
		monitoring.ErrorCounter.WithLabelValues("raft-success", "SendRaftSuccess", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) SendRaftPromise(targetId string, req *pb.RaftPromiseRequest) error {
	client, err := s.getClient(targetId)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.RaftPromise(ctx, req)
	if err != nil {
		log.WithError(err).WithField("target", targetId).Error("failed to send raft-promise request")
		monitoring.ErrorCounter.WithLabelValues("raft-promise", "SendRaftPromise", "grpc_error").Inc()
		return err
	}

	return nil
}

func (s *Sender) Broadcast(msgType string, message proto.Message) error {
	for _, replicaId := range s.config.ReplicaIds() {
		if replicaId == s.config.Id {
			continue
		}
		switch msgType {
		case "raft-prepare":
			s.SendRaftPrepare(replicaId, message.(*pb.RaftPrepareRequest))
		case "raft-accept":
			s.SendRaftAccept(replicaId, message.(*pb.RaftAcceptRequest))
		default:
			return fmt.Errorf("invalid message type: %s", msgType)
		}
	}
	return nil
}

func (s *Sender) SendRPCToPeer(id string, msgType string, message proto.Message) error {
	switch msgType {
	case "raft-prepare":
		s.SendRaftPrepare(id, message.(*pb.RaftPrepareRequest))
	case "raft-promise":
		s.SendRaftPromise(id, message.(*pb.RaftPromiseRequest))
	case "raft-accept":
		s.SendRaftAccept(id, message.(*pb.RaftAcceptRequest))
	case "raft-success":
		s.SendRaftSuccess(id, message.(*pb.RaftSuccessRequest))
	default:
		return fmt.Errorf("invalid message type: %s", msgType)
	}
	return nil
}
