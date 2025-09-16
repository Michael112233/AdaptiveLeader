package raft

import (
	"context"
	"fmt"
	"net"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"
	pb "github.com/Arman17Babaei/pbft/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// proto:
// UnimplementedPaxosServer
// RegisterPaxosServer

type Service struct {
	config *configs.Config

	Enabled bool

	raftCh     chan<- proto.Message
	listener   net.Listener
	grpcServer *grpc.Server

	pb.UnimplementedRaftElectionServer
}

func NewService(raftCh chan<- proto.Message, config *configs.Config) *Service {
	service := &Service{
		config:  config,
		raftCh:  raftCh,
		Enabled: config.General.EnabledByDefault,
	}

	var err error
	service.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port+2000))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	service.grpcServer = grpc.NewServer()
	pb.RegisterRaftElectionServer(service.grpcServer, service)

	return service
}

// PaxosPrepare handles incoming Prepare requests from other nodes
func (s *Service) RaftPrepare(_ context.Context, req *pb.RaftPrepareRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		WithField("proposer-id", req.ProposerId).
		Debug("paxos prepare request received")

	// Forward the request to the election logic
	s.raftCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "raft-prepare").Inc()

	return &pb.Empty{}, nil
}

// PaxosPromise handles incoming Promise requests from other nodes
func (s *Service) RaftPromise(_ context.Context, req *pb.RaftPromiseRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("promised", req.Promised).
		WithField("acceptor-id", req.AcceptorId).
		Debug("raft promise request received")

	// Forward the request to the election logic
	s.raftCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetAcceptorId(), s.config.Id, "raft-promise").Inc()

	return &pb.Empty{}, nil
}

// PaxosAccept handles incoming Accept requests from other nodes
func (s *Service) RaftAccept(_ context.Context, req *pb.RaftAcceptRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		WithField("proposer-id", req.ProposerId).
		WithField("proposed-value", req.ProposedValue).
		Debug("raft accept request received")

	// Forward the request to the election logic
	s.raftCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "raft-accept").Inc()

	return &pb.Empty{}, nil
}

// PaxosSuccess handles incoming Success requests from other nodes
func (s *Service) RaftSuccess(_ context.Context, req *pb.RaftSuccessRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("success", req.Success).
		WithField("acceptor-id", req.AcceptorId).
		Debug("raft success request received")

	// Forward the request to the election logic
	s.raftCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetAcceptorId(), s.config.Id, "raft-success").Inc()

	return &pb.Empty{}, nil
}

// GetElectionStatus returns the current election status
func (s *Service) GetElectionStatus(_ context.Context, req *pb.ElectionStatusRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("requesting-node", req.NodeId).
		Debug("election status request received")

	return &pb.Empty{}, nil
}

// Enable enables the election service
func (s *Service) Enable(_ context.Context, _ *pb.Empty) (*pb.Empty, error) {
	s.Enabled = true
	log.WithField("my-id", s.config.Id).Info("raft election service enabled")
	return &pb.Empty{}, nil
}

// Disable disables the election service
func (s *Service) Disable(_ context.Context, _ *pb.Empty) (*pb.Empty, error) {
	s.Enabled = false
	log.WithField("my-id", s.config.Id).Info("raft election service disabled")
	return &pb.Empty{}, nil
}

func (s *Service) Serve() {
	log.WithFields(log.Fields{"host": s.config.Address.Host, "port": s.config.Address.Port + 2000}).Printf("Starting raft election gRPC server...")
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}

func (s *Service) Stop() {
	s.grpcServer.GracefulStop()
	s.listener.Close()
	close(s.raftCh) // Maybe Not Needed
	log.Info("raft service stopped")
}
