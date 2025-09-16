package paxos

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

	paxosCh    chan<- proto.Message
	listener   net.Listener
	grpcServer *grpc.Server

	pb.UnimplementedPaxosElectionServer
}

func NewService(paxosCh chan<- proto.Message, config *configs.Config) *Service {
	service := &Service{
		config:  config,
		paxosCh: paxosCh,
		Enabled: config.General.EnabledByDefault,
	}

	var err error
	service.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port+2000))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	service.grpcServer = grpc.NewServer()
	pb.RegisterPaxosElectionServer(service.grpcServer, service)

	return service
}

// PaxosPrepare handles incoming Prepare requests from other nodes
func (s *Service) PaxosPrepare(_ context.Context, req *pb.PaxosPrepareRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		WithField("proposer-id", req.ProposerId).
		Info("paxos prepare request received")

	// Forward the request to the election logic
	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "paxos-prepare").Inc()

	return &pb.Empty{}, nil
}

// PaxosPromise handles incoming Promise requests from other nodes
func (s *Service) PaxosPromise(_ context.Context, req *pb.PaxosPromiseRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("promised", req.Promised).
		WithField("acceptor-id", req.AcceptorId).
		Info("paxos promise request received")

	// Forward the request to the election logic
	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetAcceptorId(), s.config.Id, "paxos-promise").Inc()

	return &pb.Empty{}, nil
}

// PaxosAccept handles incoming Accept requests from other nodes
func (s *Service) PaxosAccept(_ context.Context, req *pb.PaxosAcceptRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("proposal-id", req.ProposalId).
		WithField("proposer-id", req.ProposerId).
		WithField("proposed-value", req.ProposedValue).
		Info("paxos accept request received")

	// Forward the request to the election logic
	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetProposerId(), s.config.Id, "paxos-accept").Inc()

	return &pb.Empty{}, nil
}

// PaxosSuccess handles incoming Success requests from other nodes
func (s *Service) PaxosSuccess(_ context.Context, req *pb.PaxosSuccessRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).
		WithField("term", req.Term).
		WithField("success", req.Success).
		WithField("acceptor-id", req.AcceptorId).
		Info("paxos success request received")

	// Forward the request to the election logic
	s.paxosCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetAcceptorId(), s.config.Id, "paxos-success").Inc()

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
	log.WithField("my-id", s.config.Id).Info("paxos election service enabled")
	return &pb.Empty{}, nil
}

// Disable disables the election service
func (s *Service) Disable(_ context.Context, _ *pb.Empty) (*pb.Empty, error) {
	s.Enabled = false
	log.WithField("my-id", s.config.Id).Info("paxos election service disabled")
	return &pb.Empty{}, nil
}

func (s *Service) Serve() {
	log.WithFields(log.Fields{"host": s.config.Address.Host, "port": s.config.Address.Port + 2000}).Printf("Starting paxos election gRPC server...")
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}

func (s *Service) Stop() {
	s.grpcServer.GracefulStop()
	s.listener.Close()
	close(s.paxosCh) // Maybe Not Needed
	log.Info("paxos service stopped")
}
