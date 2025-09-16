package pbft

import (
	"context"
	"fmt"
	"google.golang.org/grpc/keepalive"
	"net"
	"time"

	"github.com/Arman17Babaei/pbft/pbft/configs"
	"github.com/Arman17Babaei/pbft/pbft/monitoring"

	log "github.com/sirupsen/logrus"

	pb "github.com/Arman17Babaei/pbft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	config     *configs.Config
	inputCh    chan<- proto.Message
	requestCh  chan<- *pb.ClientRequest
	enableCh   chan<- any
	disableCh  chan<- any
	listener   net.Listener
	grpcServer *grpc.Server

	Enabled bool

	pb.UnimplementedPbftServer
}

func NewService(inputCh chan<- proto.Message, requestCh chan<- *pb.ClientRequest, enableCh chan<- any, disableCh chan<- any, config *configs.Config) *Service {
	service := &Service{
		config:    config,
		inputCh:   inputCh,
		requestCh: requestCh,
		enableCh:  enableCh,
		disableCh: disableCh,
		Enabled:   config.General.EnabledByDefault,
	}

	var err error
	service.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", config.Address.Host, config.Address.Port))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	grpc.WithTransportCredentials(insecure.NewCredentials())
	// Many of these options are worthless, my apologies
	service.grpcServer = grpc.NewServer(
		grpc.MaxConcurrentStreams(2000),
		grpc.InitialWindowSize(1<<20),
		grpc.InitialConnWindowSize(1<<25),
		grpc.ReadBufferSize(1<<20),
		grpc.WriteBufferSize(1<<20),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
			MaxConnectionIdle: 30 * time.Minute,
		}),
		grpc.MaxRecvMsgSize(50<<20),
		grpc.MaxSendMsgSize(50<<20),
	)
	pb.RegisterPbftServer(service.grpcServer, service)

	return service
}

func (s *Service) Serve() {
	log.WithFields(log.Fields{"host": s.config.Address.Host, "port": s.config.Address.Port}).Printf("Starting gRPC server...")
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.WithError(err).Fatal("failed to serve")
	}
}

func (s *Service) Request(_ context.Context, req *pb.ClientRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return nil, fmt.Errorf("service is disabled")
	}

	log.WithField("my-id", s.config.Id).WithField("request", req).Info("client request received")
	isPut := putOrIgnore(s.requestCh, req)

	if !isPut {
		return nil, fmt.Errorf("request channel is full")
	}

	monitoring.MessageCounter.WithLabelValues(req.ClientId, s.config.Id, "request").Inc()
	return &pb.Empty{}, nil
}

func (s *Service) PrePrepare(_ context.Context, req *pb.PiggyBackedPrePareRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).WithField("request", req).Info("pre-prepare request received")
	s.inputCh <- req
	monitoring.MessageCounter.WithLabelValues("leader", s.config.Id, "preprepare").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) Prepare(_ context.Context, req *pb.PrepareRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).WithField("request", req).Info("prepare request received")
	s.inputCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetReplicaId(), s.config.Id, "prepare").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) Commit(_ context.Context, req *pb.CommitRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).WithField("request", req).Info("commit request received")
	s.inputCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetReplicaId(), s.config.Id, "commit").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) Checkpoint(_ context.Context, req *pb.CheckpointRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).WithField("request", req).Info("checkpoint request received")
	s.inputCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetReplicaId(), s.config.Id, "checkpoint").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) GetStatus(_ context.Context, req *pb.StatusRequest) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).Info("checkpoint request received")
	s.inputCh <- req
	monitoring.MessageCounter.WithLabelValues(req.GetReplicaId(), s.config.Id, "get-status").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) Status(_ context.Context, req *pb.StatusResponse) (*pb.Empty, error) {
	if !s.Enabled {
		return &pb.Empty{}, nil
	}

	log.WithField("my-id", s.config.Id).Info("status request received")
	s.inputCh <- req
	monitoring.MessageCounter.WithLabelValues("node", s.config.Id, "status").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) Enable(_ context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.WithField("my-id", s.config.Id).Info("enable request received")
	putOrIgnore[any](s.enableCh, req)

	s.Enabled = true
	monitoring.MessageCounter.WithLabelValues("load-test", s.config.Id, "enable").Inc()

	return &pb.Empty{}, nil
}

func (s *Service) Disable(_ context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.WithField("my-id", s.config.Id).Info("disable request received")
	putOrIgnore[any](s.disableCh, req)

	s.Enabled = false
	monitoring.MessageCounter.WithLabelValues("load-test", s.config.Id, "disable").Inc()

	return &pb.Empty{}, nil
}

func putOrIgnore[T any](channel chan<- T, value T) bool {
	select {
	case channel <- value:
		return true
	default:
		return false
	}
}

func (s *Service) EnableDelayedProposal(_ context.Context, req *pb.DelayedProposalRequest) (*pb.Empty, error) {
	log.WithField("my-id", s.config.Id).Info("enable delayed proposal request received")

	s.config.Attacks.DelayedProposal.Enabled = req.Enabled
	s.config.Attacks.DelayedProposal.WaitTimeFactor = req.WaitTimeFactor

	monitoring.MessageCounter.WithLabelValues("load-test", s.config.Id, "enable-delayed-proposal").Inc()

	return &pb.Empty{}, nil
}
