package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/AndrewK4758/shared_utils/envutil"

	"github.com/AndrewK4758/shared_utils/logger"

	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"

	"orchestrator_service/internal/events"
	"orchestrator_service/internal/server"

	"buf.build/go/protovalidate"
	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Attempt to load .env from standard deterministic execution paths
	envutil.LoadEnv(".env", "orchestrator_service/.env", "../../.env")
	logger.InitLogger()

	port := envutil.MustGetString("PORT")
	if port == "" {
		logger.Fatal("Orchestrator", "PORT environment variable is required")
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to listen on port %s: %v", port, err)
	}

	// Connect to NATS JetStream
	natsURL := envutil.MustGetString("NATS_URL")
	if natsURL == "" {
		logger.Fatal("Orchestrator", "NATS_URL environment variable is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pub, err := events.NewPublisher(ctx, natsURL)
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to create NATS publisher: %v", err)
	}
	defer pub.Close()

	// Connect to Traceability Service
	traceabilityAddr := envutil.MustGetString("TRACEABILITY_SERVICE_ADDRESS")
	if traceabilityAddr == "" {
		logger.Fatal("Orchestrator", "TRACEABILITY_SERVICE_ADDRESS environment variable is required")
	}

	maxMsgSizeMb := 100
	if envVal := envutil.MustGetString("GRPC_MAX_MESSAGE_SIZE_MB"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxMsgSizeMb = parsed
		}
	}

	traceConn, err := grpc.NewClient(traceabilityAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to connect to Traceability Service: %v", err)
	}
	defer traceConn.Close()
	traceClient := pb.NewTraceabilityQueryClient(traceConn)

	v, err := protovalidate.New()
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to initialize protovalidate: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSizeMb*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 30 * time.Second, //move to config
		}),
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(),
			server.ValidationInterceptor(v),
		),
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(),
		),
	)

	// Register the Orchestrator service and inject dependencies
	orchestratorServer := server.NewServer(ctx, pub, traceClient)
	pb.RegisterOrchestratorServiceServer(grpcServer, orchestratorServer)

	// Register reflection service on gRPC server so clients can query it
	reflection.Register(grpcServer)

	go func() {
		logger.Info("Orchestrator", "Orchestrator Service starting on port %s...", port)
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatal("Orchestrator", "Failed to serve gRPC server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("Orchestrator", "%v", "Shutting down Orchestrator Service gracefully...")
	grpcServer.GracefulStop()
}
