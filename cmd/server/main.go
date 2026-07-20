package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"strconv"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"
	"orchestrator_service/internal/server"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Attempt to load .env, ignore error if it doesn't exist
	_ = godotenv.Load()

	port := os.Getenv("PORT")
	if port == "" {
		port = "50052" // Using 50052 since ai_service uses 50051
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	// Connect to NATS JetStream
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// Connect to the external AI Service (acting as the 'Brain') via NATS
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	aiClient, err := ai_client.NewClient(ctx, natsURL)
	if err != nil {
		log.Fatalf("Failed to create AI client: %v", err)
	}
	defer aiClient.Close()

	pub, err := events.NewPublisher(ctx, natsURL)
	if err != nil {
		log.Fatalf("Failed to create NATS publisher: %v", err)
	}
	defer pub.Close()

	// Connect to Traceability Service
	traceabilityAddr := os.Getenv("TRACEABILITY_SERVICE_ADDRESS")
	if traceabilityAddr == "" {
		traceabilityAddr = "localhost:50053" // Default for local
	}

	maxMsgSizeMb := 100
	if envVal := os.Getenv("GRPC_MAX_MESSAGE_SIZE_MB"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxMsgSizeMb = parsed
		}
	}

	traceConn, err := grpc.NewClient(traceabilityAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		log.Fatalf("Failed to connect to Traceability Service: %v", err)
	}
	defer traceConn.Close()
	traceClient := pb.NewTraceabilityQueryClient(traceConn)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSizeMb*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 30 * time.Second,
		}),
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(),
		),
	)

	// Register the Orchestrator service and inject dependencies
	orchestratorServer := server.NewServer(ctx, aiClient, pub, traceClient)
	pb.RegisterOrchestratorServiceServer(grpcServer, orchestratorServer)

	// Register reflection service on gRPC server so clients can query it
	reflection.Register(grpcServer)

	go func() {
		log.Printf("Orchestrator Service starting on port %s...", port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve gRPC server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down Orchestrator Service gracefully...")
	grpcServer.GracefulStop()
}
