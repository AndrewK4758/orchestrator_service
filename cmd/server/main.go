package main

import (
	"log"
	"net"
	"os"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "doc_processor/shared_protos"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/callback_client"
	"orchestrator_service/internal/events"
	"orchestrator_service/internal/server"
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

	aiHost := os.Getenv("AI_SERVICE_ADDRESS")
	if aiHost == "" {
		aiHost = "localhost:50051"
	}

	// Connect to the external AI Service (acting as the 'Brain')
	aiClient, err := ai_client.NewClient(aiHost)
	if err != nil {
		log.Fatalf("Failed to create AI client: %v", err)
	}
	defer aiClient.Close()

	// Connect to NATS JetStream
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	pub, err := events.NewPublisher(natsURL)
	if err != nil {
		log.Fatalf("Failed to create NATS publisher: %v", err)
	}
	defer pub.Close()

	// Initialize the callback client manager for dynamic app-agnostic routing
	cbManager := callback_client.NewClientManager()
	defer cbManager.Close()

	grpcServer := grpc.NewServer()

	// Register the Orchestrator service and inject dependencies
	orchestratorServer := server.NewServer(aiClient, pub, cbManager)
	pb.RegisterOrchestratorServiceServer(grpcServer, orchestratorServer)

	// Register reflection service on gRPC server so clients can query it
	reflection.Register(grpcServer)

	log.Printf("Orchestrator Service starting on port %s...", port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
