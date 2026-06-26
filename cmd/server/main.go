package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/server"
	pb "orchestrator_service/proto"
)

func main() {
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

	grpcServer := grpc.NewServer()

	// Register the Orchestrator service and inject the AI Client
	orchestratorServer := server.NewServer(aiClient)
	pb.RegisterOrchestratorServiceServer(grpcServer, orchestratorServer)

	// Register reflection service on gRPC server so clients can query it
	reflection.Register(grpcServer)

	log.Printf("Orchestrator Service starting on port %s...", port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
