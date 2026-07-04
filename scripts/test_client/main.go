package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "doc_processor/shared_protos"
)

// ArtemisCallbackServer simulates the Client's webhook/gRPC server
type ArtemisCallbackServer struct {
	pb.UnimplementedStepCallbackServiceServer
	ResultChan chan string
}

func (s *ArtemisCallbackServer) OnStepComplete(ctx context.Context, req *pb.StepCompleteRequest) (*pb.StepCompleteResponse, error) {
	fmt.Printf("\n[ARTEMIS] Received Callback for Job: %s | Action: %s\n", req.JobId, req.ActionType)

	if req.ErrorMessage != "" {
		fmt.Printf("[ARTEMIS] Step Failed! Error: %s\n", req.ErrorMessage)
		s.ResultChan <- "ERROR: " + req.ErrorMessage
	} else {
		fmt.Printf("[ARTEMIS] Step Succeeded! Result JSON:\n%s\n", req.ResultJson)
		s.ResultChan <- req.ResultJson
	}

	return &pb.StepCompleteResponse{Success: true}, nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: go run main.go <path_to_pdf_or_directory>")
	}
	inputPath := os.Args[1]

	// 1. Start Artemis Local Callback Server
	lis, err := net.Listen("tcp", ":50055")
	if err != nil {
		log.Fatalf("Failed to listen for callbacks: %v", err)
	}
	grpcServer := grpc.NewServer()
	artemisServer := &ArtemisCallbackServer{
		ResultChan: make(chan string, 1),
	}
	pb.RegisterStepCallbackServiceServer(grpcServer, artemisServer)

	go func() {
		fmt.Println("🎧 [ARTEMIS] Callback Server listening on :50055")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve callbacks: %v", err)
		}
	}()

	// Wait a moment for server to start
	time.Sleep(1 * time.Second)

	// 2. Connect to Orchestrator
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorServiceClient(conn)

	// Determine file to process
	info, err := os.Stat(inputPath)
	if err != nil {
		log.Fatalf("Failed to stat input path: %v", err)
	}

	var files []string
	if info.IsDir() {
		entries, _ := os.ReadDir(inputPath)
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".pdf" {
				files = append(files, filepath.Join(inputPath, entry.Name()))
			}
		}
	} else {
		files = append(files, inputPath)
	}

	for _, file := range files {
		processFile(client, artemisServer, file)
	}
}

func processFile(client pb.OrchestratorServiceClient, artemis *ArtemisCallbackServer, inputPath string) {
	fmt.Println("\n==================================================")
	fmt.Printf("📄 [ARTEMIS] PROCESSING FILE: %s\n", inputPath)
	fmt.Println("==================================================")

	pdfBytes, err := os.ReadFile(inputPath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", inputPath, err)
		return
	}

	fmt.Println("\n[ARTEMIS] PHASE 1: Submitting Heavy Document Payload...")
	docRes, err := client.SubmitDocument(context.Background(), &pb.SubmitDocumentRequest{
		PdfContents: [][]byte{pdfBytes},
		TenantId:    "Artemis_Tenant",
	})
	if err != nil {
		log.Printf("Failed to submit document: %v", err)
		return
	}
	jobID := docRes.GetJobId()
	fmt.Printf("✅ Document Cached! Job ID: %s\n", jobID)

	// ReAct Loop Simulation
	fmt.Println("\n[ARTEMIS] PHASE 2: Submitting First ReAct Step (Classification)...")

	schema := `{
		"type": "object",
		"properties": {
			"DocumentType": {"type": "string", "enum": ["Invoice", "Contract", "Other"]},
			"Summary": {"type": "string"}
		},
		"required": ["DocumentType"]
	}`

	_, err = client.SubmitStep(context.Background(), &pb.SubmitStepRequest{
		JobId:              jobID,
		ActionType:         "AI_EXTRACTION",
		StepName:           "Classify Document",
		CustomInstructions: "Classify this document and provide a brief summary.",
		ExpectedSchema:     schema,
		CallbackAddress:    "localhost:50055",
	})
	if err != nil {
		log.Printf("Failed to submit step: %v", err)
		return
	}

	fmt.Println("⏳ [ARTEMIS] Waiting for Orchestrator Callback...")

	// Wait for Phase 3 (The Callback)
	result := <-artemis.ResultChan

	fmt.Println("\n[ARTEMIS] PHASE 4: ReAct Decision (Evaluating Output)")
	fmt.Printf("Decision Engine analyzed output: %s\n", result)
	fmt.Println("Decision: Next path selected based on classification. End of demo loop.")
}
