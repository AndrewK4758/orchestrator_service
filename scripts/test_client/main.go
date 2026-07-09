package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/AndrewK4758/shared_protos"
)

// ArtemisCallbackServer simulates the Client's webhook/gRPC server
type ArtemisCallbackServer struct {
	pb.UnimplementedOrchestratorCallbackServiceServer
	ResultChan chan string
}

func (s *ArtemisCallbackServer) OnStepComplete(ctx context.Context, req *pb.StepCompleteRequest) (*pb.StepCompleteResponse, error) {
	fmt.Printf("\n[Step %s] Received Callback for Job: %s\n", req.CompletedStepId, req.JobId)

	bytes, _ := req.GlobalState.MarshalJSON()
	fmt.Printf("[%s] Step Succeeded! Global State JSON:\n%s\n", req.JobId, string(bytes))
	s.ResultChan <- string(bytes)

	return &pb.StepCompleteResponse{Success: true}, nil
}

func (s *ArtemisCallbackServer) OnJobFailed(ctx context.Context, req *pb.JobFailedRequest) (*pb.JobFailedResponse, error) {
	fmt.Printf("\n[Step %s] Failed for Job: %s\n", req.FailedStepId, req.JobId)
	fmt.Printf("[%s] Step Failed! Error: %s\n", req.JobId, req.ErrorMessage)
	s.ResultChan <- "ERROR: " + req.ErrorMessage

	return &pb.JobFailedResponse{Success: true}, nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: go run main.go <path_to_pdf_or_directory>")
	}
	inputPath := os.Args[1]

	// 1. Start Local Callback Server
	lis, err := net.Listen("tcp", ":50055")
	if err != nil {
		log.Fatalf("Failed to listen for callbacks: %v", err)
	}
	grpcServer := grpc.NewServer()
	artemisServer := &ArtemisCallbackServer{
		ResultChan: make(chan string, 1),
	}
	pb.RegisterOrchestratorCallbackServiceServer(grpcServer, artemisServer)

	go func() {
		fmt.Println("🎧 Callback Server listening on :50055")
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
	fmt.Printf("📄 [test_client] PROCESSING FILE: %s\n", inputPath)
	fmt.Println("==================================================")

	pdfBytes, err := os.ReadFile(inputPath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", inputPath, err)
		return
	}

	fmt.Println("\n[test_client] PHASE 1: Submitting Initial State...")

	b64Doc := base64.StdEncoding.EncodeToString(pdfBytes)
	initialState, _ := structpb.NewStruct(map[string]interface{}{
		"document_b64": b64Doc,
	})

	jobReq := &pb.SubmitJobRequest{
		Context: &pb.WorkflowContext{
			Metadata: map[string]string{
				"app_id":    "CLI_TEST_APP",
				"tenant_id": "TEST_TENANT",
			},
			GlobalState: initialState,
		},
	}

	docRes, err := client.SubmitJob(context.Background(), jobReq)
	if err != nil {
		log.Printf("Failed to submit document: %v", err)
		return
	}
	jobID := docRes.GetJobId()
	fmt.Printf("✅ Document Cached! Job ID: %s\n", jobID)

	// ReAct Loop Simulation
	fmt.Printf("\n[%s] PHASE 2: Submitting First ReAct Step (Extraction)...\n", jobID)

	// Submit a generic step using the new generic StepDefinition
	stepReq := &pb.SubmitStepRequest{
		Context: &pb.WorkflowContext{
			JobId: jobID,
		},
		CallbackAddress: "localhost:50055",
		Steps: []*pb.StepDefinition{
			{
				StepName:           "Extract Invoice Data",
				CustomInstructions: "Extract the invoice number and total amount.",
				ExpectedSchema:     `{"type": "object", "properties": {"invoice_number": {"type": "string"}, "total": {"type": "number"}}}`,
				WritesToStateKey:   "invoice_data",
			},
		},
	}

	_, err = client.SubmitStep(context.Background(), stepReq)
	if err != nil {
		log.Printf("Failed to submit step: %v", err)
		return
	}

	fmt.Printf("⏳ [%s] Waiting for Orchestrator Callback...\n", jobID)

	// Wait for Phase 3 (The Callback)
	result := <-artemis.ResultChan

	fmt.Printf("\n[%s] PHASE 4: ReAct Decision (Evaluating Output)\n", jobID)
	fmt.Printf("Decision Engine analyzed output: %s\n", result)
	fmt.Println("Decision: Next path selected based on classification. End of demo loop.")
}
