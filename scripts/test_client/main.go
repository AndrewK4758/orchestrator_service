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

type ArtemisCallbackServer struct {
	pb.UnimplementedOrchestratorCallbackServiceServer
	ResultChan chan string
}

func (s *ArtemisCallbackServer) OnNodeExecutionComplete(stream pb.OrchestratorCallbackService_OnNodeExecutionCompleteServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		jobID := ""
		if req.Identity != nil {
			jobID = req.Identity.JobId
		}
		fmt.Printf("\n[Step %s] Received Callback for Job: %s\n", req.CompletedStepId, jobID)

		bytes, _ := req.GlobalState.MarshalJSON()
		outBytes, _ := req.NodeOutput.MarshalJSON()
		fmt.Printf("[%s] Step Succeeded!\nStep Output: %s\nGlobal State JSON:\n%s\n", jobID, string(outBytes), string(bytes))
		s.ResultChan <- string(bytes)

		if err := stream.Send(&pb.NodeExecutionCompleteResponse{Success: true}); err != nil {
			return err
		}
	}
}

func (s *ArtemisCallbackServer) OnJobFailed(ctx context.Context, req *pb.JobFailedRequest) (*pb.JobFailedResponse, error) {
	jobID := ""
	if req.Identity != nil {
		jobID = req.Identity.JobId
	}
	fmt.Printf("\n[Step %s] Failed for Job: %s\n", req.FailedStepId, jobID)
	fmt.Printf("[%s] Step Failed! Error: %s\n", jobID, req.ErrorMessage)
	s.ResultChan <- "ERROR: " + req.ErrorMessage

	return &pb.JobFailedResponse{Success: true}, nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: go run main.go <path_to_pdf_or_directory>")
	}
	inputPath := os.Args[1]

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

	time.Sleep(1 * time.Second)

	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorServiceClient(conn)

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
	initialState, _ := structpb.NewStruct(map[string]any{
		"document_b64": b64Doc,
	})

	nodeReq := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job_test_123",
			AppId:    "CLI_TEST_APP",
			TenantId: "TEST_TENANT",
		},
		StepId:          "step_1",
		GlobalState:     initialState,
		Metadata:        map[string]string{},
		CallbackAddress: "localhost:50055",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           "Extract Invoice Data",
			SystemInstructions: "Extract the invoice number and total amount.",
			ExpectedSchema:     `{"type": "object", "properties": {"invoice_number": {"type": "string"}, "total": {"type": "number"}}}`,
		},
	}

	stream, err := client.ExecuteWorkflowNode(context.Background())
	if err != nil {
		log.Printf("Failed to open stream: %v", err)
		return
	}
	if err := stream.Send(nodeReq); err != nil {
		log.Printf("Failed to submit document: %v", err)
		return
	}
	stream.CloseSend()
	nodeRes, err := stream.Recv()
	if err != nil {
		log.Printf("Failed to receive stream response: %v", err)
		return
	}
	jobID := "job_test_123"
	fmt.Printf("✅ Document Cached! Job ID: %s, Message: %s\n", jobID, nodeRes.Message)

	fmt.Printf("⏳ [%s] Waiting for Orchestrator Callback...\n", jobID)

	result := <-artemis.ResultChan

	fmt.Printf("\n[%s] PHASE 4: ReAct Decision (Evaluating Output)\n", jobID)
	fmt.Printf("Decision Engine analyzed output: %s\n", result)
	fmt.Println("Decision: Next path selected based on classification. End of demo loop.")
}
