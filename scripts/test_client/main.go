package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
)

type ArtemisCallbackServer struct {
	pb.UnimplementedOrchestratorCallbackServiceServer
	ResultChan chan string
}

func (s *ArtemisCallbackServer) OnTaskExecutionComplete(stream pb.OrchestratorCallbackService_OnTaskExecutionCompleteServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		jobID := ""
		if req.Identity != nil {
			jobID = req.Identity.JobId
		}
		fmt.Printf("\n[Step %s] Received Callback for Job: %s\n", req.StepId, jobID)

		var bytes string
		if req.Payload != nil {
			if b, err := protojson.Marshal(req.Payload); err == nil {
				bytes = string(b)
			}
		}
		fmt.Printf("[%s] Step Succeeded!\nStep Output Payload:\n%s\n", jobID, bytes)
		s.ResultChan <- bytes

		if err := stream.Send(&pb.TaskExecutionCompleteResponse{Success: true}); err != nil {
			return err
		}
	}
}

func (s *ArtemisCallbackServer) OnJobSuspended(ctx context.Context, req *pb.JobSuspendedRequest) (*pb.JobSuspendedResponse, error) {
	fmt.Printf("\n[Step %s] Suspended for Job: %s.\n", req.StepId, req.Identity.JobId)
	s.ResultChan <- "SUSPENDED"
	return &pb.JobSuspendedResponse{Success: true}, nil
}

func (s *ArtemisCallbackServer) OnJobFailed(ctx context.Context, req *pb.JobFailedRequest) (*pb.JobFailedResponse, error) {
	jobID := ""
	if req.Identity != nil {
		jobID = req.Identity.JobId
	}
	fmt.Printf("\n[Step %s] Failed for Job: %s\n", req.StepId, jobID)
	if req.Error != nil {
		fmt.Printf("Error: %s - %s\n", req.Error.Code, req.Error.Message)
	}
	s.ResultChan <- "ERROR"

	return &pb.JobFailedResponse{Success: true}, nil
}

func main() {
	if len(os.Args) < 2 {
		logger.Fatal("Orchestrator", "Usage: go run main.go <path_to_pdf_or_directory>")
	}
	inputPath := os.Args[1]

	lis, err := net.Listen("tcp", ":11115")
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to listen for callbacks: %v", err)
	}
	grpcServer := grpc.NewServer()
	artemisServer := &ArtemisCallbackServer{
		ResultChan: make(chan string, 1),
	}
	pb.RegisterOrchestratorCallbackServiceServer(grpcServer, artemisServer)

	go func() {
		fmt.Println("🎧 Callback Server listening on :11115")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Orchestrator", "Failed to serve callbacks: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	conn, err := grpc.NewClient("localhost:11112", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorServiceClient(conn)

	info, err := os.Stat(inputPath)
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to stat input path: %v", err)
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
		logger.Info("Orchestrator", "Failed to read file %s: %v", inputPath, err)
		return
	}

	fmt.Println("\n[test_client] PHASE 1: Submitting Initial State...")

	b64Doc := base64.StdEncoding.EncodeToString(pdfBytes)
	funcStruct, err := structpb.NewStruct(map[string]interface{}{
		"document_b64": b64Doc,
	})
	if err != nil {
		logger.Fatal("Orchestrator", "Failed to create payload struct: %v", err)
	}

	nodeReq := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job_test_123",
			AppId:    "CLI_TEST_APP",
			TenantId: "TEST_TENANT",
		},
		StepId:          "step_1",
		TaskSubject: "worker.human.validation",
		Payload: funcStruct,
	}

	nodeRes, err := client.ExecuteTask(context.Background(), nodeReq)
	if err != nil {
		logger.Info("Orchestrator", "Failed to execute workflow node: %v", err)
		return
	}
	jobID := "job_test_123"
	fmt.Printf("✅ Task Queued! Job ID: %s, Message: %s\n", jobID, nodeRes.Message)

	fmt.Printf("⏳ [%s] Waiting for Orchestrator Callback...\n", jobID)

	result := <-artemis.ResultChan

	fmt.Printf("\n[%s] PHASE 4: Task Output Evaluation\n", jobID)
	fmt.Printf("Decision Engine analyzed output: %s\n", result)
	fmt.Println("Decision: Next path selected based on classification. End of demo loop.")
}
