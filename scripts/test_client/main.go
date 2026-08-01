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
		fmt.Printf("\n[Step %s] Received Callback for Job: %s\n", req.StepId, jobID)

		var bytes string
		if req.GlobalState != nil {
			if b, err := protojson.Marshal(req.GlobalState); err == nil {
				bytes = string(b)
			}
		}
		var outBytes string
		if req.NodeOutput != nil {
			if b, err := protojson.Marshal(req.NodeOutput); err == nil {
				outBytes = string(b)
			}
		}
		fmt.Printf("[%s] Step Succeeded!\nStep Output: %s\nGlobal State JSON:\n%s\n", jobID, outBytes, bytes)
		s.ResultChan <- bytes

		if err := stream.Send(&pb.NodeExecutionCompleteResponse{Success: true}); err != nil {
			return err
		}
	}
}

func (s *ArtemisCallbackServer) OnJobSuspended(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*pb.JobSuspendedResponse, error) {
	fmt.Printf("\n[Step %s] Suspended for Job: %s. Reason: %s\n", req.StepId, req.Identity.JobId, req.GetErrorContract().GetMessage())
	s.ResultChan <- "SUSPENDED: " + req.GetErrorContract().GetMessage()
	return &pb.JobSuspendedResponse{Success: true}, nil
}

func (s *ArtemisCallbackServer) OnJobFailed(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*pb.JobFailedResponse, error) {
	jobID := ""
	if req.Identity != nil {
		jobID = req.Identity.JobId
	}
	fmt.Printf("\n[Step %s] Failed for Job: %s\n", req.StepId, jobID)
	fmt.Printf("[%s] Step Failed! Error: %s\n", jobID, req.GetErrorContract().GetMessage())
	s.ResultChan <- "ERROR: " + req.GetErrorContract().GetMessage()

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
		logger.Fatal("Orchestrator", "Failed to create functional struct: %v", err)
	}
	globalState := pb.GlobalState{
		Functional: funcStruct,
	}

	nodeReq := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job_test_123",
			AppId:    "CLI_TEST_APP",
			TenantId: "TEST_TENANT",
		},
		GraphId: "00000000-0000-0000-0000-000000000001", InitialNodeId: "00000000-0000-0000-0000-000000000002",
		StepId:          "step_1",
		GlobalState:     &globalState,
		Metadata:        map[string]string{},
		CallbackAddress: "localhost:11115",
		NodeConfig: &pb.NodeConfiguration{
			TargetWorkerSubject: "worker.human.validation",
			NodeName:            "Generic Extraction Worker",
			SystemInstructions:  "Extract required target fields.",
			ExpectedSchemaStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"type": structpb.NewStringValue("object"),
					"properties": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"field_1": structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"type": structpb.NewStringValue("string"),
								},
							}),
							"total": structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"type": structpb.NewStringValue("number"),
								},
							}),
						},
					}),
				},
			},
		},
	}

	nodeRes, err := client.ExecuteWorkflowNode(context.Background(), nodeReq)
	if err != nil {
		logger.Info("Orchestrator", "Failed to execute workflow node: %v", err)
		return
	}
	jobID := "job_test_123"
	fmt.Printf("✅ Document Cached! Job ID: %s, Message: %s\n", jobID, nodeRes.Message)

	fmt.Printf("⏳ [%s] Waiting for Orchestrator Callback...\n", jobID)

	result := <-artemis.ResultChan

	fmt.Printf("\n[%s] PHASE 4: Step Output Evaluation\n", jobID)
	fmt.Printf("Decision Engine analyzed output: %s\n", result)
	fmt.Println("Decision: Next path selected based on classification. End of demo loop.")
}
