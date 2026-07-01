package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "doc_processor/shared_protos"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: go run main.go <path_to_pdf_or_directory>")
	}
	inputPath := os.Args[1]

	// Check if it's a file or directory
	info, err := os.Stat(inputPath)
	if err != nil {
		log.Fatalf("Failed to stat input path: %v", err)
	}

	var files []string
	if info.IsDir() {
		entries, err := os.ReadDir(inputPath)
		if err != nil {
			log.Fatalf("Failed to read directory: %v", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".pdf" {
				files = append(files, filepath.Join(inputPath, entry.Name()))
			}
		}
		if len(files) == 0 {
			log.Fatalf("No PDF files found in directory: %s", inputPath)
		}
	} else {
		files = append(files, inputPath)
	}

	fmt.Println("==================================================")
	fmt.Println("🚀 STARTING C#-STYLE WORKFLOW SIMULATION")
	fmt.Println("==================================================")

	// Connect to Orchestrator
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorServiceClient(conn)

	for _, file := range files {
		processFile(client, file)
	}
}

func processFile(client pb.OrchestratorServiceClient, inputPath string) {
	fmt.Println("\n--------------------------------------------------")
	fmt.Printf("📄 PROCESSING FILE: %s\n", inputPath)
	fmt.Println("--------------------------------------------------")

	pdfBytes, err := os.ReadFile(inputPath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", inputPath, err)
		return
	}
	fileName := filepath.Base(inputPath)

	mockJSONSchema := `{
		"type": "object",
		"properties": {
			"OrderNumber": {"type": "string"},
			"BorrowerName": {"type": "string"},
			"DocumentState": {"type": "string", "description": "The state mentioned in the closing attorney's statement"}
		},
		"required": ["BorrowerName", "DocumentState"]
	}`

	fmt.Printf("\n[STEP 1] 🧠 Sending '%s' for EXTRACTION...\n", fileName)

	correlationID := fmt.Sprintf("corr_%d", time.Now().UnixNano())
	req := &pb.SubmitDocumentRequest{
		PdfContents: [][]byte{pdfBytes},
		Steps: []*pb.WorkflowStep{
			{
				StepName:           "Extract Action",
				ExpectedSchema:     mockJSONSchema,
				CustomInstructions: "Extract data from this document according to the requested schema.",
			},
		},
		TenantId:      "LawFirm_StaticTest",
		UserId:        "static.user@test.com",
		AppId:         "StaticTestClient_Go",
		CorrelationId: correlationID,
		DocumentType:  "Unknown",
	}

	res, err := client.SubmitDocument(context.Background(), req)
	if err != nil {
		log.Printf("Failed to submit request: %v", err)
		return
	}

	finalJSON := listenForResults(client, res.GetJobId(), "Pipeline")
	fmt.Printf("\n✅ FINAL PARSED DATA (Ready for C# Persistence): \n%s\n", finalJSON)

	fmt.Println("\n==================================================")
	fmt.Println("🏁 WORKFLOW COMPLETE FOR", fileName)
	fmt.Println("==================================================")
}

func listenForResults(client pb.OrchestratorServiceClient, jobID string, stage string) string {
	fmt.Printf("   -> [%s] Job %s assigned. Waiting for completion...\n", stage, jobID)

	stream, err := client.ListenForProgress(context.Background(), &pb.ListenRequest{JobId: jobID})
	if err != nil {
		log.Fatalf("Failed to open progress stream: %v", err)
	}

	finalResult := ""
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Stream read error: %v", err)
		}

		if update.GetStatus() != "COMPLETE" {
			// Print intermediate steps
			fmt.Printf("      - %s: %s\n", update.GetStatus(), update.GetMessage())
		} else {
			finalResult = update.GetFinalResultsJson()
		}
	}

	// Add a small pause for visual effect
	time.Sleep(500 * time.Millisecond)
	return finalResult
}
