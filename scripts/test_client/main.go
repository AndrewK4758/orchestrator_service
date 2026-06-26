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

	// ---------------------------------------------------------
	// STEP 1: CLASSIFY
	// ---------------------------------------------------------
	fmt.Printf("\n[STEP 1] 🧠 Sending '%s' for CLASSIFICATION...\n", fileName)

	classifyReq := &pb.SubmitDocumentRequest{
		PdfContent: pdfBytes,
		FileName:   fileName,
		Action:     "CLASSIFY", // Tells Orchestrator to only run a classification workflow
	}

	classifyRes, err := client.SubmitDocument(context.Background(), classifyReq)
	if err != nil {
		log.Printf("Failed to submit classification request: %v", err)
		return
	}

	classificationJSON := listenForResults(client, classifyRes.GetJobId(), "Classification")
	fmt.Printf("\n📋 CLASSIFICATION RESULT: \n%s\n", classificationJSON)

	// ---------------------------------------------------------
	// STEP 2: MOCK C# BUSINESS LOGIC (SWITCH STATEMENT)
	// ---------------------------------------------------------
	fmt.Println("\n[STEP 2] ⚙️ C# Client Business Logic triggered...")
	fmt.Println("   - Switch statement activated based on classification")
	fmt.Println("   - Database queried for required schema fields")

	// We'll assume the LLM classified it as an "Attorney Closing Statement" for this test file
	mockJSONSchema := `{
		"type": "object",
		"properties": {
			"OrderNumber": {"type": "string"},
			"BorrowerName": {"type": "string"},
			"StreetNumber": {"type": "string"},
			"StreetName": {"type": "string"},
			"StreetSuffix": {"type": "string"},
			"City": {"type": "string"},
			"State": {"type": "string"},
			"ZipCode": {"type": "string"},
			"County": {"type": ["string", "null"]},
			"LoanNumber": {"type": "string"},
			"AttorneyName": {"type": "string"},
			"DocumentState": {"type": "string", "description": "The state mentioned in the closing attorney's statement, e.g., NEW YORK"},
			"RocketCloseIdentifier": {"type": "string"},
			"SenderId": {"type": "string"}
		},
		"required": ["BorrowerName", "DocumentState"]
	}`

	fmt.Printf("   - Selected Schema for Parsing: \n%s\n", mockJSONSchema)

	// ---------------------------------------------------------
	// STEP 3: PARSE
	// ---------------------------------------------------------
	fmt.Printf("\n[STEP 3] 🔍 Sending '%s' for FULL PARSING...\n", fileName)

	parseReq := &pb.SubmitDocumentRequest{
		PdfContent:     pdfBytes,
		FileName:       fileName,
		Action:         "PARSE",
		ExpectedSchema: mockJSONSchema,
	}

	parseRes, err := client.SubmitDocument(context.Background(), parseReq)
	if err != nil {
		log.Printf("Failed to submit parse request: %v", err)
		return
	}

	parsedJSON := listenForResults(client, parseRes.GetJobId(), "Parsing")
	fmt.Printf("\n✅ FINAL PARSED DATA (Ready for C# Persistence): \n%s\n", parsedJSON)

	fmt.Println("\n==================================================")
	fmt.Println("🏁 WORKFLOW COMPLETE FOR", fileName)
	fmt.Println("   - C# Client would now save this to the DB")
	fmt.Println("   - C# Client would trigger automated emails")
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
