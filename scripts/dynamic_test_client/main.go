package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "doc_processor/shared_protos"
)

type Config struct {
	Mappings map[string]Mapping `json:"mappings"`
}

type Mapping struct {
	Schema             string `json:"schema"`
	CustomInstructions string `json:"custom_instructions,omitempty"`
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: go run main.go <path_to_pdf_or_directory> <path_to_config.json>")
	}
	inputPath := os.Args[1]
	configPath := os.Args[2]

	// Load Config
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}
	var config Config
	if err := json.Unmarshal(configBytes, &config); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

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

	// Connect to Orchestrator
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrchestratorServiceClient(conn)

	for _, file := range files {
		processFile(client, file, config)
	}
}

func processFile(client pb.OrchestratorServiceClient, inputPath string, config Config) {
	fmt.Println("\n--------------------------------------------------")
	fmt.Printf("📄 PROCESSING FILE: %s\n", inputPath)
	fmt.Println("--------------------------------------------------")

	pdfBytes, err := os.ReadFile(inputPath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", inputPath, err)
		return
	}
	fileName := filepath.Base(inputPath)

	// STEP 1: CLASSIFY
	fmt.Printf("\n[STEP 1] 🧠 Sending '%s' for CLASSIFICATION...\n", fileName)
	correlationID := fmt.Sprintf("corr_%d", time.Now().UnixNano())
	
	strictClassificationRules := `
Analyze this page and determine the classification type of this document. 
Just choose the most appropriate category based on the content.
`

	// Create a list of allowed classifications from the config map
	var allowedClassifications []string
	for k := range config.Mappings {
		allowedClassifications = append(allowedClassifications, k)
	}

	classifyReq := &pb.SubmitDocumentRequest{
		PdfContents:            [][]byte{pdfBytes},
		Action:                 "CLASSIFY_ONLY",
		CustomInstructions:     strictClassificationRules,
		TenantId:               "LawFirm_SmithAndJones",
		UserId:                 "john.doe@smithjones.com",
		AppId:                  "DynamicTestClient_Go",
		CorrelationId:          correlationID,
		DocumentType:           "Unknown",
		AllowedClassifications: allowedClassifications,
	}

	classifyRes, err := client.SubmitDocument(context.Background(), classifyReq)
	if err != nil {
		log.Printf("Failed to submit classification request: %v", err)
		return
	}
	
	classificationJSON := listenForResults(client, classifyRes.GetJobId(), "Classification")
	
	// We force the AI service to return a strict schema: {"classification_type": "..."}
	// The AI service usually returns an array of objects (one per chunk).
	type ClassifyResult struct {
		ClassificationType string `json:"classification_type"`
	}
	
	var results []ClassifyResult
	var classificationString string
	
	if err := json.Unmarshal([]byte(classificationJSON), &results); err == nil && len(results) > 0 {
		classificationString = results[0].ClassificationType
	} else {
		var singleResult ClassifyResult
		if err := json.Unmarshal([]byte(classificationJSON), &singleResult); err == nil {
			classificationString = singleResult.ClassificationType
		} else {
			// Absolute fallback if JSON parsing entirely fails
			classificationString = strings.TrimSpace(strings.ReplaceAll(classificationJSON, "\"", ""))
		}
	}
	
	fmt.Printf("\n📋 CLASSIFICATION RESULT: '%s'\n", classificationString)

	if strings.ToUpper(classificationString) == "FLAGGED FOR REVIEW" {
		fmt.Println("\n⚠️ Document was explicitly FLAGGED FOR REVIEW by the AI.")
		fmt.Println("   - Aborting parsing step. The document should be sent to a human queue.")
		return
	}

	// STEP 2: LOOKUP IN CONFIG
	fmt.Println("\n[STEP 2] ⚙️ Dynamic Business Logic triggered...")
	var mapping Mapping
	var exists bool
	for k, v := range config.Mappings {
		if strings.EqualFold(k, classificationString) {
			mapping = v
			exists = true
			break
		}
	}

	if !exists {
		fmt.Printf("   - ⚠️ Unknown classification '%s'. Falling back to FLAGGED FOR REVIEW.\n", classificationString)
		fmt.Println("   - Aborting parsing step. The document should be sent to a human queue.")
		return
	}
	
	fmt.Println("   - ✅ Found exact mapping for classification.")

	// STEP 3: PARSE
	fmt.Printf("\n[STEP 3] 🔍 Sending '%s' for FULL PARSING using matched schema...\n", fileName)
	parseReq := &pb.SubmitDocumentRequest{
		PdfContents:        [][]byte{pdfBytes},
		Action:             "PARSE",
		ExpectedSchema:     mapping.Schema,
		CustomInstructions: mapping.CustomInstructions,
		TenantId:           "LawFirm_SmithAndJones",
		UserId:             "john.doe@smithjones.com",
		AppId:              "DynamicTestClient_Go",
		CorrelationId:      correlationID, // Reusing the same correlation ID for tracking
		DocumentType:       classificationString, // Now we know the document type!
	}

	parseRes, err := client.SubmitDocument(context.Background(), parseReq)
	if err != nil {
		log.Printf("Failed to submit parse request: %v", err)
		return
	}
	
	parsedJSON := listenForResults(client, parseRes.GetJobId(), "Parsing")
	fmt.Printf("\n✅ FINAL PARSED DATA:\n%s\n", parsedJSON)
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
			fmt.Printf("      - %s: %s\n", update.GetStatus(), update.GetMessage())
		} else {
			finalResult = update.GetFinalResultsJson()
		}
	}
	
	time.Sleep(500 * time.Millisecond)
	return finalResult
}
