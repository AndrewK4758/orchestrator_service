package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
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

	// 1. Determine if input is a single file or a directory
	info, err := os.Stat(inputPath)
	if err != nil {
		log.Fatalf("Failed to stat input path: %v", err)
	}

	var filesToProcess []string
	if info.IsDir() {
		entries, err := os.ReadDir(inputPath)
		if err != nil {
			log.Fatalf("Failed to read directory: %v", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".pdf" {
				filesToProcess = append(filesToProcess, filepath.Join(inputPath, entry.Name()))
			}
		}
	} else {
		filesToProcess = append(filesToProcess, inputPath)
	}

	if len(filesToProcess) == 0 {
		log.Fatal("No PDF files found to process")
	}

	fmt.Printf("Found %d PDF file(s) to process.\n", len(filesToProcess))

	// 2. Connect to the new Orchestrator Service
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to orchestrator: %v", err)
	}
	defer conn.Close()

	client := pb.NewOrchestratorServiceClient(conn)

	// We will process them sequentially in this test client to avoid console output overlapping too much,
	// but the Orchestrator itself processes them asynchronously.
	
	// If you want to blast them all at once, you could wrap this loop in goroutines like the AI Service test client did.
	// For demonstration, let's use a WaitGroup to spawn them concurrently!
	
	var wg sync.WaitGroup
	startTime := time.Now()

	for _, filePath := range filesToProcess {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			processSinglePDF(client, path)
		}(filePath)
	}

	wg.Wait()
	fmt.Printf("\nAll %d files successfully processed through the Orchestrator in %v!\n", len(filesToProcess), time.Since(startTime))
}

func processSinglePDF(client pb.OrchestratorServiceClient, filePath string) {
	// Read the PDF
	pdfBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", filePath, err)
		return
	}
	
	fileName := filepath.Base(filePath)

	// 3. Submit the Document
	fmt.Printf("[%s] Submitting document to Orchestrator...\n", fileName)
	submitReq := &pb.SubmitDocumentRequest{
		PdfContent: pdfBytes,
		FileName:   fileName,
	}

	submitRes, err := client.SubmitDocument(context.Background(), submitReq)
	if err != nil {
		log.Printf("[%s] Failed to submit document: %v", fileName, err)
		return
	}

	jobID := submitRes.GetJobId()
	fmt.Printf("[%s] Success! Received Async JobID: %s. Opening gRPC Stream to listen for live progress...\n", fileName, jobID)

	// 4. Open the Server Stream to listen to real-time progress
	listenReq := &pb.ListenRequest{JobId: jobID}
	stream, err := client.ListenForProgress(context.Background(), listenReq)
	if err != nil {
		log.Printf("[%s] Failed to open progress stream: %v", fileName, err)
		return
	}

	// 5. Consume the stream
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			// The server closed the stream (job complete or failed)
			break
		}
		if err != nil {
			log.Printf("[%s] Stream read error: %v", fileName, err)
			break
		}

		fmt.Printf("[%s | %s] %s\n", fileName, update.GetStatus(), update.GetMessage())
		
		if update.GetStatus() == "COMPLETE" {
			fmt.Printf("\n--- FINAL AI RESULTS FOR %s ---\n", fileName)
			fmt.Println(update.GetFinalResultsJson())
			fmt.Println("--------------------------------------------------")
		}
	}
}
