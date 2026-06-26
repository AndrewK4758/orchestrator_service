package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "doc_processor/shared_protos"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/engine"
	"orchestrator_service/internal/pdf"
)

// JobTracker holds the state and update channel for a single job
type JobTracker struct {
	State   *engine.ProcessingState
	Updates chan *pb.ProgressUpdate
}

// OrchestratorServer implements the OrchestratorService gRPC interface.
type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer

	eng      *engine.Engine
	aiClient *ai_client.Client

	mu         sync.RWMutex
	activeJobs map[string]*JobTracker
}

// NewServer creates a new instance of the OrchestratorServer.
func NewServer(aiClient *ai_client.Client) *OrchestratorServer {
	// 1. Initialize the State Machine Engine
	eng := engine.NewEngine()

	// 2. Register Actions (the nodes in our DAG)
	chunker := pdf.NewChunker()
	eng.Register(engine.NewChunkAction(chunker))
	eng.Register(engine.NewParallelExtractAction(aiClient))

	return &OrchestratorServer{
		eng:        eng,
		aiClient:   aiClient,
		activeJobs: make(map[string]*JobTracker),
	}
}

// SubmitDocument handles the initial client request to process a PDF.
// It returns a job_id instantly while starting the async workflow.
func (s *OrchestratorServer) SubmitDocument(ctx context.Context, req *pb.SubmitDocumentRequest) (*pb.SubmitDocumentResponse, error) {
	jobID := "job_" + fmt.Sprintf("%d", time.Now().UnixNano())
	log.Printf("Received document: %s, size: %d bytes, assigned JobID: %s", req.GetFileName(), len(req.GetPdfContent()), jobID)

	tracker := &JobTracker{
		State: &engine.ProcessingState{
			JobID:              jobID,
			RawPDF:             req.GetPdfContent(),
			Action:             req.GetAction(),
			Schema:             req.GetExpectedSchema(),
			CustomInstructions: req.GetCustomInstructions(),
		},
		Updates: make(chan *pb.ProgressUpdate, 100), // Buffer so we don't block the engine
	}

	s.mu.Lock()
	s.activeJobs[jobID] = tracker
	s.mu.Unlock()

	// Launch the async workflow in a background goroutine
	go s.runWorkflowAsync(tracker)

	return &pb.SubmitDocumentResponse{
		JobId:  jobID,
		Status: "QUEUED",
	}, nil
}

// runWorkflowAsync is the background orchestrator thread for a specific job
func (s *OrchestratorServer) runWorkflowAsync(tracker *JobTracker) {
	defer close(tracker.Updates)

	// Helper to push progress to the gRPC stream channel
	sendProgress := func(status, msg string) {
		tracker.Updates <- &pb.ProgressUpdate{
			JobId:   tracker.State.JobID,
			Status:  status,
			Message: msg,
		}
	}

	sendProgress("STARTING", "Initializing workflow")

	ctx := context.Background() // A real app might have a global cancellation context here

	// Define the strict sequential workflow array
	workflow := []string{"ChunkPDF", "ParallelExtract"}

	// Run the engine
	err := s.eng.Run(ctx, workflow, tracker.State)
	if err != nil {
		log.Printf("ERROR: Job %s failed: %v", tracker.State.JobID, err)
		sendProgress("FAILED", fmt.Sprintf("Workflow failed: %v", err))
		return
	}

	// At this point, state.ChunkResults contains the map of extracted JSONs.
	// We'll stitch them together into a JSON array for the client
	var finalJSON string
	if len(tracker.State.ChunkResults) > 0 {
		finalJSON = "["
		for _, result := range tracker.State.ChunkResults {
			if finalJSON != "[" {
				finalJSON += ","
			}
			finalJSON += result
		}
		finalJSON += "]"
	}

	tracker.Updates <- &pb.ProgressUpdate{
		JobId:            tracker.State.JobID,
		Status:           "COMPLETE",
		Message:          "Workflow finished successfully",
		FinalResultsJson: finalJSON,
	}
}

// ListenForProgress streams real-time updates back to the client via gRPC Server Streaming.
func (s *OrchestratorServer) ListenForProgress(req *pb.ListenRequest, stream pb.OrchestratorService_ListenForProgressServer) error {
	jobID := req.GetJobId()
	log.Printf("Client listening for progress on job: %s", jobID)

	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job %s not found or already expired", jobID)
	}

	// Read from the Go channel and push over the gRPC network stream
	for update := range tracker.Updates {
		if err := stream.Send(update); err != nil {
			log.Printf("ERROR: Stream broken to client for job %s: %v", jobID, err)
			return err
		}
	}

	return nil
}
