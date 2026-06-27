package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pb "doc_processor/shared_protos"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/engine"
	"orchestrator_service/internal/events"
	"orchestrator_service/internal/pdf"
	"orchestrator_service/internal/registry_client"

	"github.com/nats-io/nats.go/jetstream"
)

// JobTracker holds the state and update channel for a single job
type JobTracker struct {
	State   *engine.ProcessingState
	Updates chan *pb.ProgressUpdate
}

// OrchestratorServer implements the OrchestratorService gRPC interface.
type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer

	eng       *engine.Engine
	aiClient  *ai_client.Client
	pub       *events.Publisher
	regClient *registry_client.Client

	mu         sync.RWMutex
	activeJobs map[string]*JobTracker
}

// NewServer creates a new instance of the OrchestratorServer.
func NewServer(aiClient *ai_client.Client, pub *events.Publisher, regClient *registry_client.Client) *OrchestratorServer {
	// 1. Initialize the State Machine Engine with Publisher
	eng := engine.NewEngine(pub)

	// 2. Register Actions (the nodes in our DAG)
	chunker := pdf.NewChunker()
	eng.Register(engine.NewChunkAction(chunker))
	eng.Register(engine.NewParallelExtractAction(aiClient, pub))
	eng.Register(engine.NewClassifyAction(aiClient, regClient, pub))

	srv := &OrchestratorServer{
		eng:        eng,
		aiClient:   aiClient,
		pub:        pub,
		regClient:  regClient,
		activeJobs: make(map[string]*JobTracker),
	}

	// Start consuming incoming jobs from NATS JetStream
	ctx := context.Background()
	err := srv.pub.ConsumeJobs(ctx, srv.handleJobMessage)
	if err != nil {
		log.Printf("ERROR: Failed to start job consumer: %v", err)
	}

	return srv
}

// SubmitDocument handles the initial client request to process a PDF.
// It returns a job_id instantly while starting the async workflow.
func (s *OrchestratorServer) SubmitDocument(ctx context.Context, req *pb.SubmitDocumentRequest) (*pb.SubmitDocumentResponse, error) {
	jobID := "job_" + fmt.Sprintf("%d", time.Now().UnixNano())
	fileCount := len(req.GetPdfContents())
	log.Printf("Received %d documents, assigned JobID: %s for Tenant: %s", fileCount, jobID, req.GetTenantId())

	tenantID := req.GetTenantId()
	if tenantID == "" {
		tenantID = "SYSTEM_DEFAULT_TENANT"
	}
	userID := req.GetUserId()
	if userID == "" {
		userID = "SYSTEM_USER"
	}
	appID := req.GetAppId()
	if appID == "" {
		appID = "DocProcessorSystem"
	}
	correlationID := req.GetCorrelationId()
	if correlationID == "" {
		correlationID = jobID // Default to job ID to group it by itself
	}

	tracker := &JobTracker{
		State: &engine.ProcessingState{
			JobID:                  jobID,
			RawPDFs:                req.GetPdfContents(),
			RawTextContents:        req.GetRawTextContents(),
			Action:                 req.GetAction(),
			Schema:                 req.GetExpectedSchema(),
			CustomInstructions:     req.GetCustomInstructions(),
			AllowedClassifications: req.GetAllowedClassifications(),
			TenantID:               tenantID,
			UserID:                 userID,
			AppID:                  appID,
			DocumentType:           req.GetDocumentType(),
			CorrelationID:          correlationID,
		},
		Updates: make(chan *pb.ProgressUpdate, 100), // Buffer so we don't block the engine
	}

	// Publish Job Started Trace Event
	err := s.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType:     events.EventJobStarted,
		Timestamp:     time.Now(),
		TenantID:      tracker.State.TenantID,
		UserID:        tracker.State.UserID,
		AppID:         tracker.State.AppID,
		JobID:         jobID,
		DocumentType:  tracker.State.DocumentType,
		CorrelationID: tracker.State.CorrelationID,
		Message:       fmt.Sprintf("Received %d documents for processing", fileCount),
	})
	if err != nil {
		log.Printf("ERROR: Failed to publish trace event: %v", err)
	}
	state := &engine.ProcessingState{
		JobID:                  jobID,
		RawPDFs:                req.GetPdfContents(),
		RawTextContents:        req.GetRawTextContents(),
		Action:                 req.GetAction(),
		Schema:                 req.GetExpectedSchema(),
		CustomInstructions:     req.GetCustomInstructions(),
		AllowedClassifications: req.GetAllowedClassifications(),
		TenantID:               tenantID,
		UserID:                 userID,
		AppID:                  appID,
		DocumentType:           req.GetDocumentType(),
		CorrelationID:          correlationID,
	}

	// Serialize state and publish to JetStream
	payload, err := json.Marshal(state)
	if err != nil {
		log.Printf("ERROR: Failed to marshal job state: %v", err)
		return nil, fmt.Errorf("failed to process job request")
	}

	err = s.pub.PublishJob(ctx, jobID, payload)
	if err != nil {
		log.Printf("ERROR: Failed to publish job to queue: %v", err)
		return nil, fmt.Errorf("failed to queue job")
	}

	return &pb.SubmitDocumentResponse{
		JobId:  jobID,
		Status: "QUEUED",
	}, nil
}

// handleJobMessage is invoked by the JetStream consumer for each new job
func (s *OrchestratorServer) handleJobMessage(ctx context.Context, msg jetstream.Msg) error {
	var state engine.ProcessingState
	if err := json.Unmarshal(msg.Data(), &state); err != nil {
		return fmt.Errorf("failed to unmarshal job state: %w", err)
	}

	tracker := &JobTracker{
		State:   &state,
		Updates: make(chan *pb.ProgressUpdate, 100),
	}

	s.mu.Lock()
	s.activeJobs[state.JobID] = tracker
	s.mu.Unlock()

	s.runWorkflowSync(tracker)

	return nil
}

// runWorkflowSync executes the orchestrator workflow for a specific job inline.
func (s *OrchestratorServer) runWorkflowSync(tracker *JobTracker) {
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

	// Define the strict sequential workflow array based on Action
	var workflow []string
	if tracker.State.Action == "CLASSIFY" {
		workflow = []string{"Classify", "ChunkPDF", "ParallelExtract"}
	} else {
		workflow = []string{"ChunkPDF", "ParallelExtract"}
	}

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
