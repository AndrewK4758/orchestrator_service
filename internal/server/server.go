package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pb "doc_processor/shared_protos"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/callback_client"
	"orchestrator_service/internal/engine"
	"orchestrator_service/internal/events"

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
	cbManager *callback_client.ClientManager

	mu         sync.RWMutex
	activeJobs map[string]*JobTracker
}

// NewServer creates a new instance of the OrchestratorServer.
func NewServer(aiClient *ai_client.Client, pub *events.Publisher, cbManager *callback_client.ClientManager) *OrchestratorServer {
	// 1. Initialize the State Machine Engine with Publisher
	eng := engine.NewEngine(aiClient, pub, cbManager)

	srv := &OrchestratorServer{
		eng:        eng,
		aiClient:   aiClient,
		pub:        pub,
		cbManager:  cbManager,
		activeJobs: make(map[string]*JobTracker),
	}

	// Start consuming incoming jobs from NATS JetStream
	err := srv.pub.ConsumeJobs(context.Background(), srv.handleJobMessage)
	if err != nil {
		log.Printf("ERROR: Failed to start job consumer: %v", err)
	}

	return srv
}

// SubmitDocument handles the initial client request to process a PDF.
// It returns a job_id instantly while starting the async workflow.
func (s *OrchestratorServer) SubmitDocument(ctx context.Context, req *pb.SubmitDocumentRequest) (*pb.SubmitDocumentResponse, error) {
	jobID := "job_" + fmt.Sprintf("%d", time.Now().UnixNano())
	fileCount := len(req.GetPdfContents()) + len(req.GetRawTextContents())
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

	var steps []engine.WorkflowStep
	for _, s := range req.GetSteps() {
		steps = append(steps, engine.WorkflowStep{
			StepName:            s.StepName,
			CustomInstructions:  s.CustomInstructions,
			Schema:              s.ExpectedSchema,
			CallbackAddress:     s.CallbackAddress,
			RequireHumanReview:  s.RequireHumanReview,
			BypassHumanFallback: s.BypassHumanFallback,
			TargetActionAddress: s.TargetActionAddress,
		})
	}

	tracker := &JobTracker{
		State: &engine.ProcessingState{
			JobID:           jobID,
			RawPDFs:         req.GetPdfContents(),
			RawTextContents: req.GetRawTextContents(),
			Steps:           steps,
			TenantID:        tenantID,
			UserID:          userID,
			AppID:           appID,
			DocumentType:    req.GetDocumentType(),
			CorrelationID:   correlationID,
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
		JobAction:     "DynamicWorkflow",
		Stage:         "JobIntake",
		Message:       fmt.Sprintf("Received %d documents for processing", fileCount),
	})
	if err != nil {
		log.Printf("ERROR: Failed to publish trace event: %v", err)
	}
	state := tracker.State

	// Serialize state and publish to JetStream
	payload, err := json.Marshal(state)
	if err != nil {
		log.Printf("ERROR: Failed to marshal job state: %v", err)
		return nil, fmt.Errorf("failed to process job request")
	}

	err = s.pub.PublishJob(ctx, payload)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel() // A real app might have a global cancellation context here

	// Run the engine with the dynamically provided steps
	err := s.eng.Run(ctx, tracker.State)
	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			log.Printf("INFO: Job %s paused for human validation: %v", tracker.State.JobID, err)
			sendProgress("PENDING_HUMAN_VALIDATION", "AI output did not fit requirements; pending human review.")
			return
		}
		log.Printf("ERROR: Job %s failed: %v", tracker.State.JobID, err)
		sendProgress("FAILED", fmt.Sprintf("Workflow failed: %v", err))
		return
	}

	// At this point, state.PreviousStepResult contains the output of the final step
	finalJSON := tracker.State.PreviousStepResult
	if finalJSON == "" {
		finalJSON = "{}"
	}

	log.Printf("DEBUG: FinalResultsJson being sent to client: %s", finalJSON)

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

	var tracker *JobTracker
	var exists bool

	// Wait up to 30 seconds for the JetStream consumer to pick up the job and populate activeJobs
	for i := 0; i < 30; i++ {
		s.mu.RLock()
		tracker, exists = s.activeJobs[jobID]
		s.mu.RUnlock()

		if exists {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !exists {
		return fmt.Errorf("job %s not found or already expired after waiting", jobID)
	}

	// Read from the Go channel and push over the gRPC network stream
	for update := range tracker.Updates {
		if err := stream.Send(update); err != nil {
			log.Printf("ERROR: Stream broken to client for job %s: %v", jobID, err)
			return err
		}
	}

	// Clean up completed job to prevent memory leak
	s.mu.Lock()
	delete(s.activeJobs, jobID)
	s.mu.Unlock()

	return nil
}

// ResumeJob handles the gRPC request from HumanValidationService to resume a suspended job.
func (s *OrchestratorServer) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
	jobID := req.GetJobId()
	correctedJSON := req.GetCorrectedJson()
	log.Printf("INFO: [Server.ResumeJob] Resuming JobID: %s with human-corrected JSON", jobID)

	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	var state *engine.ProcessingState

	if exists && tracker != nil {
		state = tracker.State
	} else {
		// If tracker is not found in memory (e.g. server restart), we cannot resume directly
		// in this prototype. However, we log and return failure clearly.
		return &pb.ResumeJobResponse{
			Success: false,
			Message: fmt.Sprintf("Job %s not found in orchestrator memory", jobID),
		}, nil
	}

	// Update the processing state with the human-corrected JSON
	state.Lock()
	if state.StepResults == nil {
		state.StepResults = make(map[int]string)
	}
	state.StepResults[state.CurrentStepIndex] = correctedJSON
	state.PreviousStepResult = correctedJSON

	// Mark current step as completed in executed list
	stepName := state.Steps[state.CurrentStepIndex].StepName
	if stepName == "" {
		stepName = fmt.Sprintf("Step-%d", state.CurrentStepIndex+1)
	}
	state.ExecutedSteps = append(state.ExecutedSteps, stepName)

	// Increment CurrentStepIndex so the engine resumes at the next step!
	state.CurrentStepIndex++
	state.Unlock()

	// Serialize state and publish back to NATS JetStream to trigger execution resume
	payload, err := json.Marshal(state)
	if err != nil {
		log.Printf("ERROR: [Server.ResumeJob] Failed to marshal updated job state: %v", err)
		return nil, fmt.Errorf("failed to serialize job state for resumption")
	}

	err = s.pub.PublishJob(ctx, payload)
	if err != nil {
		log.Printf("ERROR: [Server.ResumeJob] Failed to publish job to queue: %v", err)
		return nil, fmt.Errorf("failed to queue job resumption")
	}

	// Publish dynamic trace event for traceability
	err = s.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType:     events.EventJobStepCompleted,
		Timestamp:     time.Now(),
		TenantID:      state.TenantID,
		UserID:        state.UserID,
		AppID:         state.AppID,
		JobID:         jobID,
		DocumentType:  state.DocumentType,
		CorrelationID: state.CorrelationID,
		JobAction:     stepName,
		Stage:         "HumanValidationResolved",
		Message:       fmt.Sprintf("Step completed via human correction: %s", stepName),
	})
	if err != nil {
		log.Printf("ERROR: [Server.ResumeJob] Failed to publish trace event: %v", err)
	}

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}
