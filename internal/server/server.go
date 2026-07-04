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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// JobTracker holds the state for a single job
type JobTracker struct {
	State *engine.ProcessingState
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

func NewServer(aiClient *ai_client.Client, pub *events.Publisher, cbManager *callback_client.ClientManager) *OrchestratorServer {
	eng := engine.NewEngine(aiClient, pub, cbManager)

	srv := &OrchestratorServer{
		eng:        eng,
		aiClient:   aiClient,
		pub:        pub,
		cbManager:  cbManager,
		activeJobs: make(map[string]*JobTracker),
	}

	err := srv.pub.ConsumeJobs(context.Background(), srv.handleJobMessage)
	if err != nil {
		log.Printf("ERROR: Failed to start job consumer: %v", err)
	}

	return srv
}

func (s *OrchestratorServer) SubmitDocument(ctx context.Context, req *pb.SubmitDocumentRequest) (*pb.SubmitDocumentResponse, error) {
	jobID := "job_" + fmt.Sprintf("%d", time.Now().UnixNano())
	fileCount := len(req.GetPdfContents()) + len(req.GetRawTextContents())
	log.Printf("Received %d documents, assigned JobID: %s for Tenant: %s", fileCount, jobID, req.GetTenantId())

	tenantID := req.GetTenantId()
	if tenantID == "" {
		tenantID = "SYSTEM_DEFAULT_TENANT"
	}

	tracker := &JobTracker{
		State: &engine.ProcessingState{
			JobID:           jobID,
			RawPDFs:         req.GetPdfContents(),
			RawTextContents: req.GetRawTextContents(),
			GlobalState:     make(map[string]interface{}),
			TenantID:        tenantID,
			UserID:          req.GetUserId(),
			AppID:           req.GetAppId(),
			DocumentType:    req.GetDocumentType(),
			CorrelationID:   req.GetCorrelationId(),
		},
	}

	s.mu.Lock()
	s.activeJobs[jobID] = tracker
	s.mu.Unlock()

	return &pb.SubmitDocumentResponse{
		JobId:  jobID,
		Status: "CACHED",
	}, nil
}

func (s *OrchestratorServer) SubmitStep(ctx context.Context, req *pb.SubmitStepRequest) (*pb.SubmitStepResponse, error) {
	jobID := req.GetJobId()
	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found in cache", jobID)
	}

	stepID := "step_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Create a Job Payload to queue for processing
	tracker.State.Lock()
	tracker.State.CurrentStepReq = req // We store the current step request in the state
	tracker.State.CurrentStepID = stepID
	tracker.State.Unlock()

	payload, err := json.Marshal(tracker.State)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize state")
	}

	err = s.pub.PublishJob(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to queue step")
	}

	return &pb.SubmitStepResponse{
		Success: true,
		Message: "Step queued for execution",
		StepId:  stepID,
	}, nil
}

// handleJobMessage is invoked by the JetStream consumer for each step queued
// handleJobMessage is invoked by the JetStream consumer for each step queued
func (s *OrchestratorServer) handleJobMessage(ctx context.Context, msg jetstream.Msg) error {
	var state engine.ProcessingState
	if err := json.Unmarshal(msg.Data(), &state); err != nil {
		return fmt.Errorf("failed to unmarshal job state: %w", err)
	}

	req := state.CurrentStepReq
	if req == nil {
		log.Printf("ERROR: Job state contains no step request")
		return nil
	}

	if req.InitialStateJson != "" {
		var initial map[string]interface{}
		if err := json.Unmarshal([]byte(req.InitialStateJson), &initial); err == nil {
			if state.GlobalState == nil {
				state.GlobalState = make(map[string]interface{})
			}
			for k, v := range initial {
				state.GlobalState[k] = v
			}
		}
	}

	log.Printf("Executing workflow of %d steps for job %s", len(req.Steps), state.JobID)

	resultJSON, lastActionType, lastStepName, lastStepId, err := s.eng.ExecuteWorkflow(ctx, &state, req)

	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			log.Printf("INFO: Job %s paused for human validation", state.JobID)
			return nil
		}
		log.Printf("ERROR: Workflow execution failed: %v", err)
		s.fireCallback(&state, req, "", err.Error(), lastStepId, lastActionType, lastStepName)
		return nil
	}

	log.Printf("Workflow completed successfully for job %s. Firing callback to %s", state.JobID, req.CallbackAddress)
	s.fireCallback(&state, req, resultJSON, "", lastStepId, lastActionType, lastStepName)

	return nil
}

func (s *OrchestratorServer) fireCallback(state *engine.ProcessingState, req *pb.SubmitStepRequest, resultJSON string, errMsg string, lastStepId string, lastActionType string, lastStepName string) {
	if req.CallbackAddress == "" {
		log.Printf("No callback address provided, skipping callback")
		if req.CompleteJob {
			s.mu.Lock()
			delete(s.activeJobs, state.JobID)
			s.mu.Unlock()
			log.Printf("INFO: Job %s complete. Teared down cache.", state.JobID)
		}
		return
	}

	conn, err := grpc.NewClient(req.CallbackAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("ERROR: Failed to dial callback address %s: %v", req.CallbackAddress, err)
		if req.CompleteJob {
			s.mu.Lock()
			delete(s.activeJobs, state.JobID)
			s.mu.Unlock()
			log.Printf("INFO: Job %s complete. Teared down cache.", state.JobID)
		}
		return
	}
	defer conn.Close()

	client := pb.NewStepCallbackServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.OnStepComplete(ctx, &pb.StepCompleteRequest{
		JobId:        state.JobID,
		StepId:       lastStepId,
		ActionType:   lastActionType,
		ResultJson:   resultJSON,
		ErrorMessage: errMsg,
		StepName:     lastStepName,
	})

	if err != nil {
		log.Printf("ERROR: Callback failed: %v", err)
	}

	if req.CompleteJob {
		s.mu.Lock()
		delete(s.activeJobs, state.JobID)
		s.mu.Unlock()
		log.Printf("INFO: Job %s complete. Teared down cache.", state.JobID)
	}
}

func (s *OrchestratorServer) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
	jobID := req.GetJobId()
	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return &pb.ResumeJobResponse{Success: false, Message: "Job not found"}, nil
	}

	tracker.State.Lock()
	stepReq := tracker.State.CurrentStepReq
	tracker.State.Unlock()

	log.Printf("INFO: Resuming job %s after human correction", jobID)

	// Since the human corrected the JSON, we consider the step complete
	// and fire the callback to the client with the corrected JSON.
	s.fireCallback(tracker.State, stepReq, req.GetCorrectedJson(), "", "", "HUMAN_VALIDATION", "")

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}
