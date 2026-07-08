package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/engine"
	"orchestrator_service/internal/events"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// JobTracker holds the state for a single job
type JobTracker struct {
	State   *pb.WorkflowContext
	StepReq *pb.SubmitStepRequest
	mu      sync.Mutex
}

func (j *JobTracker) Lock() {
	j.mu.Lock()
}

func (j *JobTracker) Unlock() {
	j.mu.Unlock()
}

// OrchestratorServer implements the OrchestratorService gRPC interface.
type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer

	eng      *engine.Engine
	aiClient *ai_client.Client
	pub      *events.Publisher

	mu         sync.RWMutex
	activeJobs map[string]*JobTracker
}

func NewServer(aiClient *ai_client.Client, pub *events.Publisher) *OrchestratorServer {
	eng := engine.NewEngine(aiClient, pub)

	srv := &OrchestratorServer{
		eng:        eng,
		aiClient:   aiClient,
		pub:        pub,
		activeJobs: make(map[string]*JobTracker),
	}

	err := srv.pub.ConsumeJobs(context.Background(), srv.handleJobMessage)
	if err != nil {
		log.Printf("ERROR: Failed to start job consumer: %v", err)
	}

	return srv
}

func (s *OrchestratorServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	workflowCtx := req.Context
	if workflowCtx == nil {
		workflowCtx = &pb.WorkflowContext{}
	}

	jobID := workflowCtx.JobId
	if jobID == "" {
		appID := "app"
		if workflowCtx.Metadata != nil && workflowCtx.Metadata["app_id"] != "" {
			appID = workflowCtx.Metadata["app_id"]
		}
		jobID = fmt.Sprintf("%s_%d", appID, time.Now().UnixNano())
		workflowCtx.JobId = jobID
	}

	if workflowCtx.Metadata == nil {
		workflowCtx.Metadata = make(map[string]string)
	}
	tenantID := workflowCtx.Metadata["tenant_id"]
	if tenantID == "" {
		tenantID = "SYSTEM_DEFAULT_TENANT"
		workflowCtx.Metadata["tenant_id"] = tenantID
	}

	log.Printf("Received job, using JobID: %s for Tenant: %s", jobID, tenantID)

	tracker := &JobTracker{
		State: workflowCtx,
	}

	s.mu.Lock()
	s.activeJobs[jobID] = tracker
	s.mu.Unlock()

	return &pb.SubmitJobResponse{
		JobId:  jobID,
		Status: "CACHED",
	}, nil
}

func (s *OrchestratorServer) SubmitStep(ctx context.Context, req *pb.SubmitStepRequest) (*pb.SubmitStepResponse, error) {
	workflowCtx := req.Context
	if workflowCtx == nil {
		return nil, fmt.Errorf("context is required in SubmitStepRequest")
	}

	jobID := workflowCtx.JobId

	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found in cache", jobID)
	}

	log.Printf("Received SubmitStep for JobID: %s, Steps count: %d", jobID, len(req.GetSteps()))

	stepID := "step_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Lock the tracker and update active execution
	tracker.Lock()
	tracker.StepReq = req // We store the current step request in the tracker
	if tracker.State.ActiveExecution == nil {
		tracker.State.ActiveExecution = &pb.ActiveJobExecution{JobId: jobID}
	}
	tracker.State.ActiveExecution.CurrentStepId = stepID

	// Reset step index and action index for a fresh request
	tracker.State.CurrentStepIndex = 0
	tracker.State.CurrentActionIndex = 0
	tracker.State.PendingActions = nil
	tracker.Unlock()

	// Serialize the wrapper struct that will be passed into the job stream
	type Payload struct {
		StateBytes   []byte `json:"state_bytes"`
		StepReqBytes []byte `json:"step_req_bytes"`
	}

	stateBytes, err := proto.Marshal(tracker.State)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize state: %v", err)
	}
	stepReqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize step request: %v", err)
	}

	p := Payload{
		StateBytes:   stateBytes,
		StepReqBytes: stepReqBytes,
	}
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize payload: %v", err)
	}

	err = s.pub.PublishJob(ctx, payloadBytes)
	if err != nil {
		log.Printf("ERROR: Failed to publish job to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue step")
	}

	return &pb.SubmitStepResponse{
		Success: true,
		Message: "Step queued for execution",
		StepId:  stepID,
	}, nil
}

// handleJobMessage is invoked by the JetStream consumer for each step queued
func (s *OrchestratorServer) handleJobMessage(ctx context.Context, msg jetstream.Msg) error {
	type Payload struct {
		StateBytes   []byte `json:"state_bytes"`
		StepReqBytes []byte `json:"step_req_bytes"`
	}

	var p Payload
	if err := json.Unmarshal(msg.Data(), &p); err != nil {
		return fmt.Errorf("failed to unmarshal job payload: %w", err)
	}

	state := &pb.WorkflowContext{}
	if err := proto.Unmarshal(p.StateBytes, state); err != nil {
		return fmt.Errorf("failed to unmarshal WorkflowContext: %w", err)
	}

	req := &pb.SubmitStepRequest{}
	if err := proto.Unmarshal(p.StepReqBytes, req); err != nil {
		return fmt.Errorf("failed to unmarshal SubmitStepRequest: %w", err)
	}

	log.Printf("Executing workflow of %d steps for job %s", len(req.Steps), state.JobId)

	resultJSON, lastStepName, lastStepId, err := s.eng.ExecuteWorkflow(ctx, state, req)

	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			log.Printf("INFO: Job %s paused for human validation", state.JobId)
			// Update the cache with the pending state
			s.mu.RLock()
			tracker, exists := s.activeJobs[state.JobId]
			s.mu.RUnlock()
			if exists {
				tracker.Lock()
				tracker.State = state
				tracker.Unlock()
			}
			return nil
		}
		log.Printf("ERROR: Workflow execution failed: %v", err)
		s.fireCallback(state, req, "", err.Error(), lastStepId, lastStepName)
		return nil
	}

	log.Printf("Workflow completed successfully for job %s. Firing callback to %s", state.JobId, req.CallbackAddress)
	s.fireCallback(state, req, resultJSON, "", lastStepId, lastStepName)

	return nil
}

func (s *OrchestratorServer) fireCallback(state *pb.WorkflowContext, req *pb.SubmitStepRequest, resultJSON string, errMsg string, lastStepId string, lastStepName string) {
	if req.CallbackAddress == "" {
		log.Printf("No callback address provided, skipping callback")
		if req.CompleteJob {
			s.mu.Lock()
			delete(s.activeJobs, state.JobId)
			s.mu.Unlock()
			log.Printf("INFO: Job %s complete. Teared down cache.", state.JobId)
		}
		return
	}

	conn, err := grpc.NewClient(req.CallbackAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("ERROR: Failed to dial callback address %s: %v", req.CallbackAddress, err)
		if req.CompleteJob {
			s.mu.Lock()
			delete(s.activeJobs, state.JobId)
			s.mu.Unlock()
			log.Printf("INFO: Job %s complete. Teared down cache.", state.JobId)
		}
		return
	}
	defer conn.Close()

	client := pb.NewStepCallbackServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.OnStepComplete(ctx, &pb.StepCompleteRequest{
		JobId:        state.JobId,
		StepId:       lastStepId,
		ResultJson:   resultJSON,
		ErrorMessage: errMsg,
		StepName:     lastStepName,
	})

	if err != nil {
		log.Printf("ERROR: Callback failed: %v", err)
	}

	if req.CompleteJob {
		s.mu.Lock()
		delete(s.activeJobs, state.JobId)
		s.mu.Unlock()
		log.Printf("INFO: Job %s complete. Teared down cache.", state.JobId)
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

	tracker.Lock()
	stepReq := tracker.StepReq
	if stepReq == nil || int(tracker.State.CurrentStepIndex) >= len(stepReq.Steps) {
		tracker.Unlock()
		return &pb.ResumeJobResponse{Success: false, Message: "No active step to resume"}, nil
	}

	currentStep := stepReq.Steps[tracker.State.CurrentStepIndex]

	if currentStep.WritesToStateKey != "" && req.GetCorrectedJson() != "" {
		var parsed interface{}
		if err := json.Unmarshal([]byte(req.GetCorrectedJson()), &parsed); err == nil {
			if tracker.State.GlobalState == nil {
				tracker.State.GlobalState, _ = structpb.NewStruct(make(map[string]interface{}))
			}
			m := tracker.State.GlobalState.AsMap()
			m[currentStep.WritesToStateKey] = parsed
			newState, _ := structpb.NewStruct(m)
			tracker.State.GlobalState = newState
		}
	}

	tracker.State.CurrentActionIndex++
	tracker.Unlock()

	log.Printf("INFO: Resuming job %s after human correction", jobID)

	go func() {
		tracker.Lock()
		stateCopy := proto.Clone(tracker.State).(*pb.WorkflowContext)
		reqCopy := proto.Clone(stepReq).(*pb.SubmitStepRequest)
		tracker.Unlock()

		resultJSON, lastStepName, lastStepId, err := s.eng.ExecuteWorkflow(context.Background(), stateCopy, reqCopy)
		if err != nil {
			if errors.Is(err, engine.ErrPendingHumanValidation) {
				log.Printf("INFO: Job %s paused again for human validation", stateCopy.JobId)
				tracker.Lock()
				tracker.State = stateCopy
				tracker.Unlock()
				return
			}
			log.Printf("ERROR: Workflow execution failed after resume: %v", err)
			s.fireCallback(stateCopy, reqCopy, "", err.Error(), lastStepId, lastStepName)
			return
		}
		log.Printf("Workflow completed successfully for job %s after resume. Firing callback.", stateCopy.JobId)
		s.fireCallback(stateCopy, reqCopy, resultJSON, "", lastStepId, lastStepName)
	}()

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}
