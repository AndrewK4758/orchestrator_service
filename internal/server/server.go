package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/cache_client"
	"orchestrator_service/internal/engine"

	pb "github.com/AndrewK4758/shared_protos"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type JobTracker struct {
	State           *pb.WorkflowContext
	FullSteps       []*pb.StepDefinition
	JobGraph        []*pb.JobWorkflowEdge
	StepReq         *pb.SubmitStepRequest
	CallbackAddress string
	Cancel          context.CancelFunc
	mu              sync.Mutex
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
	pub      JobPublisher

	mu         sync.RWMutex
	activeJobs map[string]*JobTracker

	traceClient pb.TraceabilityQueryClient
	cacheClient pb.SemanticCacheServiceClient
}

type JobPublisher interface {
	engine.TracePublisher
	PublishJob(ctx context.Context, payload []byte) error
	PublishPriorityJob(ctx context.Context, payload []byte) error
	ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error
	PurgeQueues(ctx context.Context) error
}

func NewServer(aiClient *ai_client.Client, pub JobPublisher, traceClient pb.TraceabilityQueryClient) *OrchestratorServer {
	cacheClient, _, err := cache_client.NewClient(os.Getenv("SEMANTIC_CACHE_SERVICE_ADDRESS"))
	if err != nil {
		log.Printf("Warning: failed to connect to cache service, cache will be disabled: %v", err)
	}

	eng := engine.NewEngine(aiClient, cacheClient, pub)

	srv := &OrchestratorServer{
		eng:         eng,
		aiClient:    aiClient,
		pub:         pub,
		activeJobs:  make(map[string]*JobTracker),
		traceClient: traceClient,
		cacheClient: cacheClient,
	}

	// Load pending jobs from Traceability Service
	if traceClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := traceClient.GetPendingJobs(ctx, &pb.GetPendingJobsRequest{})
		if err != nil {
			log.Printf("WARNING: Failed to load pending jobs from Traceability: %v", err)
		} else {
			loadedCount := 0
			for _, jobRecord := range resp.Jobs {
				if jobRecord.OriginalRequest != nil {
					srv.activeJobs[jobRecord.JobId] = &JobTracker{
						State:           jobRecord.OriginalRequest.Context,
						FullSteps:       jobRecord.OriginalRequest.Steps,
						JobGraph:        jobRecord.OriginalRequest.JobGraph,
						CallbackAddress: jobRecord.OriginalRequest.CallbackAddress,
					}
					loadedCount++
				}
			}
			log.Printf("Successfully loaded %d pending jobs from Traceability.", loadedCount)
		}
	}

	err = srv.pub.ConsumeJobs(context.Background(), srv.handleJobMessage)
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
		State:           workflowCtx,
		FullSteps:       req.Steps,
		JobGraph:        req.JobGraph,
		CallbackAddress: req.GetCallbackAddress(),
	}

	s.mu.Lock()
	s.activeJobs[jobID] = tracker
	s.mu.Unlock()

	// Mirror State to Traceability
	if s.traceClient != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := s.traceClient.SaveJobState(ctx, &pb.SaveJobStateRequest{
				JobId:           jobID,
				OriginalRequest: req,
				Status:          "PENDING",
			})
			if err != nil {
				log.Printf("ERROR: Failed to save job state to Traceability for JobID %s: %v", jobID, err)
			} else {
				log.Printf("Saved job state to Traceability for JobID %s", jobID)
			}
		}()
	}

	return &pb.SubmitJobResponse{
		JobId:  jobID,
		Status: "CACHED",
	}, nil
}

func (s *OrchestratorServer) StartJob(ctx context.Context, req *pb.StartJobRequest) (*pb.StartJobResponse, error) {
	jobID := req.GetJobId()
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required")
	}

	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found in cache", jobID)
	}

	var stepID string
	if tracker.State != nil && tracker.State.JobDefinition != nil && tracker.State.JobDefinition.InitialStepId != "" {
		stepID = tracker.State.JobDefinition.InitialStepId
	} else if len(tracker.FullSteps) > 0 {
		stepID = tracker.FullSteps[0].Id
	} else {
		stepID = "step_" + fmt.Sprintf("%d", time.Now().UnixNano())
	}

	log.Printf("Received StartJob for JobID: %s, Initial StepID: %s", jobID, stepID)

	stepReq := &pb.SubmitStepRequest{
		JobId:       jobID,
		StepId:      stepID,
		CompleteJob: false,
	}

	payloadBytes, err := s.updateExecutionAndMarshal(tracker, stepReq)
	if err != nil {
		return nil, err
	}

	log.Printf("DEBUG: Publishing payload of size %d to NATS...", len(payloadBytes))
	err = s.pub.PublishJob(ctx, payloadBytes)
	if err != nil {
		log.Printf("ERROR: Failed to publish job to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue job start: %w", err)
	}
	log.Printf("DEBUG: Successfully published JobID %s to JetStream!", jobID)

	return &pb.StartJobResponse{
		Success:      true,
		Message:      "Job started",
		ActiveStepId: stepID,
	}, nil
}

func (s *OrchestratorServer) SubmitStep(ctx context.Context, req *pb.SubmitStepRequest) (*pb.SubmitStepResponse, error) {
	jobID := req.GetJobId()
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required in SubmitStepRequest")
	}

	s.mu.RLock()
	tracker, exists := s.activeJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found in cache", jobID)
	}

	stepID := req.GetStepId()
	if stepID == "" {
		return nil, fmt.Errorf("step_id is required in SubmitStepRequest")
	}

	log.Printf("Received SubmitStep for JobID: %s, StepID: %s", jobID, stepID)

	payloadBytes, err := s.updateExecutionAndMarshal(tracker, req)
	if err != nil {
		return nil, err
	}

	err = s.pub.PublishPriorityJob(ctx, payloadBytes)
	if err != nil {
		log.Printf("ERROR: Failed to publish priority step to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue step: %w", err)
	}

	return &pb.SubmitStepResponse{
		Success: true,
		Message: "Step queued for execution",
		StepId:  stepID,
	}, nil
}

func (s *OrchestratorServer) handleJobMessage(ctx context.Context, msg jetstream.Msg) error {
	type Payload struct {
		StateBytes      []byte `json:"state_bytes"`
		StepReqBytes    []byte `json:"step_req_bytes"`
		CallbackAddress string `json:"callback_address"`
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

	s.mu.RLock()
	tracker, exists := s.activeJobs[req.JobId]
	s.mu.RUnlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if !exists {
		log.Printf("INFO: Job %s not found in cache. Attempting state recovery.", req.JobId)

		// Fallback 1: Query Traceability Service
		if s.traceClient != nil {
			ctxTrace, cancelTrace := context.WithTimeout(ctx, 5*time.Second)
			defer cancelTrace()
			resp, err := s.traceClient.GetJobState(ctxTrace, &pb.GetJobStateRequest{JobId: req.JobId})
			if err == nil && resp.Found && resp.Job != nil && resp.Job.OriginalRequest != nil {
				tracker = &JobTracker{
					State:           resp.Job.OriginalRequest.Context,
					FullSteps:       resp.Job.OriginalRequest.Steps,
					JobGraph:        resp.Job.OriginalRequest.JobGraph,
					CallbackAddress: resp.Job.OriginalRequest.CallbackAddress,
				}
				s.mu.Lock()
				s.activeJobs[req.JobId] = tracker
				s.mu.Unlock()
				exists = true
				log.Printf("INFO: [JobID: %s] Recovered state from Traceability", req.JobId)
			}
		}

		// Fallback 2: Query Client via CallbackAddress
		if !exists && p.CallbackAddress != "" {
			maxMsgSizeMb := 100
			if envVal := os.Getenv("GRPC_MAX_MESSAGE_SIZE_MB"); envVal != "" {
				if parsed, err := strconv.Atoi(envVal); err == nil {
					maxMsgSizeMb = parsed
				}
			}
			conn, err := grpc.NewClient(p.CallbackAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
			if err == nil {
				defer conn.Close()
				client := pb.NewOrchestratorCallbackServiceClient(conn)
				ctxCb, cancelCb := context.WithTimeout(ctx, 5*time.Second)
				defer cancelCb()

				resp, err := client.GetJobDetails(ctxCb, &pb.GetJobDetailsRequest{JobId: req.JobId})
				if err == nil && resp.Found && resp.OriginalRequest != nil {
					tracker = &JobTracker{
						State:           resp.OriginalRequest.Context,
						FullSteps:       resp.OriginalRequest.Steps,
						JobGraph:        resp.OriginalRequest.JobGraph,
						CallbackAddress: resp.OriginalRequest.CallbackAddress,
					}
					s.mu.Lock()
					s.activeJobs[req.JobId] = tracker
					s.mu.Unlock()
					exists = true
					log.Printf("INFO: [JobID: %s] Recovered state from Client", req.JobId)
				}
			}
		}

		if !exists {
			log.Printf("CRITICAL: Job %s could not be recovered from Traceability or Client. Dropping message.", req.JobId)
			msg.Ack()
			return nil
		}
	}

	tracker.Lock()
	tracker.Cancel = cancel
	tracker.Unlock()

	var activeStep *pb.StepDefinition
	for _, st := range tracker.FullSteps {
		if st.Id == req.StepId {
			activeStep = st
			break
		}
	}
	if activeStep == nil {
		return fmt.Errorf("step %s not found in job %s cache", req.StepId, req.JobId)
	}

	activeStepCopy := proto.Clone(activeStep).(*pb.StepDefinition)
	if req.CustomInstructions != "" {
		activeStepCopy.CustomInstructions = req.CustomInstructions
	}
	if req.ExpectedSchema != "" {
		activeStepCopy.ExpectedSchema = req.ExpectedSchema
	}

	// ZERO-SHOT CACHE BYPASS
	if req.CacheConfig != nil && req.CacheConfig.UseCache && s.cacheClient != nil {
		log.Printf("INFO: [JobID: %s | StepID: %s] Cache bypass enabled. Querying collection: %s", req.JobId, req.StepId, req.CacheConfig.CollectionName)
		ctxCache, cancelCache := context.WithTimeout(ctx, 10*time.Second)
		cacheResp, err := s.cacheClient.CheckCache(ctxCache, &pb.CheckCacheRequest{
			CollectionName: req.CacheConfig.CollectionName,
			Text:           req.CacheConfig.CacheInput,
			Metadata:       req.CacheConfig.Metadata,
			Threshold:      req.CacheConfig.Threshold,
		})
		cancelCache()

		if err != nil {
			log.Printf("WARN: [JobID: %s | StepID: %s] Cache bypass query failed: %v. Falling back to standard ReAct execution.", req.JobId, req.StepId, err)
		} else if cacheResp.Hit {
			log.Printf("SUCCESS: [JobID: %s | StepID: %s] CACHE HIT! Confidence: %.2f. Bypassing ReAct Engine entirely.", req.JobId, req.StepId, cacheResp.Confidence)
			s.fireCallback(tracker, state, req, cacheResp.ExtractedPayload, "", req.StepId)
			return nil
		} else {
			log.Printf("INFO: [JobID: %s | StepID: %s] CACHE MISS. Proceeding with standard ReAct execution.", req.JobId, req.StepId)
		}
	}

	log.Printf("Executing step %s for job %s", activeStepCopy.StepName, state.JobId)

	_, stepOutputJSON, _, lastStepId, err := s.eng.ExecuteWorkflow(ctx, state, activeStepCopy)

	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			log.Printf("INFO: Job %s paused for human validation", state.JobId)
			tracker.Lock()
			tracker.State = state
			tracker.Unlock()
			return nil
		}
		log.Printf("ERROR: Workflow execution failed: %v", err)
		s.fireCallback(tracker, state, req, stepOutputJSON, err.Error(), lastStepId)
		return nil
	}

	log.Printf("Workflow completed successfully for step %s. Firing callback.", activeStep.StepName)
	s.fireCallback(tracker, state, req, stepOutputJSON, "", lastStepId)

	return nil
}

func (s *OrchestratorServer) fireCallback(tracker *JobTracker, state *pb.WorkflowContext, req *pb.SubmitStepRequest, stepOutputJSON string, errMsg string, lastStepId string) {
	cbAddress := tracker.CallbackAddress
	if cbAddress == "" {
		log.Printf("No callback address provided, skipping callback")
		if req.CompleteJob || errMsg != "" {
			s.handleCallbackError(errMsg, state)
		}
		return
	}

	maxMsgSizeMb := 100
	if envVal := os.Getenv("GRPC_MAX_MESSAGE_SIZE_MB"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxMsgSizeMb = parsed
		}
	}
	conn, err := grpc.NewClient(cbAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		log.Printf("ERROR: Failed to dial callback address %s: %v", cbAddress, err)
		if req.CompleteJob || errMsg != "" {
			s.handleCallbackError(errMsg, state)
		}
		return
	}
	defer conn.Close()

	client := pb.NewOrchestratorCallbackServiceClient(conn)

	timeoutSecs := 10
	if envVal := os.Getenv("GRPC_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			timeoutSecs = parsed
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	if errMsg != "" {
		_, err = client.OnJobFailed(ctx, &pb.JobFailedRequest{
			JobId:        state.JobId,
			FailedStepId: lastStepId,
			ErrorMessage: errMsg,
		})
	} else {
		jobDefId := ""
		if state.JobDefinition != nil {
			jobDefId = state.JobDefinition.Id
		}

		var stepOutMap map[string]interface{}
		json.Unmarshal([]byte(stepOutputJSON), &stepOutMap)
		stepOutStruct, _ := structpb.NewStruct(stepOutMap)

		_, err = client.OnStepComplete(ctx, &pb.StepCompleteRequest{
			JobId:           state.JobId,
			JobDefinitionId: jobDefId,
			CompletedStepId: lastStepId,
			GlobalState:     state.GlobalState,
			StepOutput:      stepOutStruct,
		})
	}

	if err != nil {
		log.Printf("ERROR: Callback failed: %v", err)
	}

	if req.CompleteJob || errMsg != "" {
		s.handleCallbackError(errMsg, state)
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
	if stepReq == nil {
		tracker.Unlock()
		return &pb.ResumeJobResponse{Success: false, Message: "No active step to resume"}, nil
	}

	var currentStep *pb.StepDefinition
	for _, st := range tracker.FullSteps {
		if st.Id == stepReq.StepId {
			currentStep = st
			break
		}
	}
	if currentStep == nil {
		tracker.Unlock()
		return &pb.ResumeJobResponse{Success: false, Message: "Step definition not found in cache"}, nil
	}

	// Remove arbitrary WriteToStateKey, instead assume GlobalState merge from ReAct loop
	tracker.State.CurrentActionIndex++
	tracker.Unlock()

	log.Printf("INFO: Resuming job %s after human correction", jobID)

	payloadBytes, err := s.updateExecutionAndMarshal(tracker, stepReq)
	if err != nil {
		return nil, err
	}

	err = s.pub.PublishPriorityJob(ctx, payloadBytes)
	if err != nil {
		log.Printf("ERROR: Failed to publish resumed step to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue resumed step: %w", err)
	}

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *OrchestratorServer) handleCallbackError(errMsg string, state *pb.WorkflowContext) {
	s.mu.Lock()
	delete(s.activeJobs, state.JobId)
	s.mu.Unlock()
	log.Printf("INFO: Job %s complete or failed. Teared down cache.", state.JobId)

	if s.traceClient != nil {
		go func() {
			status := "COMPLETED"
			if errMsg != "" {
				status = "FAILED"
			}
			ctxTrace, cancelTrace := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelTrace()
			_, _ = s.traceClient.SaveJobState(ctxTrace, &pb.SaveJobStateRequest{
				JobId:  state.JobId,
				Status: status,
			})
		}()
	}
	return
}

func (s *OrchestratorServer) PurgeSystemState(ctx context.Context, req *pb.PurgeSystemStateRequest) (*pb.PurgeSystemStateResponse, error) {
	if req.GetAppId() == "" {
		return nil, fmt.Errorf("app_id is required")
	}

	log.Printf("INFO: Received PurgeSystemState request for AppID: %s (purge_queues=%v)", req.GetAppId(), req.GetPurgeQueues())

	s.mu.Lock()
	var jobsToCancel []string
	for jobID, tracker := range s.activeJobs {
		if tracker.State != nil && tracker.State.Metadata != nil {
			if tracker.State.Metadata["app_id"] == req.GetAppId() {
				jobsToCancel = append(jobsToCancel, jobID)
				if tracker.Cancel != nil {
					tracker.Cancel()
				}
			}
		}
	}
	// Remove cancelled jobs from tracking
	for _, jobID := range jobsToCancel {
		delete(s.activeJobs, jobID)
	}
	s.mu.Unlock()

	queuesPurged := 0
	if req.GetPurgeQueues() {
		err := s.pub.PurgeQueues(ctx)
		if err != nil {
			log.Printf("ERROR: Failed to purge queues: %v", err)
		} else {
			queuesPurged = 3 // JOBS, TRACE_EVENTS, CACHE_REQUESTS
		}
	}

	log.Printf("WARN: [Engine] [AppID: %s] System state purge commanded by client. Cancelled %d jobs, purged %d queues.", req.GetAppId(), len(jobsToCancel), queuesPurged)

	return &pb.PurgeSystemStateResponse{
		Success:       true,
		JobsCancelled: int32(len(jobsToCancel)),
		QueuesPurged:  int32(queuesPurged),
	}, nil
}

// updateExecutionAndMarshal locks the tracker, updates the ActiveExecution state, and serializes the JetStream payload.
func (s *OrchestratorServer) updateExecutionAndMarshal(tracker *JobTracker, stepReq *pb.SubmitStepRequest) ([]byte, error) {
	tracker.Lock()
	tracker.StepReq = stepReq
	if tracker.State.ActiveExecution == nil {
		tracker.State.ActiveExecution = &pb.ActiveJobExecution{JobId: stepReq.JobId}
	}
	tracker.State.ActiveExecution.CurrentStepId = stepReq.StepId
	tracker.Unlock()

	type Payload struct {
		StateBytes      []byte `json:"state_bytes"`
		StepReqBytes    []byte `json:"step_req_bytes"`
		CallbackAddress string `json:"callback_address"`
	}

	stateBytes, err := proto.Marshal(tracker.State)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize state: %w", err)
	}
	stepReqBytes, err := proto.Marshal(stepReq)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize step request: %w", err)
	}

	payloadBytes, err := json.Marshal(Payload{
		StateBytes:      stateBytes,
		StepReqBytes:    stepReqBytes,
		CallbackAddress: tracker.CallbackAddress,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to serialize payload: %w", err)
	}

	return payloadBytes, nil
}
