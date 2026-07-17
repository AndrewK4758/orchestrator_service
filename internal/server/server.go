package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"orchestrator_service/internal/logger"
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
	Req            *pb.ExecuteWorkflowNodeRequest
	Cancel         context.CancelFunc
	CurrentStepId  string
	StepInProgress bool
	mu             sync.Mutex
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
		logger.Warn("System", "failed to connect to cache service, cache will be disabled: %v", err)
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
	err = srv.pub.ConsumeJobs(context.Background(), srv.handleJobMessage)
	if err != nil {
		logger.Error("System", "Failed to start job consumer: %v", err)
	}

	return srv
}

func (s *OrchestratorServer) ExecuteWorkflowNode(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*pb.ExecuteWorkflowNodeResponse, error) {
	if req.GlobalState == nil {
		req.GlobalState, _ = structpb.NewStruct(map[string]any{})
	}

	jobID := req.JobId
	if jobID == "" {
		appID := "app"
		if req.Metadata != nil && req.Metadata["app_id"] != "" {
			appID = req.Metadata["app_id"]
		}
		jobID = fmt.Sprintf("%s_%d", appID, time.Now().UnixNano())
		req.JobId = jobID
	}

	if req.Metadata == nil {
		req.Metadata = make(map[string]string)
	}
	tenantID := req.Metadata["tenant_id"]
	if tenantID == "" {
		tenantID = "SYSTEM_DEFAULT_TENANT"
		req.Metadata["tenant_id"] = tenantID
	}

	logger.Info("System", "Received ExecuteWorkflowNode for JobID: %s, StepID: %s", jobID, req.StepId)

	tracker := &JobTracker{
		Req: req,
	}

	s.mu.Lock()
	s.activeJobs[jobID] = tracker
	s.mu.Unlock()

	payloadBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ExecuteWorkflowNodeRequest for job %s: %w", jobID, err)
	}

	err = s.pub.PublishJob(ctx, payloadBytes)
	if err != nil {
		logger.Error("System", "Failed to publish job to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue job: %w", err)
	}

	return &pb.ExecuteWorkflowNodeResponse{
		Success: true,
		Message: "Node queued for execution",
		StepId:  req.StepId,
	}, nil
}

func (s *OrchestratorServer) handleJobMessage(ctx context.Context, msg jetstream.Msg) error {
	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		return fmt.Errorf("failed to unmarshal ExecuteWorkflowNodeRequest: %w", err)
	}

	s.mu.Lock()
	tracker, exists := s.activeJobs[req.JobId]
	if !exists {
		tracker = &JobTracker{Req: req}
		s.activeJobs[req.JobId] = tracker
	}
	s.mu.Unlock()

	tracker.Lock()
	if tracker.StepInProgress && tracker.CurrentStepId == req.StepId {
		tracker.Unlock()
		logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for step in progress. Ignoring.", req.JobId, req.StepId)
		return nil
	}
	tracker.StepInProgress = true
	tracker.CurrentStepId = req.StepId

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tracker.Cancel = cancel
	tracker.Unlock()

	if req.NodeConfig == nil {
		return fmt.Errorf("node config is missing from request for job %s", req.JobId)
	}

	// ZERO-SHOT CACHE BYPASS
	if req.NodeConfig.Cache != nil && req.NodeConfig.Cache.Enabled && s.cacheClient != nil {
		logger.Info("Server", "[JobID: %s | StepID: %s] Cache bypass enabled. Querying collection: %s", req.JobId, req.StepId, req.NodeConfig.Cache.CollectionName)
		
		timeoutSecs := 120
		if envVal := os.Getenv("CACHE_TIMEOUT_SECONDS"); envVal != "" {
			if parsed, err := strconv.Atoi(envVal); err == nil {
				timeoutSecs = parsed
			}
		}
		
		var cacheMetadata *structpb.Struct
		if req.NodeConfig.Cache.Metadata != nil {
			metaMap := make(map[string]any)
			for k, v := range req.NodeConfig.Cache.Metadata {
				metaMap[k] = v
			}
			cacheMetadata, _ = structpb.NewStruct(metaMap)
		}

		ctxCache, cancelCache := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		cacheResp, err := s.cacheClient.CheckCache(ctxCache, &pb.CheckCacheRequest{
			CollectionName: req.NodeConfig.Cache.CollectionName,
			Text:           req.NodeConfig.Cache.LookupInput,
			Metadata:       cacheMetadata,
			Threshold:      req.NodeConfig.Cache.ConfidenceThreshold,
		})
		cancelCache()

		if err != nil {
			logger.Warn("Server", "[JobID: %s | StepID: %s] Cache bypass query failed: %v. Falling back to standard ReAct execution.", req.JobId, req.StepId, err)
		} else if cacheResp.Hit {
			tracker.Lock()
			tracker.StepInProgress = false
			tracker.Unlock()
			logger.Success("Server", "[JobID: %s | StepID: %s] CACHE HIT! Confidence: %.2f (Threshold: %.3f). Bypassing ReAct Engine entirely.", req.JobId, req.StepId, cacheResp.Confidence, req.NodeConfig.Cache.ConfidenceThreshold)
			if err := engine.MergeStateContext(req, cacheResp.ExtractedPayload, ""); err != nil {
				logger.Error("Server", "Failed to merge cache state context: %v", err)
			}
			s.fireCallback(req, cacheResp.ExtractedPayload, "", req.StepId)
			return nil
		} else {
			logger.Info("Server", "[JobID: %s | StepID: %s] CACHE MISS. Proceeding with standard ReAct execution.", req.JobId, req.StepId)
		}
	}

	logger.Info("System", "Executing step %s for job %s", req.NodeConfig.NodeName, req.JobId)

	_, stepOutputJSON, _, lastStepId, err := s.eng.ExecuteWorkflow(ctx, req)

	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			logger.Info("System", "Job %s paused for human validation", req.JobId)
			tracker.Lock()
			tracker.Req = req
			tracker.StepInProgress = false
			tracker.Unlock()
			return nil
		}
		tracker.Lock()
		tracker.StepInProgress = false
		tracker.Unlock()
		logger.Error("System", "Workflow execution failed: %v", err)
		s.fireCallback(req, stepOutputJSON, err.Error(), lastStepId)
		return nil
	}

	tracker.Lock()
	tracker.StepInProgress = false
	tracker.Unlock()
	logger.Info("System", "Workflow completed successfully for step %s. Firing callback.", req.NodeConfig.NodeName)
	if err := engine.MergeStateContext(req, stepOutputJSON, ""); err != nil {
		logger.Error("Server", "Failed to merge final state context: %v", err)
	}
	s.fireCallback(req, stepOutputJSON, "", lastStepId)

	return nil
}

func (s *OrchestratorServer) fireCallback(req *pb.ExecuteWorkflowNodeRequest, stepOutputJSON string, errMsg string, lastStepId string) {
	cbAddress := req.CallbackAddress
	if cbAddress == "" {
		logger.Info("System", "No callback address provided, skipping callback")
		if errMsg != "" {
			s.handleCallbackError(errMsg, req)
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
		logger.Error("System", "Failed to dial callback address %s: %v", cbAddress, err)
		s.handleCallbackError(errMsg, req)
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
			JobId:        req.JobId,
			FailedStepId: lastStepId,
			ErrorMessage: errMsg,
		})
	} else {
		var stepOutMap map[string]any
		json.Unmarshal([]byte(stepOutputJSON), &stepOutMap)
		stepOutStruct, _ := structpb.NewStruct(stepOutMap)

		_, err = client.OnNodeExecutionComplete(ctx, &pb.NodeExecutionCompleteRequest{
			JobId:           req.JobId,
			CompletedStepId: lastStepId,
			GlobalState:     req.GlobalState,
			NodeOutput:      stepOutStruct,
		})
	}

	if err != nil {
		logger.Error("System", "Callback failed: %v", err)
		s.handleCallbackError(errMsg, req)
	} else if errMsg != "" {
		s.handleCallbackError(errMsg, req)
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
	stepReq := tracker.Req
	if stepReq == nil {
		tracker.Unlock()
		return &pb.ResumeJobResponse{Success: false, Message: "No active step to resume"}, nil
	}
	tracker.Unlock()

	logger.Info("System", "Resuming job %s after human correction", jobID)

	payloadBytes, err := proto.Marshal(stepReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resumed request for job %s: %w", jobID, err)
	}

	err = s.pub.PublishPriorityJob(ctx, payloadBytes)
	if err != nil {
		logger.Error("System", "Failed to publish resumed step to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue resumed step: %w", err)
	}

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *OrchestratorServer) handleCallbackError(errMsg string, req *pb.ExecuteWorkflowNodeRequest) {
	s.mu.Lock()
	delete(s.activeJobs, req.JobId)
	s.mu.Unlock()
	logger.Info("System", "Job %s complete or failed. Teared down cache.", req.JobId)

	if s.traceClient != nil {
		go func() {
			status := "COMPLETED"
			if errMsg != "" {
				status = "FAILED"
			}
			ctxTrace, cancelTrace := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelTrace()
			_, _ = s.traceClient.SaveJobState(ctxTrace, &pb.SaveJobStateRequest{
				JobId:  req.JobId,
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

	logger.Info("System", "Received PurgeSystemState request for AppID: %s (purge_queues=%v)", req.GetAppId(), req.GetPurgeQueues())

	s.mu.Lock()
	var jobsToCancel []string
	for jobID, tracker := range s.activeJobs {
		if tracker.Req != nil && tracker.Req.Metadata != nil {
			if tracker.Req.Metadata["app_id"] == req.GetAppId() {
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
			logger.Error("System", "Failed to purge queues: %v", err)
		} else {
			queuesPurged = 3 // JOBS, TRACE_EVENTS, CACHE_REQUESTS
		}
	}

	logger.Warn("Engine", "[AppID: %s] System state purge commanded by client. Cancelled %d jobs, purged %d queues.", req.GetAppId(), len(jobsToCancel), queuesPurged)

	return &pb.PurgeSystemStateResponse{
		Success:       true,
		JobsCancelled: int32(len(jobsToCancel)),
		QueuesPurged:  int32(queuesPurged),
	}, nil
}
