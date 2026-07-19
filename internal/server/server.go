package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"orchestrator_service/internal/logger"
	"os"
	"strconv"
	"strings"
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
	CompletedSteps map[string]bool
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
	PublishJob(ctx context.Context, msgId string, payload []byte) error
	PublishPriorityJob(ctx context.Context, msgId string, payload []byte) error
	ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error
	PurgeQueues(ctx context.Context) error
}

func sanitizeMsgId(id string) string {
	id = strings.ReplaceAll(id, " ", "_")
	id = strings.ReplaceAll(id, ".", "_")
	id = strings.ReplaceAll(id, ">", "_")
	id = strings.ReplaceAll(id, "*", "_")
	return id
}

func NewServer(aiClient *ai_client.Client, pub JobPublisher, traceClient pb.TraceabilityQueryClient) *OrchestratorServer {
	cacheClient, _, err := cache_client.NewClient(os.Getenv("SEMANTIC_CACHE_SERVICE_ADDRESS"))
	if err != nil {
		logger.Warn("System", "failed to connect to cache service, cache will be disabled: %v", err)
	}

	eng := engine.NewEngine(aiClient, cacheClient, pub)

	docProcAddr := os.Getenv("DOCUMENT_PROCESSING_SERVICE_ADDRESS")
	if docProcAddr == "" {
		docProcAddr = "127.0.0.1:50054"
	}
	docConn, err := grpc.NewClient(docProcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Warn("System", "failed to connect to document processing service: %v", err)
	} else {
		eng.SetChunkingClient(pb.NewDocumentProcessingServiceClient(docConn))
	}

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

func (s *OrchestratorServer) ExecuteWorkflowNode(stream pb.OrchestratorService_ExecuteWorkflowNodeServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		ctx := stream.Context()

		if req.GlobalState == nil {
			req.GlobalState, _ = structpb.NewStruct(map[string]any{})
		}

		jobID := ""
		if req.Identity != nil {
			jobID = req.Identity.JobId
		}
		if jobID == "" {
			appID := "app"
			if req.Identity != nil && req.Identity.AppId != "" {
				appID = req.Identity.AppId
			}
			jobID = fmt.Sprintf("%s_%d", appID, time.Now().UnixNano())
			if req.Identity == nil {
				req.Identity = &pb.InfrastructureIdentity{}
			}
			req.Identity.JobId = jobID
		}

		tenantID := ""
		if req.Identity != nil {
			tenantID = req.Identity.TenantId
		}
		if tenantID == "" {
			tenantID = "SYSTEM_DEFAULT_TENANT"
			if req.Identity == nil {
				req.Identity = &pb.InfrastructureIdentity{}
			}
			req.Identity.TenantId = tenantID
		}

		logger.Info("System", "Received ExecuteWorkflowNode for JobID: %s, StepID: %s", jobID, req.StepId)

		s.mu.Lock()
		tracker, exists := s.activeJobs[jobID]
		if !exists {
			tracker = &JobTracker{Req: req, CompletedSteps: make(map[string]bool)}
			s.activeJobs[jobID] = tracker
		} else {
			tracker.Lock()
			tracker.Req = req
			tracker.Unlock()
		}
		s.mu.Unlock()

		payloadBytes, err := proto.Marshal(req)
		if err != nil {
			return fmt.Errorf("failed to marshal ExecuteWorkflowNodeRequest for job %s: %w", jobID, err)
		}

		msgId := fmt.Sprintf("%s-%s", jobID, req.StepId)
		msgId = sanitizeMsgId(msgId)
		err = s.pub.PublishJob(ctx, msgId, payloadBytes)
		if err != nil {
			logger.Error("System", "Failed to publish job to JetStream for JobID %s: %v", jobID, err)
			return fmt.Errorf("failed to queue job: %w", err)
		}

		if err := stream.Send(&pb.ExecuteWorkflowNodeResponse{
			Success: true,
			Message: "Node queued for execution",
			StepId:  req.StepId,
		}); err != nil {
			return err
		}
	}
}

func (s *OrchestratorServer) handleJobMessage(ctx context.Context, msg jetstream.Msg) error {
	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		return fmt.Errorf("failed to unmarshal ExecuteWorkflowNodeRequest: %w", err)
	}

	jobID := ""
	if req.Identity != nil {
		jobID = req.Identity.JobId
	}

	s.mu.Lock()
	tracker, exists := s.activeJobs[jobID]
	if !exists {
		s.mu.Unlock()
		if s.traceClient != nil {
			ctxTrace, cancelTrace := context.WithTimeout(ctx, 2*time.Second)
			resp, err := s.traceClient.GetJobState(ctxTrace, &pb.GetJobStateRequest{Identity: req.Identity})
			cancelTrace()
			if err == nil && resp.Found && resp.Job != nil && (resp.Job.Status == "COMPLETED" || resp.Job.Status == "FAILED") {
				logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for fully terminated job. Ignoring.", jobID, req.StepId)
				return nil
			}
		}
		s.mu.Lock()
		tracker, exists = s.activeJobs[jobID]
		if !exists {
			tracker = &JobTracker{Req: req, CompletedSteps: make(map[string]bool)}
			s.activeJobs[jobID] = tracker
		}
	}
	s.mu.Unlock()

	tracker.Lock()
	if tracker.CompletedSteps != nil && tracker.CompletedSteps[req.StepId] {
		tracker.Unlock()
		logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for already completed step. Ignoring.", jobID, req.StepId)
		return nil
	}

	if tracker.StepInProgress && tracker.CurrentStepId == req.StepId {
		tracker.Unlock()
		logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for step in progress. Ignoring.", jobID, req.StepId)
		return nil
	}
	tracker.StepInProgress = true
	tracker.CurrentStepId = req.StepId

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tracker.Cancel = cancel
	tracker.Unlock()

	if req.NodeConfig == nil {
		return fmt.Errorf("node config is missing from request for job %s", jobID)
	}

	// ZERO-SHOT CACHE BYPASS
	if req.NodeConfig.Cache != nil && req.NodeConfig.Cache.Enabled && s.cacheClient != nil {
		logger.Info("Server", "[JobID: %s | StepID: %s] Cache bypass enabled. Querying collection: %s", jobID, req.StepId, req.NodeConfig.Cache.CollectionName)

		timeoutSecs := 120
		if envVal := os.Getenv("CACHE_TIMEOUT_SECONDS"); envVal != "" {
			if parsed, err := strconv.Atoi(envVal); err == nil {
				timeoutSecs = parsed
			}
		}

		ctxCache, cancelCache := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		cacheResp, err := s.cacheClient.CheckCache(ctxCache, &pb.CheckCacheRequest{
			CollectionName: req.NodeConfig.Cache.CollectionName,
			Text:           req.NodeConfig.Cache.LookupInput,
			Identity:       req.GetIdentity(),
			Threshold:      req.NodeConfig.Cache.ConfidenceThreshold,
		})
		cancelCache()

		if err != nil {
			logger.Warn("Server", "[JobID: %s | StepID: %s] Cache bypass query failed: %v. Falling back to standard ReAct execution.", jobID, req.StepId, err)
		} else if cacheResp.Hit {
			tracker.Lock()
			tracker.StepInProgress = false
			tracker.Unlock()
			logger.Success("Server", "[JobID: %s | StepID: %s] CACHE HIT! Confidence: %.2f (Threshold: %.3f). Bypassing ReAct Engine entirely.", jobID, req.StepId, cacheResp.Confidence, req.NodeConfig.Cache.ConfidenceThreshold)
			if err := engine.MergeStateContext(req, cacheResp.ExtractedPayload, ""); err != nil {
				logger.Error("Server", "Failed to merge cache state context: %v", err)
			}
			err := s.fireCallback(ctx, req, cacheResp.ExtractedPayload, "", req.StepId)
			if err == nil {
				tracker.Lock()
				if tracker.CompletedSteps == nil {
					tracker.CompletedSteps = make(map[string]bool)
				}
				tracker.CompletedSteps[req.StepId] = true
				tracker.Unlock()
			}
			if err != nil {
				return fmt.Errorf("failed to fire callback for cache hit: %w", err)
			}
			return nil
		} else {
			logger.Info("Server", "[JobID: %s | StepID: %s] CACHE MISS. Proceeding with standard ReAct execution.", jobID, req.StepId)
		}
	}

	logger.Info("System", "Executing step %s for job %s", req.NodeConfig.NodeName, jobID)

	_, stepOutputJSON, _, lastStepId, err := s.eng.ExecuteWorkflow(ctx, req)

	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			logger.Info("System", "Job %s paused for human validation", jobID)
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
		return s.fireCallback(ctx, req, stepOutputJSON, err.Error(), lastStepId)
	}

	tracker.Lock()
	tracker.StepInProgress = false
	tracker.Unlock()
	logger.Info("System", "Workflow completed successfully for step %s. Firing callback.", req.NodeConfig.NodeName)
	if err := engine.MergeStateContext(req, stepOutputJSON, ""); err != nil {
		logger.Error("Server", "Failed to merge final state context: %v", err)
	}
	err = s.fireCallback(ctx, req, stepOutputJSON, "", lastStepId)
	if err == nil {
		tracker.Lock()
		if tracker.CompletedSteps == nil {
			tracker.CompletedSteps = make(map[string]bool)
		}
		tracker.CompletedSteps[req.StepId] = true
		tracker.Unlock()
	}
	if err != nil {
		return fmt.Errorf("failed to fire callback for successful workflow: %w", err)
	}
	return nil
}

func (s *OrchestratorServer) fireCallback(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest, stepOutputJSON string, errMsg string, lastStepId string) error {
	cbAddress := req.CallbackAddress
	if cbAddress == "" {
		logger.Info("System", "No callback address provided, skipping callback")
		if errMsg != "" {
			s.handleCallbackError(ctx, errMsg, req)
		}
		return nil
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
		return fmt.Errorf("failed to dial callback address %s: %w", cbAddress, err)
	}
	defer conn.Close()

	client := pb.NewOrchestratorCallbackServiceClient(conn)

	timeoutSecs := 10
	if envVal := os.Getenv("GRPC_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			timeoutSecs = parsed
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	if errMsg != "" {
		_, err = client.OnJobFailed(ctx, &pb.JobFailedRequest{
			Identity:     req.Identity,
			FailedStepId: lastStepId,
			ErrorMessage: errMsg,
		})
	} else {
		var stepOutMap map[string]any
		json.Unmarshal([]byte(stepOutputJSON), &stepOutMap)
		stepOutStruct, _ := structpb.NewStruct(stepOutMap)

		stream, err := client.OnNodeExecutionComplete(ctx)
		if err == nil {
			err = stream.Send(&pb.NodeExecutionCompleteRequest{
				Identity:        req.Identity,
				CompletedStepId: lastStepId,
				GlobalState:     req.GlobalState,
				NodeOutput:      stepOutStruct,
			})
			if err == nil {
				stream.CloseSend()
				_, err = stream.Recv()
			}
		}
	}

	if err != nil {
		logger.Error("System", "Callback failed: %v", err)
		s.handleCallbackError(ctx, err.Error(), req)
		return fmt.Errorf("callback failed: %w", err)
	} else if errMsg != "" {
		s.handleCallbackError(ctx, errMsg, req)
	}
	return nil
}

func (s *OrchestratorServer) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
	jobID := ""
	if req.GetIdentity() != nil {
		jobID = req.GetIdentity().GetJobId()
	}
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

	msgId := fmt.Sprintf("%s-%s-resume", jobID, stepReq.StepId)
	msgId = sanitizeMsgId(msgId)
	err = s.pub.PublishPriorityJob(ctx, msgId, payloadBytes)
	if err != nil {
		logger.Error("System", "Failed to publish resumed step to JetStream for JobID %s: %v", jobID, err)
		return nil, fmt.Errorf("failed to queue resumed step: %w", err)
	}

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *OrchestratorServer) handleCallbackError(ctx context.Context, errMsg string, req *pb.ExecuteWorkflowNodeRequest) {
	jobID := ""
	if req.Identity != nil {
		jobID = req.Identity.JobId
	}
	s.mu.Lock()
	delete(s.activeJobs, jobID)
	s.mu.Unlock()
	logger.Info("System", "Job %s complete or failed. Teared down cache.", jobID)

	if s.traceClient != nil {
		go func() {
			status := "COMPLETED"
			if errMsg != "" {
				status = "FAILED"
			}
			ctxTrace, cancelTrace := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancelTrace()
			_, _ = s.traceClient.SaveJobState(ctxTrace, &pb.SaveJobStateRequest{
				Identity: req.Identity,
				Status:   status,
			})
		}()
	}
	return
}

func (s *OrchestratorServer) PurgeSystemState(ctx context.Context, req *pb.PurgeSystemStateRequest) (*pb.PurgeSystemStateResponse, error) {
	if req.GetIdentity().GetAppId() == "" {
		return nil, fmt.Errorf("app_id is required")
	}

	logger.Info("System", "Received PurgeSystemState request for AppID: %s (purge_queues=%v)", req.GetIdentity().GetAppId(), req.GetPurgeQueues())

	s.mu.Lock()
	var jobsToCancel []string
	for jobID, tracker := range s.activeJobs {
		if tracker.Req != nil {
			var appID string
			if tracker.Req.Identity != nil {
				appID = tracker.Req.Identity.AppId
			} else if tracker.Req.Metadata != nil {
				appID = tracker.Req.Metadata["app_id"]
			}
			if appID != "" && appID == req.GetIdentity().GetAppId() {
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

	logger.Warn("Engine", "[AppID: %s] System state purge commanded by client. Cancelled %d jobs, purged %d queues.", req.GetIdentity().GetAppId(), len(jobsToCancel), queuesPurged)

	return &pb.PurgeSystemStateResponse{
		Success:       true,
		JobsCancelled: int32(len(jobsToCancel)),
		QueuesPurged:  int32(queuesPurged),
	}, nil
}
