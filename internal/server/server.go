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
	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type JobExecInfo struct {
	AppID  string
	Cancel context.CancelFunc
}

// OrchestratorServer implements the OrchestratorService gRPC interface.
type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer

	eng      *engine.Engine
	aiClient *ai_client.Client
	pub      JobPublisher

	kvStore      jetstream.KeyValue
	activeExecMu sync.RWMutex
	activeExecs  map[string]*JobExecInfo

	traceClient pb.TraceabilityQueryClient
	cacheClient pb.SemanticCacheServiceClient
}

type JobPublisher interface {
	engine.TracePublisher
	PublishJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error
	PublishPriorityJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error
	ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error
	PublishWebhook(ctx context.Context, payload *events.WebhookEvent) error
	ConsumeWebhooks(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error
	PurgeQueues(ctx context.Context) error
	GetActiveJobsKV(ctx context.Context) (jetstream.KeyValue, error)
}

func sanitizeMsgId(id string) string {
	id = strings.ReplaceAll(id, " ", "_")
	id = strings.ReplaceAll(id, ".", "_")
	id = strings.ReplaceAll(id, ">", "_")
	id = strings.ReplaceAll(id, "*", "_")
	return id
}

func NewServer(ctx context.Context, aiClient *ai_client.Client, pub JobPublisher, traceClient pb.TraceabilityQueryClient) *OrchestratorServer {
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

	kvStore, err := pub.GetActiveJobsKV(ctx)
	if err != nil {
		logger.Error("System", "Failed to initialize active_jobs KV store: %v", err)
	}

	srv := &OrchestratorServer{
		eng:         eng,
		aiClient:    aiClient,
		pub:         pub,
		kvStore:     kvStore,
		activeExecs: make(map[string]*JobExecInfo),
		traceClient: traceClient,
		cacheClient: cacheClient,
	}

	// Load pending jobs from Traceability Service
	err = srv.pub.ConsumeJobs(ctx, srv.handleJobMessage)
	if err != nil {
		logger.Error("System", "Failed to start job consumer: %v", err)
	}

	err = srv.pub.ConsumeWebhooks(ctx, srv.processWebhookMsg)
	if err != nil {
		logger.Error("System", "Failed to start webhooks consumer: %v", err)
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

		if req.Identity == nil {
			return fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity is strictly required")
		}
		
		jobID := req.Identity.JobId
		if jobID == "" {
			return fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity.JobId is strictly required")
		}

		tenantID := req.Identity.TenantId
		if tenantID == "" {
			return fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity.TenantId is strictly required")
		}

		logger.Info("System", "Received ExecuteWorkflowNode for JobID: %s, StepID: %s", jobID, req.StepId)

		payloadBytes, err := proto.Marshal(req)
		if err != nil {
			return fmt.Errorf("failed to marshal ExecuteWorkflowNodeRequest for job %s: %w", jobID, err)
		}

		msgId := fmt.Sprintf("%s-%s", jobID, req.StepId)
		msgId = sanitizeMsgId(msgId)
		err = s.pub.PublishJob(ctx, req.Identity, msgId, payloadBytes)
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
	appID := ""
	if req.Identity != nil {
		jobID = req.Identity.JobId
		appID = req.Identity.AppId
	}

	if s.traceClient != nil {
		ctxTrace, cancelTrace := context.WithTimeout(ctx, 2*time.Second)
		resp, err := s.traceClient.GetJobState(ctxTrace, &pb.GetJobStateRequest{Identity: req.Identity})
		cancelTrace()
		if err == nil && resp.Found && resp.Job != nil && (resp.Job.Status == "COMPLETED" || resp.Job.Status == "FAILED") {
			logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for fully terminated job. Ignoring.", jobID, req.StepId)
			return nil
		}
	}

	s.activeExecMu.Lock()
	if _, exists := s.activeExecs[jobID]; exists {
		s.activeExecMu.Unlock()
		logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for step in progress. Ignoring.", jobID, req.StepId)
		return nil
	}
	
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.activeExecs[jobID] = &JobExecInfo{AppID: appID, Cancel: cancel}
	s.activeExecMu.Unlock()

	defer func() {
		s.activeExecMu.Lock()
		delete(s.activeExecs, jobID)
		s.activeExecMu.Unlock()
	}()

	if req.NodeConfig == nil {
		return fmt.Errorf("node config is missing from request for job %s", jobID)
	}
	
	// ZERO-SHOT CACHE BYPASS (remove tracker references, use execCtx)
	if req.NodeConfig.Cache != nil && req.NodeConfig.Cache.Enabled && s.cacheClient != nil {
		logger.Info("Server", "[JobID: %s | StepID: %s] Cache bypass enabled. Querying collection: %s", jobID, req.StepId, req.NodeConfig.Cache.CollectionName)

		timeoutSecs := 120
		if envVal := os.Getenv("CACHE_TIMEOUT_SECONDS"); envVal != "" {
			if parsed, err := strconv.Atoi(envVal); err == nil {
				timeoutSecs = parsed
			}
		}

		ctxCache, cancelCache := context.WithTimeout(execCtx, time.Duration(timeoutSecs)*time.Second)
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
			logger.Success("Server", "[JobID: %s | StepID: %s] CACHE HIT! Confidence: %.2f (Threshold: %.3f). Bypassing ReAct Engine entirely.", jobID, req.StepId, cacheResp.Confidence, req.NodeConfig.Cache.ConfidenceThreshold)
			if err := engine.MergeStateContext(req, cacheResp.ExtractedPayload, ""); err != nil {
				logger.Error("Server", "Failed to merge cache state context: %v", err)
			}
			err := s.fireCallback(execCtx, req, cacheResp.ExtractedPayload, "", req.StepId)
			if err != nil {
				return fmt.Errorf("failed to fire callback for cache hit: %w", err)
			}
			return nil
		} else {
			logger.Info("Server", "[JobID: %s | StepID: %s] CACHE MISS. Proceeding with standard ReAct execution.", jobID, req.StepId)
		}
	}

	logger.Info("System", "Executing step %s for job %s", req.NodeConfig.NodeName, jobID)

	_, stepOutputJSON, _, lastStepId, err := s.eng.ExecuteWorkflow(execCtx, req)

	if err != nil {
		if errors.Is(err, engine.ErrPendingHumanValidation) {
			logger.Info("System", "Job %s paused for human validation", jobID)
			if s.kvStore != nil {
				reqBytes, marshalErr := proto.Marshal(req)
				if marshalErr != nil {
					return fmt.Errorf("failed to marshal request for paused job: %w", marshalErr)
				}
				_, putErr := s.kvStore.Put(execCtx, jobID, reqBytes)
				if putErr != nil {
					return fmt.Errorf("failed to persist paused job in KV store: %w", putErr)
				}
			}
			return nil
		}
		logger.Error("System", "Workflow execution failed: %v", err)
		return s.fireCallback(execCtx, req, stepOutputJSON, err.Error(), lastStepId)
	}

	logger.Info("System", "Workflow completed successfully for step %s. Firing callback.", req.NodeConfig.NodeName)
	if err := engine.MergeStateContext(req, stepOutputJSON, ""); err != nil {
		logger.Error("Server", "Failed to merge final state context: %v", err)
	}
	err = s.fireCallback(execCtx, req, stepOutputJSON, "", lastStepId)
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

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request for webhook payload: %w", err)
	}

	evt := &events.WebhookEvent{
		ReqBytes:       reqBytes,
		StepOutputJSON: stepOutputJSON,
		ErrMsg:         errMsg,
		LastStepId:     lastStepId,
	}

	err = s.pub.PublishWebhook(ctx, evt)
	if err != nil {
		return fmt.Errorf("failed to publish webhook event: %w", err)
	}

	if errMsg != "" {
		s.handleCallbackError(ctx, errMsg, req)
	}

	return nil
}

func (s *OrchestratorServer) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
	jobID := ""
	if req.GetIdentity() != nil {
		jobID = req.GetIdentity().GetJobId()
	}

	if s.kvStore == nil {
		return nil, fmt.Errorf("KV store is not initialized")
	}

	kvEntry, err := s.kvStore.Get(ctx, jobID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return &pb.ResumeJobResponse{Success: false, Message: "Job not found in KV store"}, nil
		}
		return nil, fmt.Errorf("failed to get paused job from KV store: %w", err)
	}

	// Delete from KV store now that it's resumed
	err = s.kvStore.Delete(ctx, jobID)
	if err != nil {
		logger.Warn("System", "Failed to delete KV entry for job %s: %v", jobID, err)
	}

	stepReq := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(kvEntry.Value(), stepReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal saved request: %w", err)
	}

	logger.Info("System", "Resuming job %s after human correction", jobID)

	// Merge the corrected JSON into the original state context
	if err := engine.MergeStateContext(stepReq, req.GetCorrectedJson(), ""); err != nil {
		return nil, fmt.Errorf("failed to merge corrected JSON: %w", err)
	}

	// Immediately fire webhook callback instead of publishing to JetStream priority queue
	err = s.fireCallback(ctx, stepReq, req.GetCorrectedJson(), "", stepReq.StepId)
	if err != nil {
		logger.Error("System", "Failed to fire callback for resumed job: %v", err)
		return nil, fmt.Errorf("failed to fire callback for resumed job: %w", err)
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

	s.activeExecMu.Lock()
	var jobsToCancel []string
	for jobID, execInfo := range s.activeExecs {
		if execInfo.AppID != "" && execInfo.AppID == req.GetIdentity().GetAppId() {
			jobsToCancel = append(jobsToCancel, jobID)
			if execInfo.Cancel != nil {
				execInfo.Cancel()
			}
		}
	}
	// Remove cancelled jobs from tracking
	for _, jobID := range jobsToCancel {
		delete(s.activeExecs, jobID)
	}
	s.activeExecMu.Unlock()

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

func (s *OrchestratorServer) processWebhookMsg(ctx context.Context, msg jetstream.Msg) error {
	var evt events.WebhookEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		logger.Error("System", "Failed to unmarshal webhook msg: %v", err)
		return nil // Invalid payload, don't retry
	}

	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(evt.ReqBytes, req); err != nil {
		logger.Error("System", "Failed to unmarshal proto for webhook: %v", err)
		return nil // Invalid proto, don't retry
	}

	cbAddress := req.CallbackAddress
	if cbAddress == "" {
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

	if evt.ErrMsg != "" {
		_, err = client.OnJobFailed(ctx, &pb.JobFailedRequest{
			Identity:     req.Identity,
			FailedStepId: evt.LastStepId,
			ErrorMessage: evt.ErrMsg,
			Metadata:     req.Metadata,
		})
	} else {
		var stepOutMap map[string]any
		json.Unmarshal([]byte(evt.StepOutputJSON), &stepOutMap)
		stepOutStruct, _ := structpb.NewStruct(stepOutMap)

		var stream pb.OrchestratorCallbackService_OnNodeExecutionCompleteClient
		stream, err = client.OnNodeExecutionComplete(ctx)
		if err == nil {
			err = stream.Send(&pb.NodeExecutionCompleteRequest{
				Identity:        req.Identity,
				CompletedStepId: evt.LastStepId,
				GlobalState:     req.GlobalState,
				NodeOutput:      stepOutStruct,
				Metadata:        req.Metadata,
			})
			if err == nil {
				stream.CloseSend()
				_, err = stream.Recv()
				if err == io.EOF {
					err = nil
				}
			}
		}
	}

	if err != nil {
		return fmt.Errorf("callback failed: %w", err)
	}

	return nil
}
