package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/workflowparser"

	"google.golang.org/protobuf/types/known/structpb"

	"orchestrator_service/internal/cache_client"
	"orchestrator_service/internal/engine"
	"orchestrator_service/internal/events"

	"github.com/santhosh-tekuri/jsonschema/v5"

	pb "github.com/AndrewK4758/shared_protos"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

type JobExecInfo struct {
	AppID  string
	Cancel context.CancelFunc
}

// OrchestratorServer implements the OrchestratorService gRPC interface.
type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer

	eng *engine.Engine
	pub JobPublisher

	kvStore      jetstream.KeyValue
	activeExecMu sync.RWMutex
	activeExecs  map[string]*JobExecInfo

	traceClient pb.TraceabilityQueryClient
	cacheClient pb.SemanticCacheServiceClient

	webhookConnMu sync.RWMutex
	webhookConns  map[string]*grpc.ClientConn

	cacheTimeoutSecs     int
	grpcMaxMessageSizeMb int
	grpcTimeoutSecs      int
}

type JobPublisher interface {
	engine.TracePublisher
	PublishJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error
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

func NewServer(ctx context.Context, pub JobPublisher, traceClient pb.TraceabilityQueryClient) *OrchestratorServer {
	cacheTimeout := envutil.MustGetInt("CACHE_TIMEOUT_SECONDS")
	grpcMaxMsg := envutil.MustGetInt("GRPC_MAX_MESSAGE_SIZE_MB")
	grpcTimeout := envutil.MustGetInt("GRPC_TIMEOUT_SECONDS")

	cacheClient, _, err := cache_client.NewClient(envutil.MustGetString("SEMANTIC_CACHE_SERVICE_ADDRESS"))
	if err != nil {
		logger.Warn("System", "failed to connect to cache service, cache will be disabled: %v", err)
	}

	kvStore, err := pub.GetActiveJobsKV(ctx)
	if err != nil {
		logger.Error("System", "Failed to initialize active_jobs KV store: %v", err)
	}

	eng, err := engine.NewEngine(ctx, cacheClient, pub, kvStore)
	if err != nil {
		logger.Error("System", "Failed to initialize engine: %v", err)
		panic(fmt.Errorf("failed to initialize engine: %w", err))
	}

	srv := &OrchestratorServer{
		eng:                  eng,
		pub:                  pub,
		kvStore:              kvStore,
		activeExecs:          make(map[string]*JobExecInfo),
		traceClient:          traceClient,
		cacheClient:          cacheClient,
		webhookConns:         make(map[string]*grpc.ClientConn),
		cacheTimeoutSecs:     cacheTimeout,
		grpcMaxMessageSizeMb: grpcMaxMsg,
		grpcTimeoutSecs:      grpcTimeout,
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

func (s *OrchestratorServer) ExecuteWorkflowNode(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*pb.ExecuteWorkflowNodeResponse, error) {
	if req.GlobalState == nil {
		return nil, fmt.Errorf("rpc error: code = InvalidArgument desc = GlobalState is strictly required")
	}
	if req.NodeConfig == nil {
		return nil, fmt.Errorf("rpc error: code = InvalidArgument desc = NodeConfig is strictly required")
	}

	if req.Identity == nil {
		return nil, fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity is strictly required")
	}

	jobID := req.Identity.JobId
	if jobID == "" {
		return nil, fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity.JobId is strictly required")
	}

	tenantID := req.Identity.TenantId
	if tenantID == "" {
		return nil, fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity.TenantId is strictly required")
	}

	appID := req.Identity.AppId
	if appID == "" {
		return nil, fmt.Errorf("rpc error: code = InvalidArgument desc = InfrastructureIdentity.AppId is strictly required")
	}

	logger.Info("System", "Received ExecuteWorkflowNode for JobID: %s, StepID: %s", jobID, req.StepId)

	payloadBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ExecuteWorkflowNodeRequest for job %s: %w", jobID, err)
	}

	msgId := fmt.Sprintf("%s-%s", jobID, req.StepId)
	msgId = sanitizeMsgId(msgId)

	pubTimeoutSecs := envutil.MustGetInt("PUBLISH_TIMEOUT_SECONDS")
	if pubTimeoutSecs <= 0 {
		pubTimeoutSecs = 5
	}
	pubCtx, cancel := context.WithTimeout(ctx, time.Duration(pubTimeoutSecs)*time.Second)
	defer cancel()

	err = s.pub.PublishJob(pubCtx, req.Identity, msgId, payloadBytes)
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

	if req.Identity == nil {
		return fmt.Errorf("missing Identity for job message")
	}
	if req.NodeConfig == nil {
		return fmt.Errorf("missing NodeConfig for job %s", req.Identity.JobId)
	}

	parser := workflowparser.NewNodeRequestParser(req)
	if err := parser.Validate(); err != nil {
		logger.Error("System", "Job message missing valid InfrastructureIdentity. Terminating message: %v", err)
		msg.Term()
		return nil
	}
	_, appID, jobID := parser.ExtractIdentity()

	if s.traceClient != nil {
		traceTimeoutSecs := envutil.MustGetInt("TRACEABILITY_TIMEOUT_SECONDS")
		if traceTimeoutSecs <= 0 {
			traceTimeoutSecs = 2
		}
		ctxTrace, cancelTrace := context.WithTimeout(ctx, time.Duration(traceTimeoutSecs)*time.Second)
		resp, err := s.traceClient.GetJobState(ctxTrace, &pb.GetJobStateRequest{Identity: parser.GetIdentity()})
		cancelTrace()
		if err == nil && resp.Found && resp.Job != nil && (resp.Job.Status == StatusCompleted || resp.Job.Status == StatusFailed) {
			logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for fully terminated job. Ignoring.", jobID, parser.GetStepId())
			return nil
		}
	}

	s.activeExecMu.Lock()
	if _, exists := s.activeExecs[jobID]; exists {
		s.activeExecMu.Unlock()
		logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for step in progress. Ignoring.", jobID, parser.GetStepId())
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

	// ZERO-SHOT CACHE BYPASS (remove tracker references, use execCtx)
	cacheConfig := parser.GetCacheContract()
	if cacheConfig != nil && cacheConfig.Enabled && s.cacheClient != nil {
		logger.Info("Server", "[JobID: %s | StepID: %s] Cache bypass enabled. Querying collection: %s", jobID, parser.GetStepId(), cacheConfig.CollectionName)
		logger.Info("Server", "[JobID: %s | StepID: %s] Cache Inputs -> MetadataLookup: %v | EmbeddingLookup provided: %t", 
			jobID, parser.GetStepId(), cacheConfig.MetadataLookup, cacheConfig.EmbeddingLookup != nil)


		ctxCache, cancelCache := context.WithTimeout(execCtx, time.Duration(s.cacheTimeoutSecs)*time.Second)
		cacheResp, err := s.cacheClient.CheckCache(ctxCache, &pb.CheckCacheRequest{
			Enabled:         cacheConfig.Enabled,
			CollectionName:  cacheConfig.CollectionName,
			CacheHit:        cacheConfig.CacheHit,
			Confidence:      cacheConfig.Confidence,
			EmbeddingLookup: cacheConfig.EmbeddingLookup,
			MetadataLookup:  cacheConfig.MetadataLookup,
			Identity:        parser.GetIdentity(),
		})
		cancelCache()

		if err != nil {
			logger.Warn("Server", "[JobID: %s | StepID: %s] Cache bypass query failed: %v. Falling back to standard step execution.", jobID, req.StepId, err)
		} else if cacheResp.Hit {
			validCacheHit := true
			if req.NodeConfig != nil && req.NodeConfig.ExpectedSchemaStruct != nil {
				compiler := jsonschema.NewCompiler()
				schemaBytes, _ := req.NodeConfig.ExpectedSchemaStruct.MarshalJSON()
				if err := compiler.AddResource("schema.json", bytes.NewReader(schemaBytes)); err == nil {
					if schema, err := compiler.Compile("schema.json"); err == nil {
						var v any
						if err := json.Unmarshal([]byte(cacheResp.ExtractedPayload), &v); err == nil {
							if err := schema.Validate(v); err != nil {
								logger.Warn("Server", "[JobID: %s | StepID: %s] CACHE HIT payload failed schema validation: %v. Treating as cache miss.", jobID, req.StepId, err)
								validCacheHit = false
							}
						} else {
							logger.Warn("Server", "[JobID: %s | StepID: %s] CACHE HIT payload is not valid JSON. Treating as cache miss.", jobID, req.StepId)
							validCacheHit = false
						}
					}
				}
			}

			if validCacheHit {
				logger.Success("Server", "[JobID: %s | StepID: %s] CACHE HIT! Confidence: %.2f (Threshold: %.3f). Bypassing step execution via cache hit.", jobID, req.StepId, cacheResp.Confidence, req.Cache.Confidence)
				if cacheResp.ExtractedPayload != "" {
					var outputMap map[string]any
					if err := json.Unmarshal([]byte(cacheResp.ExtractedPayload), &outputMap); err == nil {
						if parsedStruct, err := structpb.NewStruct(outputMap); err == nil {
							req.NodeOutput = parsedStruct
						}
					}
				}
				err := s.fireCallback(execCtx, req, &engine.StepResult{Request: req, NodeOutput: req.NodeOutput})
				if err != nil {
					return fmt.Errorf("failed to fire callback for cache hit: %w", err)
				}
				return nil
			}
		}

		logger.Info("Server", "[JobID: %s | StepID: %s] CACHE MISS. Proceeding with standard step execution.", jobID, req.StepId)
	}

	// Save request to KV store for async hydration (with deduplication check)
	reqBytes, marshalErr := proto.Marshal(req)
	if marshalErr == nil && s.kvStore != nil {
		if _, err := s.kvStore.Get(execCtx, jobID); err == nil {
			logger.Info("Server", "[JobID: %s | StepID: %s] Duplicate message received for job already waiting in KV store. Ignoring.", jobID, req.StepId)
			return nil
		}
		var putErr error
		for attempt := 1; attempt <= 3; attempt++ {
			if _, putErr = s.kvStore.Put(execCtx, jobID, reqBytes); putErr == nil {
				break
			}
			if attempt < 3 {
				logger.Warn("Server", "[JobID: %s] Failed to put job in KV store (attempt %d). Retrying in 250ms... Error: %v", jobID, attempt, putErr)
				time.Sleep(250 * time.Millisecond)
			}
		}
		if putErr != nil {
			logger.Error("Server", "[JobID: %s] Failed to put job in KV store after 3 attempts: %v", jobID, putErr)
			return fmt.Errorf("failed to put job in KV store: %w", putErr)
		}
	}

	logger.Info("System", "Executing step %s for job %s", req.NodeConfig.NodeName, jobID)

	res, err := s.eng.ExecuteNode(execCtx, req)

	if err != nil {
		if errors.Is(err, engine.ErrAsyncPending) {
			logger.Info("System", "Workflow step %s is pending async execution via JetStream queue", req.NodeConfig.NodeName)
			return nil
		}
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
			s.handleCallbackError(execCtx, err.Error(), req)

			// We MUST fire the Job Suspended webhook here explicitly
			var errContract *pb.ErrorContract
			if res != nil {
				errContract = res.ErrorContract
			}
			evt := &events.WebhookEvent{
				Request:       req,
				ErrorContract: errContract,
				IsSuspended:   true,
			}
			_ = s.pub.PublishWebhook(execCtx, evt)

			return nil
		}

		s.handleCallbackError(execCtx, err.Error(), req)
		return s.fireCallback(execCtx, req, res)
	}

	// This block runs if executeGenericStep somehow returns success synchronously (e.g. mock or fallback).
	s.handleCallbackError(execCtx, "", req)
	err = s.fireCallback(execCtx, req, res)
	if err != nil {
		return fmt.Errorf("failed to fire callback for successful workflow: %w", err)
	}
	return nil
}

func (s *OrchestratorServer) fireJobSuspendedCallback(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest, res *engine.StepResult) error {
	cbAddress := req.CallbackAddress
	if cbAddress == "" {
		return nil
	}
	var errContract *pb.ErrorContract
	if res != nil {
		errContract = res.ErrorContract
	}

	evt := &events.WebhookEvent{
		Request:       req,
		ErrorContract: errContract,
		IsSuspended:   true,
	}
	err := s.pub.PublishWebhook(ctx, evt)
	if err != nil {
		return fmt.Errorf("failed to publish webhook event: %w", err)
	}
	return nil
}

func (s *OrchestratorServer) fireCallback(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest, res *engine.StepResult) error {
	cbAddress := req.CallbackAddress
	if cbAddress == "" {
		logger.Info("System", "No callback address provided, skipping callback")
		return nil
	}

	var errContract *pb.ErrorContract
	if res != nil {
		errContract = res.ErrorContract
	}

	evt := &events.WebhookEvent{
		Request:       req,
		ErrorContract: errContract,
	}

	err := s.pub.PublishWebhook(ctx, evt)
	if err != nil {
		return fmt.Errorf("failed to publish webhook event: %w", err)
	}

	return nil
}

func (s *OrchestratorServer) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
	if req.GetIdentity() == nil || req.GetIdentity().GetJobId() == "" {
		return nil, fmt.Errorf("invalid request: missing Identity or JobId")
	}
	jobID := req.GetIdentity().GetJobId()

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

	stepReq := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(kvEntry.Value(), stepReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal saved request: %w", err)
	}

	logger.Info("System", "Resuming job %s after human correction", jobID)

	// Merge the corrected JSON into the original state context
	if req.GetCorrected() != nil {
		stepReq.NodeOutput = req.GetCorrected()
	}

	// Immediately fire webhook callback instead of publishing to JetStream priority queue
	err = s.fireCallback(ctx, stepReq, &engine.StepResult{Request: stepReq, NodeOutput: stepReq.NodeOutput})
	if err != nil {
		logger.Error("System", "Failed to fire callback for resumed job: %v", err)
		return nil, fmt.Errorf("failed to fire callback for resumed job: %w", err)
	}

	// Delete from KV store ONLY after successful callback (Idempotency fix)
	if deleteErr := s.kvStore.Delete(ctx, jobID); deleteErr != nil {
		logger.Warn("System", "Failed to delete KV entry for job %s: %v", jobID, deleteErr)
	}

	return &pb.ResumeJobResponse{
		Success: true,
		Message: "Job resumed successfully",
	}, nil
}

func (s *OrchestratorServer) handleCallbackError(ctx context.Context, errMsg string, req *pb.ExecuteWorkflowNodeRequest) {
	parser := workflowparser.NewNodeRequestParser(req)
	jobID := parser.GetIdentity().GetJobId()
	logger.Info("System", "Job %s complete or failed. Teared down cache.", jobID)

	if s.traceClient != nil {
		go func() {
			status := StatusCompleted
			if errMsg != "" {
				status = StatusFailed
			}
			ctxTrace, cancelTrace := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancelTrace()
			_, _ = s.traceClient.SaveJobState(ctxTrace, &pb.SaveJobStateRequest{
				Identity: parser.GetIdentity(),
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

	req := evt.Request
	if req == nil {
		logger.Error("System", "Webhook event missing Request payload")
		return nil // Invalid payload, don't retry
	}

	cbAddress := req.CallbackAddress
	if cbAddress == "" {
		return nil
	}

	s.webhookConnMu.RLock()
	conn, ok := s.webhookConns[cbAddress]
	s.webhookConnMu.RUnlock()

	if !ok {
		s.webhookConnMu.Lock()
		conn, ok = s.webhookConns[cbAddress]
		if !ok {
			maxMsgSizeMb := s.grpcMaxMessageSizeMb
			var err error
			conn, err = grpc.NewClient(cbAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
			if err != nil {
				s.webhookConnMu.Unlock()
				return fmt.Errorf("failed to dial callback address %s: %w", cbAddress, err)
			}
			s.webhookConns[cbAddress] = conn
		}
		s.webhookConnMu.Unlock()
	}

	client := pb.NewOrchestratorCallbackServiceClient(conn)

	timeoutSecs := s.grpcTimeoutSecs
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	req.NodeOutput = evt.Request.NodeOutput
	req.ErrorContract = evt.ErrorContract

	var err error
	if evt.IsSuspended {
		req.Success = false
		_, err = client.OnJobSuspended(ctx, req)
	} else if req.ErrorContract != nil {
		req.Success = false
		_, err = client.OnJobFailed(ctx, req)
	} else {
		req.Success = true
		var stream pb.OrchestratorCallbackService_OnNodeExecutionCompleteClient
		stream, err = client.OnNodeExecutionComplete(ctx)
		if err == nil {
			err = stream.Send(req)
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
