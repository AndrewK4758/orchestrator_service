package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"orchestrator_service/internal/events"

	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrPendingHumanValidation = errors.New("pending human validation")
var ErrAsyncPending = errors.New("async execution pending")

type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
	PublishCacheStore(ctx context.Context, data []byte) error
	PublishWebhook(ctx context.Context, event *events.WebhookEvent) error
	PublishAction(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) error
}

type AIClient interface {
	PerformAction(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error)
}

type StepResult struct {
	FinalJSON        string
	OutputJSON       string
	StepName         string
	StepId           string
	ErrorContract    *pb.ErrorContract
	IsTransientError bool
}

type Engine struct {
	aiClient         AIClient
	cacheClient      pb.SemanticCacheServiceClient
	pub              TracePublisher
	pendingCacheJobs sync.Map
	chunkingClient   pb.DocumentProcessingServiceClient

	nc       *nats.Conn
	js       jetstream.JetStream
	clientID string
	sub      jetstream.ConsumeContext
	kvStore  jetstream.KeyValue
}

func NewEngine(ctx context.Context, aiClient AIClient, cacheClient pb.SemanticCacheServiceClient, pub TracePublisher, kvStore jetstream.KeyValue) (*Engine, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Error("System", "Failed to connect to NATS in Engine: %v", err)
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("System", "Failed to init JetStream in Engine: %v", err)
		return nil, fmt.Errorf("failed to init JetStream: %w", err)
	}

	clientID := uuid.New().String()
	e := &Engine{
		aiClient:    aiClient,
		cacheClient: cacheClient,
		pub:         pub,
		nc:          nc,
		js:          js,
		clientID:    clientID,
		kvStore:     kvStore,
	}

	setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	streamName := "ORCHESTRATOR_RESPONSES"
	_, err = js.Stream(setupCtx, streamName)
	if err != nil {
		_, err = js.CreateStream(setupCtx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{"orchestrator.response.>"},
		})
		if err != nil {
			logger.Warn("System", "Failed to create ORCHESTRATOR_RESPONSES stream: %v", err)
			return nil, fmt.Errorf("failed to create ORCHESTRATOR_RESPONSES stream: %w", err)
		}
	}

	subSubject := fmt.Sprintf("orchestrator.response.%s.>", clientID)
	cons, err := js.CreateOrUpdateConsumer(setupCtx, streamName, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subSubject,
	})
	if err != nil {
		logger.Error("System", "Failed to create consumer: %v", err)
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	cc, err := cons.Consume(e.ProcessJetStreamMessage, jetstream.PullMaxMessages(100))

	if err != nil {
		logger.Error("System", "Failed to consume JS messages: %v", err)
		return nil, fmt.Errorf("failed to consume JS messages: %w", err)
	}

	e.sub = cc

	return e, nil
}

func (e *Engine) ProcessJetStreamMessage(msg jetstream.Msg) {
	startTime := time.Now()
	meta, _ := msg.Metadata()
	var attempt uint64 = 1
	if meta != nil {
		attempt = meta.NumDelivered
	}

	replySubj := msg.Subject()

	parts := strings.Split(replySubj, ".")
	if len(parts) < 4 {
		msg.Ack()
		return
	}
	jobID := parts[3]

	if e.kvStore == nil {
		msg.Ack()
		return
	}

	entry, err := e.kvStore.Get(context.Background(), jobID)
	if err != nil {
		logger.Warn("Engine", "Job %s not found in KV store for async response, ignoring", jobID)
		msg.Ack()
		return
	}

	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(entry.Value(), req); err != nil {
		logger.Error("Engine", "Failed to unmarshal request from KV store for job %s: %v", jobID, err)
		msg.Ack()
		return
	}

	t, a, j := natsutil.ExtractIdentity(req.Identity)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "Orchestrator",
		Event:     "DEQUEUED",
		Stream:    "ORCHESTRATOR_RESPONSES",
		Subject:   replySubj,
		Consumer:  e.clientID,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(msg.Data()),
		Attempt:   attempt,
	})

	var resp pb.PerformActionResponse
	var rawJson string
	var errContract *pb.ErrorContract

	if err := proto.Unmarshal(msg.Data(), &resp); err != nil {
		rawJson = cleanJSON(string(msg.Data()))
	} else {
		if !resp.Success {
			errContract = resp.GetErrorContract()
			logger.Warn("Engine", "Worker returned error asynchronously: %v", errContract)
		} else {
			rawJson = cleanJSON(resp.GetActionResultJson())
		}
	}

	if errContract == nil && req.NodeConfig != nil && req.NodeConfig.ExpectedSchema != "" {
		compiler := jsonschema.NewCompiler()
		if err := compiler.AddResource("schema.json", strings.NewReader(req.NodeConfig.ExpectedSchema)); err == nil {
			if schema, err := compiler.Compile("schema.json"); err == nil {
				var v any
				if err := json.Unmarshal([]byte(rawJson), &v); err == nil {
					if err := schema.Validate(v); err != nil {
						req.InferenceRetryCount++
						maxRetries := int32(3)
						if envMax := os.Getenv("MAX_INFERENCE_RETRIES"); envMax != "" {
							if m, e := strconv.Atoi(envMax); e == nil {
								maxRetries = int32(m)
							}
						}

						if req.InferenceRetryCount < maxRetries {
							logger.Warn("Engine", "JSON schema validation failed, retrying (count: %d): %v", req.InferenceRetryCount, err)
							req.NodeConfig.SystemInstructions += "\n[VALIDATION FAILED]: " + err.Error()

							if reqBytes, err := proto.Marshal(req); err == nil {
								_, _ = e.kvStore.Put(context.Background(), jobID, reqBytes)
							}
							_ = e.pub.PublishAction(context.Background(), req)
							msg.Ack()
							return
						} else {
							logger.Error("Engine", "Validation retry exhausted: %v", err)
							errContract = &pb.ErrorContract{Code: "400", Message: "ValidationRetryExhausted: " + err.Error()}
							resp.Success = false
						}
					}
				}
			}
		}
	}

	_ = MergeStateContext(req, rawJson, "")

	eventType := events.EventActionCompleted
	var errMsg string
	if errContract != nil {
		eventType = events.EventActionFailed
		errMsg = errContract.GetMessage()
	}

	_ = e.pub.PublishTrace(context.Background(), events.TraceabilityEvent{
		EventID:   uuid.New().String(),
		EventType: eventType,
		Timestamp: time.Now(),
		TenantID:  req.Identity.GetTenantId(),
		AppID:     req.Identity.GetAppId(),
		JobID:     req.Identity.GetJobId(),
		JobAction: req.NodeConfig.NodeName,
		Error:     errMsg,
	})

	if req.CallbackAddress != "" {
		reqBytes, _ := proto.Marshal(req)
		var contractBytes []byte
		if errContract != nil {
			contractBytes, _ = proto.Marshal(errContract)
		}
		evt := &events.WebhookEvent{
			ReqBytes:           reqBytes,
			StepOutputJSON:     rawJson,
			ErrorContractBytes: contractBytes,
			JobId:              req.GetIdentity().GetJobId(),
			LastStepId:         req.StepId,
			IsSuspended:        errContract != nil && strings.HasPrefix(errContract.Message, "ValidationRetryExhausted"),
		}
		if err := e.pub.PublishWebhook(context.Background(), evt); err != nil {
			logger.Error("Engine", "Failed to publish webhook for job %s: %v", jobID, err)
			msg.NakWithDelay(1 * time.Minute)
			return
		}
	}

	_ = e.kvStore.Delete(context.Background(), jobID)
	msg.Ack()
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component:  "Orchestrator",
		Event:      "ACKNOWLEDGED",
		Stream:     "ORCHESTRATOR_RESPONSES",
		Subject:    replySubj,
		Consumer:   e.clientID,
		TenantID:   t,
		AppID:      a,
		JobID:      j,
		Attempt:    attempt,
		DurationMs: time.Since(startTime).Milliseconds(),
	})
}

func (e *Engine) SetChunkingClient(client pb.DocumentProcessingServiceClient) {
	e.chunkingClient = client
}

func (e *Engine) Close() {
	if e.sub != nil {
		e.sub.Stop()
	}
	if e.nc != nil {
		e.nc.Close()
	}
}

func MergeStateContext(req *pb.ExecuteWorkflowNodeRequest, jsonStr string, targetKey string) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}

	if jsonStr != "" {
		var parsed any
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
			return fmt.Errorf("malformed JSON in global state merge: %w", err)
		}

		if req.GlobalState == nil {
			req.GlobalState, _ = structpb.NewStruct(make(map[string]any))
		}

		m := req.GlobalState.AsMap()
		if targetKey != "" {
			if parsedMap, isParsedMap := parsed.(map[string]any); isParsedMap {
				if existing, ok := m[targetKey].(map[string]any); ok {
					deepMerge(existing, parsedMap)
					m[targetKey] = existing
				} else {
					m[targetKey] = parsedMap
				}
			} else {
				m[targetKey] = parsed
			}
		} else {
			if parsedMap, isParsedMap := parsed.(map[string]any); isParsedMap {
				deepMerge(m, parsedMap)
			} else {
				return fmt.Errorf("cannot merge non-object JSON into global root state without a targetKey")
			}
		}

		newState, err := structpb.NewStruct(m)
		if err != nil {
			return fmt.Errorf("failed to update struct for global state: %w", err)
		}
		req.GlobalState = newState
	}

	return nil
}

func deepMerge(dst map[string]any, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	for k, vSrc := range src {
		vDst, exists := dst[k]
		if exists {
			mapDst, okDst := vDst.(map[string]any)
			mapSrc, okSrc := vSrc.(map[string]any)
			if okDst && okSrc {
				deepMerge(mapDst, mapSrc)
				continue
			}
		}
		dst[k] = vSrc
	}
}

func cleanJSON(input string) string {
	input = strings.TrimSpace(input)
	if start := strings.Index(input, "```"); start != -1 {
		if end := strings.LastIndex(input, "```"); end > start+2 {
			content := input[start+3 : end]
			content = strings.TrimSpace(content)
			if strings.HasPrefix(strings.ToLower(content), "json") {
				content = strings.TrimSpace(content[4:])
			}
			input = content
		}
	}
	firstBrace := strings.Index(input, "{")
	firstBracket := strings.Index(input, "[")
	first := -1
	if firstBrace != -1 && firstBracket != -1 {
		first = min(firstBrace, firstBracket)
	} else if firstBrace != -1 {
		first = firstBrace
	} else if firstBracket != -1 {
		first = firstBracket
	}
	if first == -1 {
		return input
	}
	lastBrace := strings.LastIndex(input, "}")
	lastBracket := strings.LastIndex(input, "]")
	last := -1
	if lastBrace != -1 && lastBracket != -1 {
		last = max(lastBrace, lastBracket)
	} else if lastBrace != -1 {
		last = lastBrace
	} else if lastBracket != -1 {
		last = lastBracket
	}
	if last != -1 && last > first {
		return input[first : last+1]
	}
	return input
}

func (e *Engine) ExecuteWorkflow(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*StepResult, error) {
	if req == nil || req.NodeConfig == nil {
		return nil, fmt.Errorf("invalid request")
	}

	lastStepName := req.NodeConfig.NodeName
	lastStepId := req.StepId

	_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:   uuid.New().String(),
		EventType: events.EventActionStarted,
		Timestamp: time.Now(),
		TenantID:  req.Identity.GetTenantId(),
		AppID:     req.Identity.GetAppId(),
		JobID:     req.Identity.GetJobId(),
		JobAction: lastStepName,
	})

	res, err := e.executeGenericStep(ctx, req)
	if err != nil {
		if errors.Is(err, ErrAsyncPending) {
			return nil, err
		}
		if res == nil {
			res = &StepResult{}
		}
		res.StepName = lastStepName
		res.StepId = lastStepId

		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:   uuid.New().String(),
			EventType: events.EventActionFailed,
			Timestamp: time.Now(),
			TenantID:  req.Identity.GetTenantId(),
			AppID:     req.Identity.GetAppId(),
			JobID:     req.Identity.GetJobId(),
			JobAction: lastStepName,
			Error:     err.Error(),
		})

		return res, fmt.Errorf("failed to execute generic step: %w", err)
	}

	var finalJSON []byte
	if req.GlobalState != nil {
		finalJSON, _ = req.GlobalState.MarshalJSON()
	}

	res.FinalJSON = string(finalJSON)
	res.StepName = lastStepName
	res.StepId = lastStepId

	_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:   uuid.New().String(),
		EventType: events.EventActionCompleted,
		Timestamp: time.Now(),
		TenantID:  req.Identity.GetTenantId(),
		AppID:     req.Identity.GetAppId(),
		JobID:     req.Identity.GetJobId(),
		JobAction: lastStepName,
	})

	return res, nil
}

func (e *Engine) executeGenericStep(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*StepResult, error) {
	subject := req.NodeConfig.TargetWorkerSubject
	if subject == "" {
		return nil, fmt.Errorf("target worker subject is empty")
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	replySubj := fmt.Sprintf("orchestrator.response.%s.%s", e.clientID, req.Identity.GetJobId())

	msgID := fmt.Sprintf("%s-%s", req.Identity.GetJobId(), req.StepId)
	nMsg := nats.NewMsg(subject)
	nMsg.Reply = replySubj
	nMsg.Header = make(nats.Header)
	nMsg.Header.Set("Reply-Subject", replySubj)
	nMsg.Data = reqBytes

	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		_, err = e.js.PublishMsg(ctx, nMsg, jetstream.WithMsgID(msgID))
		if err == nil {
			break
		}
		logger.Warn("Engine", "Failed to publish to %s (attempt %d/%d): %v", subject, i+1, maxRetries, err)
		select {
		case <-ctx.Done():
			break
		case <-time.After(time.Duration(1<<i) * time.Second):
		}
	}

	t, a, j := natsutil.ExtractIdentity(req.Identity)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "Orchestrator",
		Event:     "ENQUEUED",
		Stream:    "WORKERS",
		Subject:   subject,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		MsgID:     msgID,
		SizeBytes: len(reqBytes),
		Error:     err,
	})
	if err != nil {
		logger.Error("Engine", "Failed to publish to %s: %v", subject, err)
		return nil, fmt.Errorf("failed to publish to worker: %w", err)
	}

	return nil, ErrAsyncPending
}
