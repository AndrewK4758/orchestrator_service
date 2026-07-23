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

	"orchestrator_service/internal/events"
	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/AndrewK4758/shared_utils/schemautil"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrPendingHumanValidation = errors.New("pending human validation")

type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
	PublishCacheStore(ctx context.Context, data []byte) error
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

type EngState struct {
	Req   *pb.ExecuteWorkflowNodeRequest
	JobId string
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
	pending  sync.Map
}

func NewEngine(ctx context.Context, aiClient AIClient, cacheClient pb.SemanticCacheServiceClient, pub TracePublisher) (*Engine, error) {
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

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		replySubj := msg.Subject()
		if ch, ok := e.pending.Load(replySubj); ok {
			select {
			case ch.(chan jetstream.Msg) <- msg:
			default:
				msg.Ack()
			}
		} else {
			msg.Ack()
		}
	}, jetstream.PullMaxMessages(100))
	if err != nil {
		logger.Error("System", "Failed to consume JS messages: %v", err)
		return nil, fmt.Errorf("failed to consume JS messages: %w", err)
	}

	e.sub = cc

	return e, nil
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

	maxRetriesStr := os.Getenv("MAX_INFERENCE_RETRIES")
	maxRetries := 3
	if maxRetriesStr != "" {
		if val, err := strconv.Atoi(maxRetriesStr); err == nil {
			maxRetries = val
		}
	}

	timeoutSecStr := os.Getenv("WORKER_RESPONSE_TIMEOUT_SEC")
	timeoutSec := 300
	if timeoutSecStr != "" {
		if val, err := strconv.Atoi(timeoutSecStr); err == nil {
			timeoutSec = val
		}
	}

	schemaStr := req.NodeConfig.ExpectedSchema
	compiler := schemautil.SetupRelaxedCompiler()
	var schema *jsonschema.Schema
	if schemaStr != "" {
		var err error
		if err = compiler.AddResource("schema.json", strings.NewReader(schemaStr)); err != nil {
			return nil, fmt.Errorf("invalid expected schema format: %w", err)
		}
		schema, err = compiler.Compile("schema.json")
		if err != nil {
			return nil, fmt.Errorf("failed to compile expected schema: %w", err)
		}
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	replySubj := fmt.Sprintf("orchestrator.response.%s.%s", e.clientID, uuid.New().String())
	ch := make(chan jetstream.Msg, 1)
	e.pending.Store(replySubj, ch)
	defer e.pending.Delete(replySubj)

	var lastErr error
	var rawJson string
	var lastContract *pb.ErrorContract
	var lastTransient bool

	for attempt := 0; attempt <= maxRetries; attempt++ {
		msgID := uuid.New().String()
		nMsg := nats.NewMsg(subject)
		nMsg.Reply = replySubj
		nMsg.Header = make(nats.Header)
		nMsg.Header.Set("Reply-Subject", replySubj)
		nMsg.Data = reqBytes

		_, err := e.js.PublishMsg(ctx, nMsg, jetstream.WithMsgID(msgID))
		if err != nil {
			logger.Error("Engine", "Failed to publish to %s on attempt %d: %v", subject, attempt, err)
			lastErr = err
			continue
		}

		timer := time.NewTimer(time.Duration(timeoutSec) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("context cancelled during worker execution wait: %w", ctx.Err())
		case <-timer.C:
			timer.Stop()
			logger.Warn("Engine", "Timeout waiting for worker response on attempt %d", attempt)
			lastErr = fmt.Errorf("timeout waiting for worker response")
			continue
		case respMsg := <-ch:
			timer.Stop()
			respMsg.Ack()

			var resp pb.PerformActionResponse
			if err := proto.Unmarshal(respMsg.Data(), &resp); err != nil {
				rawJson = cleanJSON(string(respMsg.Data()))
			} else {
				if !resp.Success {
					contract := resp.GetErrorContract()
					errMsg := "unknown worker error"
					if contract != nil && contract.GetMessage() != "" {
						errMsg = contract.GetMessage()
					}
					logger.Warn("Engine", "Worker returned error on attempt %d: %s", attempt, errMsg)
					
					lastErr = fmt.Errorf("worker error: %s", errMsg)
					lastContract = contract
					lastTransient = resp.GetIsTransientError()
					
					if !lastTransient {
						return &StepResult{
							ErrorContract:    lastContract,
							IsTransientError: lastTransient,
						}, fmt.Errorf("%w: deterministic fatal error from worker: %w", ErrPendingHumanValidation, lastErr)
					}
					continue
				}
				rawJson = cleanJSON(resp.GetActionResultJson())
			}

			if schema != nil {
				var doc any
				if err := json.Unmarshal([]byte(rawJson), &doc); err != nil {
					logger.Warn("Engine", "Failed to unmarshal worker JSON on attempt %d: %v", attempt, err)
					lastErr = fmt.Errorf("failed to unmarshal result: %w", err)
					time.Sleep(1 * time.Second)
					continue
				}

				if err := schema.Validate(doc); err != nil {
					logger.Warn("Engine", "JSON validation failed on attempt %d: %v", attempt, err)
					lastErr = fmt.Errorf("schema validation failed: %w", err)
					time.Sleep(1 * time.Second)
					continue
				}
			}

			return &StepResult{
				OutputJSON: rawJson,
			}, nil
		}
	}

	logger.Error("Engine", "Generic step failed after %d retries. Last error: %v", maxRetries, lastErr)
	return &StepResult{
		ErrorContract:    lastContract,
		IsTransientError: lastTransient,
	}, fmt.Errorf("%w: max inference retries reached: %w", ErrPendingHumanValidation, lastErr)
}
