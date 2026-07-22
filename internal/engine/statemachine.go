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
	"orchestrator_service/internal/logger"

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

func NewEngine(aiClient AIClient, cacheClient pb.SemanticCacheServiceClient, pub TracePublisher) *Engine {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Error("System", "Failed to connect to NATS in Engine: %v", err)
	}

	var js jetstream.JetStream
	if nc != nil {
		js, err = jetstream.New(nc)
		if err != nil {
			logger.Error("System", "Failed to init JetStream in Engine: %v", err)
		}
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

	if js != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		streamName := "WORKER_RESPONSES"
		_, err = js.Stream(ctx, streamName)
		if err != nil {
			_, err = js.CreateStream(ctx, jetstream.StreamConfig{
				Name:     streamName,
				Subjects: []string{"worker.response.>"},
			})
			if err != nil {
				logger.Warn("System", "Failed to create WORKER_RESPONSES stream: %v", err)
			}
		}

		subSubject := fmt.Sprintf("worker.response.%s.>", clientID)
		cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: subSubject,
		})
		if err != nil {
			logger.Error("System", "Failed to create consumer: %v", err)
		} else {
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
			} else {
				e.sub = cc
			}
		}
	}

	return e
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
		var parsed map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
			return fmt.Errorf("malformed JSON in global state merge: %w", err)
		}

		if req.GlobalState == nil {
			req.GlobalState, _ = structpb.NewStruct(make(map[string]any))
		}

		m := req.GlobalState.AsMap()
		if targetKey != "" {
			if existing, ok := m[targetKey].(map[string]any); ok {
				deepMerge(existing, parsed)
				m[targetKey] = existing
			} else {
				m[targetKey] = parsed
			}
		} else {
			deepMerge(m, parsed)
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

func (e *Engine) ExecuteWorkflow(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (string, string, string, string, error) {
	if req == nil || req.NodeConfig == nil {
		return "", "", "", "", fmt.Errorf("invalid request")
	}

	lastStepName := req.NodeConfig.NodeName
	lastStepId := req.StepId

	assembledJson, err := e.executeGenericStep(ctx, req)
	if err != nil {
		return "", "", lastStepName, lastStepId, fmt.Errorf("failed to execute generic step: %w", err)
	}

	var finalJSON []byte
	if req.GlobalState != nil {
		finalJSON, _ = req.GlobalState.MarshalJSON()
	}

	return string(finalJSON), assembledJson, lastStepName, lastStepId, nil
}

func (e *Engine) executeGenericStep(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (string, error) {
	subject := req.NodeConfig.TargetWorkerSubject
	if subject == "" {
		return "", fmt.Errorf("target worker subject is empty")
	}

	maxRetriesStr := os.Getenv("MAX_INFERENCE_RETRIES")
	maxRetries := 3
	if maxRetriesStr != "" {
		if val, err := strconv.Atoi(maxRetriesStr); err == nil {
			maxRetries = val
		}
	}

	schemaStr := req.NodeConfig.ExpectedSchema
	compiler := jsonschema.NewCompiler()
	var schema *jsonschema.Schema
	if schemaStr != "" {
		if err := compiler.AddResource("schema.json", strings.NewReader(schemaStr)); err == nil {
			schema, _ = compiler.Compile("schema.json")
		}
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	replySubj := fmt.Sprintf("worker.response.%s.%s", e.clientID, uuid.New().String())
	ch := make(chan jetstream.Msg, 1)
	e.pending.Store(replySubj, ch)
	defer e.pending.Delete(replySubj)

	var lastErr error
	var rawJson string

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

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled during worker execution wait: %w", ctx.Err())
		case <-time.After(30 * time.Second):
			logger.Warn("Engine", "Timeout waiting for worker response on attempt %d", attempt)
			lastErr = fmt.Errorf("timeout waiting for worker response")
			continue
		case respMsg := <-ch:
			respMsg.Ack()

			var resp pb.PerformActionResponse
			if err := proto.Unmarshal(respMsg.Data(), &resp); err != nil {
				rawJson = cleanJSON(string(respMsg.Data()))
			} else {
				if !resp.Success {
					logger.Warn("Engine", "Worker returned error on attempt %d: %s", attempt, resp.ErrorMessage)
					lastErr = fmt.Errorf("worker error: %s", resp.ErrorMessage)
					continue
				}
				rawJson = cleanJSON(resp.GetActionResultJson())
			}

			if schema != nil {
				var doc any
				if err := json.Unmarshal([]byte(rawJson), &doc); err != nil {
					logger.Warn("Engine", "Failed to unmarshal worker JSON on attempt %d: %v", attempt, err)
					lastErr = err
					continue
				}

				if err := schema.Validate(doc); err != nil {
					logger.Warn("Engine", "JSON validation failed on attempt %d: %v", attempt, err)
					lastErr = err
					continue
				}
			}

			return rawJson, nil
		}
	}

	logger.Error("Engine", "Generic step failed after %d retries. Last error: %v", maxRetries, lastErr)
	return "", fmt.Errorf("%w: max inference retries reached: %v", ErrPendingHumanValidation, lastErr)
}
