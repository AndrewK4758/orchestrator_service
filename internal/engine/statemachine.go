package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AndrewK4758/shared_utils/envutil"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"orchestrator_service/internal/events"


	"github.com/AndrewK4758/shared_utils/jsonclean"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var ErrPendingHumanValidation = errors.New("pending human validation")
var ErrAsyncPending = errors.New("async execution pending")

type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
	PublishCacheStore(ctx context.Context, data []byte) error
	PublishWebhook(ctx context.Context, event *events.WebhookEvent) error

}



type StepResult struct {
	Request          *pb.ExecuteWorkflowNodeRequest
	NodeOutputJson   string
	ErrorContract    *pb.ErrorContract
	IsTransientError bool
}

type Engine struct {
	maxInferenceRetries int32
	publishMaxRetries   int
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

func NewEngine(ctx context.Context, cacheClient pb.SemanticCacheServiceClient, pub TracePublisher, kvStore jetstream.KeyValue) (*Engine, error) {
	natsURL := envutil.MustGetString("NATS_URL")
	m := envutil.MustGetInt("MAX_INFERENCE_RETRIES")
	pm := envutil.MustGetInt("PUBLISH_MAX_RETRIES")

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
		cacheClient:         cacheClient,
		pub:                 pub,
		nc:                  nc,
		js:                  js,
		clientID:            clientID,
		kvStore:             kvStore,
		maxInferenceRetries: int32(m),
		publishMaxRetries:   pm,
	}

	setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	streamName := "ORCHESTRATOR_RESPONSES"
	_, err = js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"orchestrator.responses.>", "orchestrator.response.>"},
		MaxAge:   1 * time.Hour,
		MaxBytes: 1024 * 1024 * 1024,
		MaxMsgs:  100000,
		Discard:  jetstream.DiscardOld,
	})
	if err != nil {
		logger.Warn("System", "Failed to create or update ORCHESTRATOR_RESPONSES stream: %v", err)
		return nil, fmt.Errorf("failed to create or update ORCHESTRATOR_RESPONSES stream: %w", err)
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

	if req.Identity == nil || req.Identity.TenantId == "" || req.Identity.AppId == "" || req.Identity.JobId == "" {
		logger.Error("Engine", "Identity is missing from KV request for job %s", jobID)
		msg.Ack()
		return
	}
	t, a, j := req.Identity.TenantId, req.Identity.AppId, req.Identity.JobId
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
		errContract = &pb.ErrorContract{Code: "400", Message: "PoisonPill: Failed to unmarshal protobuf: " + err.Error()}
	} else {
		if !resp.Success {
			errContract = resp.GetErrorContract()
			logger.Warn("Engine", "Worker returned error asynchronously: %v", errContract)
		} else {
			rawJson = jsonclean.CleanJSON(resp.GetActionResultJson())
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
						maxRetries := e.maxInferenceRetries

						if req.InferenceRetryCount < maxRetries {
							logger.Warn("Engine", "JSON schema validation failed, retrying (count: %d): %v", req.InferenceRetryCount, err)
							req.NodeConfig.SystemInstructions += "\n[VALIDATION FAILED]: " + err.Error()

							if reqBytes, err := proto.Marshal(req); err == nil {
								_, _ = e.kvStore.Put(context.Background(), jobID, reqBytes)
							}
							_, _ = e.executeGenericStep(context.Background(), req)
							msg.Ack()
							return
						}

						logger.Error("Engine", "Validation retry exhausted: %v", err)
						errContract = &pb.ErrorContract{Code: "400", Message: "ValidationRetryExhausted: " + err.Error()}
						resp.Success = false
					}
				}
			}
		}
	}

	if rawJson != "" {
		req.NodeOutputJson = rawJson
	}
	eventType := events.EventActionCompleted
	if errContract != nil {
		eventType = events.EventActionFailed
	}

	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	reqSnapshotBytes, _ := marshaler.Marshal(req)

	_ = e.pub.PublishTrace(context.Background(), events.TraceabilityEvent{
		EventID:             uuid.New().String(),
		EventType:           eventType,
		Timestamp:           time.Now(),
		Identity:            req.Identity,
		JobNameID:           req.GetJobNameId(),
		JobAction:           req.NodeConfig.GetNodeName(),
		ErrorContract:       errContract,
		NodeOutputJson:      req.NodeOutputJson,
		RequestSnapshotJson: string(reqSnapshotBytes),
	})

	if req.CallbackAddress != "" {
		evt := &events.WebhookEvent{
			Request:       req,
			ErrorContract: errContract,
			IsSuspended:   errContract != nil && strings.HasPrefix(errContract.Message, "ValidationRetryExhausted"),
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

func (e *Engine) ExecuteWorkflow(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*StepResult, error) {
	if req == nil || req.NodeConfig == nil {
		return nil, fmt.Errorf("invalid request")
	}

	lastStepName := req.NodeConfig.NodeName

	_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:   uuid.New().String(),
		EventType: events.EventActionStarted,
		Timestamp: time.Now(),
		Identity:  req.Identity,
		JobNameID: req.GetJobNameId(),
		JobAction: lastStepName,
	})

	res, err := e.executeGenericStep(ctx, req)
	if err != nil {
		if errors.Is(err, ErrAsyncPending) {
			return nil, err
		}
		if res == nil {
			res = &StepResult{Request: req}
		} else {
			res.Request = req
		}

		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:   uuid.New().String(),
			EventType: events.EventActionFailed,
			Timestamp: time.Now(),
			Identity:  req.Identity,
			JobNameID: req.GetJobNameId(),
			JobAction: lastStepName,
			ErrorContract: &pb.ErrorContract{
				Code:    "500",
				Message: err.Error(),
			},
		})

		return res, fmt.Errorf("failed to execute generic step: %w", err)
	}

	res.Request = req
	if res.NodeOutputJson == "" {
		res.NodeOutputJson = req.NodeOutputJson
	}

	_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:        uuid.New().String(),
		EventType:      events.EventActionCompleted,
		Timestamp:      time.Now(),
		Identity:       req.Identity,
		JobNameID:      req.GetJobNameId(),
		JobAction:      lastStepName,
		NodeOutputJson: res.NodeOutputJson,
	})

	return res, nil
}

func (e *Engine) executeGenericStep(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*StepResult, error) {
	if req.Identity == nil || req.Identity.TenantId == "" || req.Identity.AppId == "" || req.Identity.JobId == "" {
		return nil, fmt.Errorf("infrastructure identity is strictly required")
	}
	t, a, j := req.Identity.TenantId, req.Identity.AppId, req.Identity.JobId

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

	maxRetries := e.publishMaxRetries
	for i := range maxRetries {
		if e.js != nil {
			_, err = e.js.PublishMsg(ctx, nMsg, jetstream.WithMsgID(msgID))
		} else {
			err = nil
		}
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
