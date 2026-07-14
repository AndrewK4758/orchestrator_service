package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"google.golang.org/grpc"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

type MockAIClient struct {
	PerformActionFunc func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error)
}

func (m *MockAIClient) PerformAction(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
	if m.PerformActionFunc != nil {
		return m.PerformActionFunc(ctx, workflowCtx, currentAction, inputBytes, inputText, modelChoice)
	}
	return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
}

type MockTracePublisher struct {
	PublishTraceFunc      func(ctx context.Context, event events.TraceabilityEvent) error
	PublishCacheStoreFunc func(ctx context.Context, data []byte) error
}

func (m *MockTracePublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	if m.PublishTraceFunc != nil {
		return m.PublishTraceFunc(ctx, event)
	}
	return nil
}

func (m *MockTracePublisher) PublishCacheStore(ctx context.Context, data []byte) error {
	if m.PublishCacheStoreFunc != nil {
		return m.PublishCacheStoreFunc(ctx, data)
	}
	return nil
}

type MockCacheClient struct {
	CheckCacheFunc      func(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error)
	StoreExtractionFunc func(ctx context.Context, req *pb.StoreExtractionRequest, opts ...grpc.CallOption) (*pb.StoreExtractionResponse, error)
	SeedCacheFunc       func(ctx context.Context, req *pb.SeedCacheRequest, opts ...grpc.CallOption) (*pb.SeedCacheResponse, error)
}

func (m *MockCacheClient) CheckCache(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
	if m.CheckCacheFunc != nil {
		return m.CheckCacheFunc(ctx, req, opts...)
	}
	return &pb.CheckCacheResponse{Hit: false}, nil
}

func (m *MockCacheClient) StoreExtraction(ctx context.Context, req *pb.StoreExtractionRequest, opts ...grpc.CallOption) (*pb.StoreExtractionResponse, error) {
	if m.StoreExtractionFunc != nil {
		return m.StoreExtractionFunc(ctx, req, opts...)
	}
	return &pb.StoreExtractionResponse{Success: true}, nil
}

func (m *MockCacheClient) SeedCache(ctx context.Context, req *pb.SeedCacheRequest, opts ...grpc.CallOption) (*pb.SeedCacheResponse, error) {
	if m.SeedCacheFunc != nil {
		return m.SeedCacheFunc(ctx, req, opts...)
	}
	return &pb.SeedCacheResponse{Success: true}, nil
}

func TestExecuteWorkflow_FuzzState(t *testing.T) {
	// 1. Generic State Testing (Crucial)
	// Pass completely arbitrary, complex JSON schemas (State) to the Orchestrator
	complexJSONStr := `{"fuzzed_key1": ["arbitrary", "data", {"nested": true}], "fuzzed_key2": 12345}`
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	json.Unmarshal([]byte(complexJSONStr), &statePayload.Fields)

	state := &pb.WorkflowContext{
		JobId:       "fuzz_job_123",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-123",
		StepName:           "Fuzzed Step",
		CustomInstructions: "Do nothing but return the JSON",
		ExpectedSchema:     `{"type": "object"}`,
		UseCache:           true,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_AI_STEP", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"output": "valid"}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Execute") {
				return &pb.PerformActionResponse{ActionResultJson: `{"result": "success"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockPub := &MockTracePublisher{}
	mockCache := &MockCacheClient{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed to process arbitrary state: %v", err)
	}

	if !strings.Contains(resultJson, "valid") {
		t.Errorf("Expected result JSON to contain 'valid', got: %s", resultJson)
	}
}

func TestExecuteWorkflow_WritesToStateKey(t *testing.T) {
	// Proves that when the Client passes WritesToStateKey, the Go Orchestrator correctly
	// nests the state output without hardcoding the key check anywhere in business logic.
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "fuzz_namespace_123",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-999",
		StepName:           "Namespaced Step",
		CustomInstructions: "Output anything",
		ExpectedSchema:     `{"type": "object"}`,
		WritesToStateKey:   "DynamicTargetEntityKey",
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"extracted_data": "success"}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Execute") {
				return &pb.PerformActionResponse{ActionResultJson: `{"result": "success"}`}, nil
			} else if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				return &pb.PerformActionResponse{ActionResultJson: `{"actions": [{"action_type": "PRIMITIVE_AI_STEP", "prompt": "test", "schema": {}}]}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockPub := &MockTracePublisher{}
	mockCache := &MockCacheClient{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed to process namespaced state: %v", err)
	}

	// Verify that the global state now contains the nested key
	m := state.GlobalState.AsMap()
	val, ok := m["DynamicTargetEntityKey"]
	if !ok {
		t.Fatalf("Expected GlobalState to contain 'DynamicTargetEntityKey', but it didn't. Result: %s, State: %v", resultJson, m)
	}

	nestedMap, ok := val.(map[string]interface{})
	if !ok || nestedMap["extracted_data"] != "success" {
		t.Errorf("Expected nested extracted_data == 'success', got: %v", val)
	}
}

func TestExecuteWorkflow_AIClientFailure(t *testing.T) {
	// 2. Idempotency & Resiliency Checks
	// Simulate AI service unavailability. Verify the system handles it gracefully.
	state := &pb.WorkflowContext{
		JobId:       "error_job_123",
		GlobalState: &structpb.Struct{},
	}

	step := &pb.StepDefinition{
		Id:                 "step-456",
		StepName:           "Error Step",
		CustomInstructions: "Fail this",
		ExpectedSchema:     `{"type": "object"}`,
	}

	expectedErr := errors.New("ai service unavailable")

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			return nil, expectedErr
		},
	}

	mockPub := &MockTracePublisher{}
	mockCache := &MockCacheClient{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err == nil {
		t.Fatalf("Expected error when AI service is down, got nil")
	}

	if !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected error to contain '%v', got: %v", expectedErr, err)
	}
}


func TestExecuteWorkflow_SemanticCacheCheck_Hit(t *testing.T) {
	// Proves that when Cache Check hits, the response is accurately merged into State.
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "cache_hit_job",
		GlobalState: statePayload,
	}
	step := &pb.StepDefinition{
		Id:                 "step-cache-1",
		StepName:           "Cache Step",
		CustomInstructions: "Check cache",
		ExpectedSchema:     `{"type": "object"}`,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_ZERO_SHOT_CLASSIFICATION", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"cache_hit": true, "confidence_score": 0.99, "extracted_payload": {"cached": "data"}}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockCache := &MockCacheClient{
		CheckCacheFunc: func(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			return &pb.CheckCacheResponse{
				Hit:              true,
				Confidence:       0.99,
				ExtractedPayload: `{"cached": "data"}`,
			}, nil
		},
	}

	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed to process cache hit state: %v", err)
	}

	if !strings.Contains(resultJson, "cache_hit\":true") && !strings.Contains(resultJson, "cache_hit\": true") {
		t.Errorf("Expected result JSON to contain cache_hit, got: %s", resultJson)
	}
	if !strings.Contains(resultJson, "cached\":\"data") && !strings.Contains(resultJson, "cached\": \"data") {
		t.Errorf("Expected result JSON to contain cached: data, got: %s", resultJson)
	}
}

func TestExecuteWorkflow_SemanticCacheStore_Resiliency(t *testing.T) {
	// Proves that when Cache Store throws an error (JetStream down), the Orchestrator degrades gracefully and does NOT fail the job.
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "cache_store_fail_job",
		GlobalState: statePayload,
	}
	step := &pb.StepDefinition{
		Id:                 "step-cache-store-1",
		StepName:           "Cache Store Step",
		CustomInstructions: "Store cache",
		ExpectedSchema:     `{"type": "object"}`,
		UseCache:           true,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_SEMANTIC_CACHE_STORE", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"cache_stored": true}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockCache := &MockCacheClient{}

	mockPub := &MockTracePublisher{
		PublishCacheStoreFunc: func(ctx context.Context, data []byte) error {
			return errors.New("jetstream timeout")
		},
	}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed the job because of a cache publish failure, expected graceful degradation! err: %v", err)
	}
	if !strings.Contains(resultJson, "cache_stored") {
		t.Errorf("Expected result JSON to contain cache_stored, got: %s", resultJson)
	}
}

func TestExecuteWorkflow_SemanticCacheStore_FireAndForget(t *testing.T) {
	// Happy path for Cache Writes. Ensure PublishCacheStore is called and it continues instantly.
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "cache_store_happy_job",
		GlobalState: statePayload,
	}
	step := &pb.StepDefinition{
		Id:                 "step-cache-store-2",
		StepName:           "Cache Store Step Happy",
		CustomInstructions: "Store cache",
		ExpectedSchema:     `{"type": "object"}`,
		UseCache:           true,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_SEMANTIC_CACHE_STORE", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"cache_stored": true}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockCache := &MockCacheClient{}

	publishCalled := false
	mockPub := &MockTracePublisher{
		PublishCacheStoreFunc: func(ctx context.Context, data []byte) error {
			publishCalled = true
			return nil
		},
	}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed the job during cache publish: %v", err)
	}
	if !publishCalled {
		t.Errorf("Expected PublishCacheStore to be called, but it was not")
	}
	if !strings.Contains(resultJson, "cache_stored") {
		t.Errorf("Expected result JSON to contain cache_stored, got: %s", resultJson)
	}
}

func TestExecuteWorkflow_StampedeLock_Release(t *testing.T) {
	// Idempotency / Concurrency validation.
	// Ensure that the Orchestrator successfully drops the pendingCacheJobs lock exactly after the JetStream publish step.
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "cache_store_stampede_job",
		GlobalState: statePayload,
	}
	step := &pb.StepDefinition{
		Id:                 "step-cache-store-3",
		StepName:           "Cache Store Step Stampede",
		CustomInstructions: "Store cache",
		ExpectedSchema:     `{"type": "object"}`,
		UseCache:           true,
		CacheKeyInput:      "some_identical_hash_input",
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_SEMANTIC_CACHE_STORE", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"cache_stored": true}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockCache := &MockCacheClient{}

	mockPub := &MockTracePublisher{
		PublishCacheStoreFunc: func(ctx context.Context, data []byte) error {
			return nil
		},
	}
	eng := NewEngine(mockAI, mockCache, mockPub)

	// Pre-load a lock to simulate another job currently reading/checking
	// The store should clear it.
	hashBytes := sha256.Sum256([]byte("some_identical_hash_input"))
	hashKey := hex.EncodeToString(hashBytes[:])
	
	lockCh := make(chan struct{})
	eng.pendingCacheJobs.Store(hashKey, lockCh)

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed the job: %v", err)
	}

	// Lock should be deleted!
	_, loaded := eng.pendingCacheJobs.Load(hashKey)
	if loaded {
		t.Errorf("Expected stampede lock to be released from pendingCacheJobs map, but it was still loaded")
	}

	// The channel should also be closed, we can test this by selecting from it.
	select {
	case <-lockCh:
		// Success, channel is closed
	default:
		t.Errorf("Expected lock channel to be closed by executeSemanticCacheStore")
	}
}
