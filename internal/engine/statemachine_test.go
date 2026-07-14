package engine

import (
	"context"
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
			} else if strings.Contains(currentAction.Name, "Generate Plan") {
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
			} else if strings.Contains(currentAction.Name, "Generate Plan") {
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


func TestExecuteWorkflow_ClientPrimitiveOverride_AI(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "override_ai_job",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-ai-override",
		StepName:           "AI Override Step",
		CustomInstructions: "Run AI directly",
		ExpectedSchema:     `{"type": "object"}`,
		StepPrimitive:      pb.OrchestratorPrimitive_PRIMITIVE_AI_STEP,
	}

	planCalled := false
	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Generate Plan") {
				planCalled = true
				return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
			} else if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"ai_ran": true}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockCache := &MockCacheClient{}
	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err != nil {
		t.Fatalf("Engine failed processing step: %v", err)
	}

	if planCalled {
		t.Fatalf("Expected AI Planner to be completely bypassed, but PerformAction(Plan) was called.")
	}

	if !strings.Contains(resultJson, "ai_ran") {
		t.Errorf("Expected result JSON to contain 'ai_ran', got: %s", resultJson)
	}
}

func TestExecuteWorkflow_ClientPrimitiveOverride_HUMAN(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "override_human_job",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-human-override",
		StepName:           "Human Override Step",
		CustomInstructions: "Run Human directly",
		ExpectedSchema:     `{"type": "object"}`,
		StepPrimitive:      pb.OrchestratorPrimitive_PRIMITIVE_HUMAN_STEP,
	}

	planCalled := false
	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Generate Plan") {
				planCalled = true
			}
			// Just pass dummy responses
			return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "approved": true, "human_ran": true}`}, nil
		},
	}

	mockCache := &MockCacheClient{}
	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err == nil {
		t.Fatalf("Expected Engine to fail processing step with human validation error, got nil")
	}

	if !errors.Is(err, ErrPendingHumanValidation) {
		t.Fatalf("Expected ErrPendingHumanValidation, got: %v", err)
	}

	if planCalled {
		t.Fatalf("Expected AI Planner to be completely bypassed, but PerformAction(Plan) was called.")
	}
}

func TestExecuteWorkflow_LegacyRequireHumanReview(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	state := &pb.WorkflowContext{
		JobId:       "legacy_human_job",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-legacy-human",
		StepName:           "Legacy Human Step",
		CustomInstructions: "Legacy flag",
		ExpectedSchema:     `{"type": "object"}`,
		RequireHumanReview: true, // Legacy flag
		StepPrimitive:      pb.OrchestratorPrimitive_PRIMITIVE_UNSPECIFIED,
	}

	planCalled := false
	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Generate Plan") {
				planCalled = true
			}
			return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "approved": true, "legacy_human_ran": true}`}, nil
		},
	}

	mockCache := &MockCacheClient{}
	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err == nil {
		t.Fatalf("Expected Engine to fail processing step with human validation error, got nil")
	}

	if !errors.Is(err, ErrPendingHumanValidation) {
		t.Fatalf("Expected ErrPendingHumanValidation, got: %v", err)
	}

	if planCalled {
		t.Fatalf("Expected AI Planner to be bypassed because RequireHumanReview was true.")
	}
}
