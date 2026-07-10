package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

type MockAIClient struct {
	PerformActionFunc func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string) (*pb.PerformActionResponse, error)
}

func (m *MockAIClient) PerformAction(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string) (*pb.PerformActionResponse, error) {
	if m.PerformActionFunc != nil {
		return m.PerformActionFunc(ctx, workflowCtx, currentAction, inputBytes, inputText)
	}
	return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
}

type MockTracePublisher struct {
	PublishTraceFunc func(ctx context.Context, event events.TraceabilityEvent) error
}

func (m *MockTracePublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	if m.PublishTraceFunc != nil {
		return m.PublishTraceFunc(ctx, event)
	}
	return nil
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
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string) (*pb.PerformActionResponse, error) {
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
	eng := NewEngine(mockAI, mockPub)

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
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string) (*pb.PerformActionResponse, error) {
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
	eng := NewEngine(mockAI, mockPub)

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
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string) (*pb.PerformActionResponse, error) {
			return nil, expectedErr
		},
	}

	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockPub)

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
	if err == nil {
		t.Fatalf("Expected error when AI service is down, got nil")
	}

	if !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected error to contain '%v', got: %v", expectedErr, err)
	}
}
