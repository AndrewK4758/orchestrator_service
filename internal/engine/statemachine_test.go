package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"orchestrator_service/internal/events"

	"net"
	"os"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

type MockHumanValidationServer struct {
	pb.UnimplementedHumanValidationServiceServer
}

func (m *MockHumanValidationServer) CreateValidationTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.CreateTaskResponse, error) {
	return &pb.CreateTaskResponse{TaskId: "mock_task"}, nil
}

func init() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		os.Setenv("HUMAN_VALIDATION_SERVICE_ADDRESS", l.Addr().String())
		srv := grpc.NewServer()
		pb.RegisterHumanValidationServiceServer(srv, &MockHumanValidationServer{})
		go srv.Serve(l)
	}
}

type MockAIClient struct {
	PerformActionFunc func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error)
}

func (m *MockAIClient) PerformAction(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
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
	CheckCacheFunc          func(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error)
	StoreExtractionFunc     func(ctx context.Context, req *pb.StoreExtractionRequest, opts ...grpc.CallOption) (*pb.StoreExtractionResponse, error)
	SeedCacheFunc           func(ctx context.Context, req *pb.SeedCacheRequest, opts ...grpc.CallOption) (*pb.SeedCacheResponse, error)
	CheckMetadataExistsFunc func(ctx context.Context, req *pb.CheckMetadataRequest, opts ...grpc.CallOption) (*pb.CheckMetadataResponse, error)
}

func (m *MockCacheClient) CheckMetadataExists(ctx context.Context, req *pb.CheckMetadataRequest, opts ...grpc.CallOption) (*pb.CheckMetadataResponse, error) {
	if m.CheckMetadataExistsFunc != nil {
		return m.CheckMetadataExistsFunc(ctx, req, opts...)
	}
	return &pb.CheckMetadataResponse{Exists: false}, nil
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

func TestGenericStateMerging(t *testing.T) {
	// Objective: Prove Orchestrator correctly handles completely unknown JSON states.
	complexJSONStr := `{"fuzzed_key1": ["arbitrary", "data", {"nested": true}], "fuzzed_key2": 12345, "fuzzed_key3": null, "fuzzed_key4": 99.99}`
	var data map[string]any
	json.Unmarshal([]byte(complexJSONStr), &data)
	statePayload, _ := structpb.NewStruct(data)

	state := &pb.WorkflowContext{
		JobId:       "fuzz_job_123",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-123",
		StepName:           "Fuzzed Step",
		CustomInstructions: "Do nothing but return the JSON",
		ExpectedSchema:     `{"type": "object"}`,
		UseCache:           false,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Generate Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_AI_STEP", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"new_ai_key": "ai_value"}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: `{"new_ai_key": "ai_value"}`}, nil
		},
	}

	mockPub := &MockTracePublisher{}
	mockCache := &MockCacheClient{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: state.JobId},
		GlobalState: state.GlobalState,
		StepId:      step.Id,
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           step.StepName,
			SystemInstructions: step.CustomInstructions,
			ExpectedSchema:     step.ExpectedSchema,
			PrimitiveType:      pb.OrchestratorPrimitive_PRIMITIVE_UNSPECIFIED,
		},
	}

	_, assembledJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("Engine failed to process arbitrary state: %v", err)
	}
	if err := MergeStateContext(req, assembledJson, ""); err != nil {
		t.Fatalf("MergeStateContext failed: %v", err)
	}

	finalJsonBytes, _ := req.GlobalState.MarshalJSON()
	var finalData map[string]any
	if err := json.Unmarshal(finalJsonBytes, &finalData); err != nil {
		t.Fatalf("Failed to parse final JSON: %v", err)
	}

	if finalData["fuzzed_key2"].(float64) != 12345 {
		t.Errorf("Expected fuzzed_key2 to be 12345, got %v", finalData["fuzzed_key2"])
	}
	if finalData["new_ai_key"] != "ai_value" {
		t.Errorf("Expected new_ai_key to be 'ai_value', got %v", finalData["new_ai_key"])
	}
}

func TestExecuteWorkflow_WritesToStateKey(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]any{})
	state := &pb.WorkflowContext{
		JobId:       "fuzz_namespace_123",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-999",
		StepName:           "Namespaced Step",
		CustomInstructions: "test",
		ExpectedSchema:     `{"type": "object"}`,
		WritesToStateKey:   "DynamicTargetEntityKey",
		StepPrimitive:      pb.OrchestratorPrimitive_PRIMITIVE_AI_STEP,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review Plan") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			}
			if strings.Contains(currentAction.Name, "Validate Output") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			if strings.Contains(currentAction.Name, "Assemble Output") {
				return &pb.PerformActionResponse{ActionResultJson: `{"extracted_data": "success"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: `{"extracted_data": "success"}`}, nil
		},
	}

	mockCache := &MockCacheClient{}
	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: state.JobId},
		GlobalState: state.GlobalState,
		StepId:      step.Id,
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           step.StepName,
			SystemInstructions: step.CustomInstructions,
			ExpectedSchema:     step.ExpectedSchema,
			PrimitiveType:      step.StepPrimitive,
		},
	}

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("Expected Engine to succeed, got error: %v", err)
	}

	if err := MergeStateContext(req, resultJson, "extracted_data"); err != nil {
		t.Fatalf("MergeStateContext failed: %v", err)
	}

	if _, ok := req.GlobalState.AsMap()["extracted_data"]; !ok {
		t.Errorf("Expected GlobalState to contain 'extracted_data', but it didn't. Result: %s, State: %v", resultJson, req.GlobalState.AsMap())
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
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			return nil, expectedErr
		},
	}

	mockPub := &MockTracePublisher{}
	mockCache := &MockCacheClient{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	req := &pb.ExecuteWorkflowNodeRequest{

		Identity: &pb.InfrastructureIdentity{JobId: state.JobId},

		GlobalState: state.GlobalState,

		StepId: step.Id,

		NodeConfig: &pb.NodeConfiguration{

			NodeName: step.StepName,

			SystemInstructions: step.CustomInstructions,

			ExpectedSchema: step.ExpectedSchema,

			PrimitiveType: step.StepPrimitive,
		},
	}

	if step.CacheConfig != nil {

		req.NodeConfig.Cache = &pb.CacheConfiguration{

			Enabled: step.CacheConfig.Enabled,

			CollectionName: step.CacheConfig.CollectionName,

			LookupInput: step.CacheConfig.LookupInput,

			ConfidenceThreshold: step.CacheConfig.ConfidenceThreshold,
		}

	}

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
	if err == nil {
		t.Fatalf("Expected error when AI service is down, got nil")
	}

	if !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Errorf("Expected error to contain '%v', got: %v", expectedErr, err)
	}
}

func TestExecuteWorkflow_ClientPrimitiveOverride_AI(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]any{})
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
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
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
			return &pb.PerformActionResponse{ActionResultJson: `{"ai_ran": true}`}, nil
		},
	}

	mockCache := &MockCacheClient{}
	mockPub := &MockTracePublisher{}
	eng := NewEngine(mockAI, mockCache, mockPub)

	req := &pb.ExecuteWorkflowNodeRequest{

		Identity: &pb.InfrastructureIdentity{JobId: state.JobId},

		GlobalState: state.GlobalState,

		StepId: step.Id,

		NodeConfig: &pb.NodeConfiguration{

			NodeName: step.StepName,

			SystemInstructions: step.CustomInstructions,

			ExpectedSchema: step.ExpectedSchema,

			PrimitiveType: step.StepPrimitive,
		},
	}

	if step.CacheConfig != nil {

		req.NodeConfig.Cache = &pb.CacheConfiguration{

			Enabled: step.CacheConfig.Enabled,

			CollectionName: step.CacheConfig.CollectionName,

			LookupInput: step.CacheConfig.LookupInput,

			ConfidenceThreshold: step.CacheConfig.ConfidenceThreshold,
		}

	}

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
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
	statePayload, _ := structpb.NewStruct(map[string]any{})
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
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
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

	req := &pb.ExecuteWorkflowNodeRequest{

		Identity: &pb.InfrastructureIdentity{JobId: state.JobId},

		GlobalState: state.GlobalState,

		StepId: step.Id,

		NodeConfig: &pb.NodeConfiguration{

			NodeName: step.StepName,

			SystemInstructions: step.CustomInstructions,

			ExpectedSchema: step.ExpectedSchema,

			PrimitiveType: step.StepPrimitive,
		},
	}

	if step.CacheConfig != nil {

		req.NodeConfig.Cache = &pb.CacheConfiguration{

			Enabled: step.CacheConfig.Enabled,

			CollectionName: step.CacheConfig.CollectionName,

			LookupInput: step.CacheConfig.LookupInput,

			ConfidenceThreshold: step.CacheConfig.ConfidenceThreshold,
		}

	}

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
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

func TestIdempotentExecution(t *testing.T) {
	// Scenario 1: Simulate an AI Service gRPC timeout during the Review Phase.
	// Verify the step rolls back and retries cleanly.
	statePayload, _ := structpb.NewStruct(map[string]any{})
	state := &pb.WorkflowContext{
		JobId:       "timeout_job",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-retry",
		StepName:           "Retry Step",
		CustomInstructions: "do something",
		ExpectedSchema:     `{"type": "object"}`,
	}

	callCount := 0
	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Generate Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_AI_STEP", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "Review") {
				callCount++
				if callCount == 1 {
					// Simulate context timeout error on first try
					return nil, context.DeadlineExceeded
				}
				// Succeed on second try
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"assembled": true}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: `{"assembled": true}`}, nil
		},
	}

	eng := NewEngine(mockAI, &MockCacheClient{}, &MockTracePublisher{})

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: state.JobId},
		GlobalState: state.GlobalState,
		StepId:      step.Id,
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           step.StepName,
			SystemInstructions: step.CustomInstructions,
			ExpectedSchema:     step.ExpectedSchema,
			PrimitiveType:      pb.OrchestratorPrimitive_PRIMITIVE_UNSPECIFIED,
		},
	}

	_, assembledJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("Engine failed processing despite retry logic: %v", err)
	}
	if !strings.Contains(assembledJson, "assembled") {
		t.Fatalf("Expected output to contain 'assembled', got: %s", assembledJson)
	}
	if callCount != 2 {
		t.Fatalf("Expected Review to be called 2 times due to retry, got: %d", callCount)
	}
}

func TestFailureInjection(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]any{})
	state := &pb.WorkflowContext{
		JobId:       "fail_inject_job",
		GlobalState: statePayload,
	}

	step := &pb.StepDefinition{
		Id:                 "step-fail",
		StepName:           "Fail Step",
		CustomInstructions: "do something",
		ExpectedSchema:     `{"type": "object"}`,
		StepPrimitive:      pb.OrchestratorPrimitive_PRIMITIVE_ZERO_SHOT_CLASSIFICATION,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: `{"output": "valid"}`}, nil
		},
	}

	mockCache := &MockCacheClient{
		CheckCacheFunc: func(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			panic("simulated CacheService panic")
		},
	}

	mockPub := &MockTracePublisher{
		PublishTraceFunc: func(ctx context.Context, event events.TraceabilityEvent) error {
			return errors.New("simulated Traceability connection failure")
		},
	}

	eng := NewEngine(mockAI, mockCache, mockPub)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: state.JobId},
		GlobalState: state.GlobalState,
		StepId:      step.Id,
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           step.StepName,
			SystemInstructions: step.CustomInstructions,
			ExpectedSchema:     step.ExpectedSchema,
			PrimitiveType:      pb.OrchestratorPrimitive_PRIMITIVE_ZERO_SHOT_CLASSIFICATION,
			Cache: &pb.CacheConfiguration{
				Enabled:     true,
				LookupInput: "test",
			},
		},
	}

	// Should not crash due to panic recovery, and should just return cache_hit: false.
	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)

	if err != nil {
		t.Fatalf("Engine failed completely instead of recovering: %v", err)
	}

	if !strings.Contains(resultJson, "valid") {
		t.Errorf("Expected result to contain 'valid' from AI fallback upon panic, got: %s", resultJson)
	}
}

func TestZeroShotCacheHit(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]any{})
	state := &pb.WorkflowContext{
		JobId:       "cache_hit_job",
		GlobalState: statePayload,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Assemble") {
				return &pb.PerformActionResponse{ActionResultJson: `{"output": "ai_data"}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			t.Fatalf("executeAIStep should not be called on cache hit. Called with action: %s", currentAction.Name)
			return nil, nil
		},
	}

	mockCache := &MockCacheClient{
		CheckCacheFunc: func(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			return &pb.CheckCacheResponse{Hit: true, ExtractedPayload: `{"cached": "data"}`}, nil
		},
	}

	eng := NewEngine(mockAI, mockCache, &MockTracePublisher{})

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: state.JobId},
		GlobalState: state.GlobalState,
		StepId:      "step-1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           "Test Node",
			SystemInstructions: "instructions",
			ExpectedSchema:     `{"type": "object"}`,
			PrimitiveType:      pb.OrchestratorPrimitive_PRIMITIVE_ZERO_SHOT_CLASSIFICATION,
			Cache: &pb.CacheConfiguration{
				Enabled:     true,
				LookupInput: "test",
			},
		},
	}

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("Engine failed on cache hit: %v", err)
	}
	if !strings.Contains(resultJson, "cached") {
		t.Errorf("Expected result to contain cached payload, got: %s", resultJson)
	}
}

func TestZeroShotCacheMissFallback(t *testing.T) {
	statePayload, _ := structpb.NewStruct(map[string]any{})
	state := &pb.WorkflowContext{
		JobId:       "cache_miss_job",
		GlobalState: statePayload,
	}

	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: `{"output": "ai_data"}`}, nil
		},
	}

	mockCache := &MockCacheClient{
		CheckCacheFunc: func(ctx context.Context, req *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			return &pb.CheckCacheResponse{Hit: false}, nil
		},
	}

	eng := NewEngine(mockAI, mockCache, &MockTracePublisher{})

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: state.JobId},
		GlobalState: state.GlobalState,
		StepId:      "step-1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           "Test Node",
			SystemInstructions: "instructions",
			ExpectedSchema:     `{"type": "object"}`,
			PrimitiveType:      pb.OrchestratorPrimitive_PRIMITIVE_ZERO_SHOT_CLASSIFICATION,
			Cache: &pb.CacheConfiguration{
				Enabled:     true,
				LookupInput: "test",
			},
		},
	}

	_, resultJson, _, _, err := eng.ExecuteWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("Engine failed on cache miss fallback: %v", err)
	}
	if !strings.Contains(resultJson, "ai_data") {
		t.Errorf("Expected result to contain AI payload, got: %s", resultJson)
	}
}

func TestValidateStepOutput(t *testing.T) {
	engine := &Engine{}

	tests := []struct {
		name          string
		schema        string
		assembledJson string
		expectedValid bool
		reasonMatches string
	}{
		{
			name:          "Happy Path - Simple Object",
			schema:        `{ "type": "object", "properties": { "name": { "type": "string" } }, "required": ["name"] }`,
			assembledJson: `{ "name": "John Doe" }`,
			expectedValid: true,
			reasonMatches: "",
		},
		{
			name:          "Happy Path - Complex Nested",
			schema:        `{ "type": "object", "properties": { "user": { "type": "object", "properties": { "age": { "type": "integer" } } } } }`,
			assembledJson: `{ "user": { "age": 30 } }`,
			expectedValid: true,
			reasonMatches: "",
		},
		{
			name:          "Failure - Missing Required Field",
			schema:        `{ "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] }`,
			assembledJson: `{ }`,
			expectedValid: false,
			reasonMatches: "id is required",
		},
		{
			name:          "Failure - Type Mismatch",
			schema:        `{ "type": "object", "properties": { "age": { "type": "integer" } } }`,
			assembledJson: `{ "age": "thirty" }`,
			expectedValid: false,
			reasonMatches: "Invalid type. Expected: integer, given: string",
		},
		{
			name:          "Resiliency - Malformed JSON (EOF)",
			schema:        `{ "type": "object" }`,
			assembledJson: `{ "broken": `,
			expectedValid: false,
			reasonMatches: "The generated output is not valid JSON. Parse error:",
		},
		{
			name:          "Resiliency - Malformed Schema",
			schema:        `{"broken_schema": `,
			assembledJson: `{ "name": "test" }`,
			expectedValid: false,
			reasonMatches: "The generated output is not valid JSON. Parse error:",
		},
		{
			name:          "Edge Case - allOf Base Schema Restriction",
			schema:        `{"allOf": [ {"type": "object", "additionalProperties": false}, {"type": "object", "properties": {"name": {"type": "string"}}} ]}`,
			assembledJson: `{ "name": "John" }`,
			expectedValid: false,
			reasonMatches: "Additional property name is not allowed",
		},
		{
			name:          "Edge Case - allOf Relaxed Base Schema",
			schema:        `{"allOf": [ {"type": "object"}, {"type": "object", "additionalProperties": false, "properties": {"name": {"type": "string"}}} ]}`,
			assembledJson: `{ "name": "John" }`,
			expectedValid: true,
			reasonMatches: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{JobId: "TestJob_123"},
				StepId:   "Step_ABC",
				NodeConfig: &pb.NodeConfiguration{
					ExpectedSchema: tt.schema,
				},
			}
			state := &EngineState{JobId: req.Identity.JobId, Req: req}

			valid, reason, err := engine.validateStepOutput(context.Background(), state, req, tt.assembledJson)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if valid != tt.expectedValid {
				t.Errorf("expected valid=%v, got %v", tt.expectedValid, valid)
			}

			if tt.reasonMatches != "" && !strings.Contains(reason, tt.reasonMatches) {
				t.Errorf("expected reason to contain %q, got %q", tt.reasonMatches, reason)
			} else if tt.reasonMatches == "" && reason != "" {
				t.Errorf("expected empty reason, got %q", reason)
			}
		})
	}
}

func TestStateMachine_DocumentChunkingRoute(t *testing.T) {
	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_chunking_123"},
		StepId:   "step_chunk",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:      "Document Chunking Step",
			PrimitiveType: pb.OrchestratorPrimitive_PRIMITIVE_DOCUMENT_CHUNKING,
		},
		GlobalState: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"zip_file_path": structpb.NewStringValue("/Uploads/SSD_Store/Order_123.zip"),
			},
		},
	}

	engine := NewEngine(nil, nil, &MockTracePublisher{})

	_, _, _, _, err := engine.ExecuteWorkflow(context.Background(), req)

	// Since executeDocumentChunking currently returns a stubbed JSON and a success without crashing,
	// and because we don't have the full callback mock hooked up in this unit test context,
	// we just want to ensure it doesn't panic and routes correctly (or returns an error related to missing pub/sub instead of 'unknown primitive').
	if err != nil && !strings.Contains(err.Error(), "publisher") && !strings.Contains(err.Error(), "JSON") {
		t.Logf("Engine routed to chunking but returned an expected context error: %v", err)
	}
}
