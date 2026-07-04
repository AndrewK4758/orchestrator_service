package engine

import (
	"context"
	"net"
	"testing"

	pb "doc_processor/shared_protos"
	"google.golang.org/grpc"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/callback_client"
)

type mockDocumentProcessorServer struct {
	pb.UnimplementedDocumentProcessorServer
	extractedJson string
}

func (m *mockDocumentProcessorServer) ExtractDocumentData(ctx context.Context, req *pb.ExtractRequest) (*pb.ExtractResponse, error) {
	return &pb.ExtractResponse{
		ExtractedJson: m.extractedJson,
	}, nil
}

type mockSchemaRegistryServer struct {
	pb.UnimplementedSchemaRegistryServiceServer
	rulesText  string
	jsonSchema string
}

func (m *mockSchemaRegistryServer) GetRulesForClassification(ctx context.Context, req *pb.GetRulesRequest) (*pb.GetRulesResponse, error) {
	return &pb.GetRulesResponse{
		Success:    true,
		RulesText:  m.rulesText,
		JsonSchema: m.jsonSchema,
	}, nil
}

func TestEngine_ExecuteAIStep_Success(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockAI := &mockDocumentProcessorServer{
		extractedJson: `{"id": "valid_id", "status": "extracted"}`,
	}
	pb.RegisterDocumentProcessorServer(grpcServer, mockAI)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	aiClient, err := ai_client.NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create AI client: %v", err)
	}
	defer aiClient.Close()

	e := NewEngine(aiClient, nil, nil)

	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{
				ActionType:         "AI_EXTRACTION",
				StepName:           "ExtractionStep",
				CustomInstructions: "Extract status and id",
				ExpectedSchema:     `{"type": "object"}`,
				WritesToStateKey:   "extracted_data",
			},
		},
	}

	state := &ProcessingState{
		JobID:           "job_ai_success",
		RawTextContents: []string{"some raw content"},
		GlobalState:     make(map[string]interface{}),
	}

	handlers, _ := e.BuildRuntimeHandlers(nil)
	result, lastActionType, lastStepName, _, err := e.ExecuteWorkflow(context.Background(), state, req, handlers)
	if err != nil {
		t.Fatalf("Expected successful execution, got error: %v", err)
	}

	if lastActionType != "AI_EXTRACTION" || lastStepName != "ExtractionStep" {
		t.Errorf("Unexpected last step info: action=%s, name=%s", lastActionType, lastStepName)
	}

	if state.GlobalState["extracted_data"] == nil {
		t.Fatalf("Expected global state to have 'extracted_data'")
	}

	rawRes, ok := state.GlobalState["extracted_data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected extracted data to be unmarshaled as map, got: %T", state.GlobalState["extracted_data"])
	}

	if rawRes["id"] != "valid_id" || rawRes["status"] != "extracted" {
		t.Errorf("Unexpected values in global state: %+v", rawRes)
	}

	expectedResult := `{"extracted_data":{"id":"valid_id","status":"extracted"}}`
	if result != expectedResult {
		t.Errorf("expected final JSON %s, got %s", expectedResult, result)
	}
}

func TestEngine_ExecuteStep_DynamicRuleFetching(t *testing.T) {
	// Start Mock Schema Registry / Callback Server
	regListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer regListener.Close()

	grpcRegServer := grpc.NewServer()
	mockRegistry := &mockSchemaRegistryServer{
		rulesText:  "instructions fetched dynamically",
		jsonSchema: `{"type": "object", "properties": {"dynamic_id": {"type": "string"}}}`,
	}
	pb.RegisterSchemaRegistryServiceServer(grpcRegServer, mockRegistry)
	go func() {
		_ = grpcRegServer.Serve(regListener)
	}()
	defer grpcRegServer.Stop()

	// Start Mock AI Service
	aiListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer aiListener.Close()

	grpcAIServer := grpc.NewServer()
	mockAI := &mockDocumentProcessorServer{
		extractedJson: `{"dynamic_id": "fetched_123"}`,
	}
	pb.RegisterDocumentProcessorServer(grpcAIServer, mockAI)
	go func() {
		_ = grpcAIServer.Serve(aiListener)
	}()
	defer grpcAIServer.Stop()

	aiClient, err := ai_client.NewClient(aiListener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create AI client: %v", err)
	}
	defer aiClient.Close()

	// Instantiate Client Manager with Registry callback address
	cbManager := callback_client.NewClientManager()
	defer cbManager.Close()

	e := NewEngine(aiClient, nil, cbManager)

	// Step request with empty prompt/schema to trigger dynamic fetching
	req := &pb.SubmitStepRequest{
		CallbackAddress: regListener.Addr().String(),
		Steps: []*pb.WorkflowStep{
			{
				ActionType:       "AI_EXTRACTION",
				StepName:         "DynamicExtractionStep",
				WritesToStateKey: "dynamic_data",
			},
		},
	}

	state := &ProcessingState{
		JobID:           "job_dynamic_fetching",
		RawTextContents: []string{"some dynamic document text"},
		GlobalState:     make(map[string]interface{}),
		DocumentType:    "Invoice", // Needs to be set to trigger rule fetching
	}

	handlers, _ := e.BuildRuntimeHandlers(nil)
	_, _, _, _, err = e.ExecuteWorkflow(context.Background(), state, req, handlers)
	if err != nil {
		t.Fatalf("Expected successful execution, got error: %v", err)
	}

	// Verify that the empty step instructions/schema were populated
	step := req.Steps[0]
	if step.CustomInstructions != "instructions fetched dynamically" {
		t.Errorf("Expected dynamically fetched instructions, got: %q", step.CustomInstructions)
	}
	if step.ExpectedSchema != `{"type": "object", "properties": {"dynamic_id": {"type": "string"}}}` {
		t.Errorf("Expected dynamically fetched schema, got: %q", step.ExpectedSchema)
	}

	// Verify extracted data is in GlobalState
	rawRes, ok := state.GlobalState["dynamic_data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected dynamic_data in GlobalState, got %T", state.GlobalState["dynamic_data"])
	}
	if rawRes["dynamic_id"] != "fetched_123" {
		t.Errorf("Expected dynamic_id 'fetched_123', got %v", rawRes["dynamic_id"])
	}
}

func TestEngine_ExecuteStep_UnsupportedAction(t *testing.T) {
	e := NewEngine(nil, nil, nil)
	state := &ProcessingState{JobID: "job_unsupported"}
	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{
				ActionType: "NON_EXISTENT_ACTION",
				StepName:   "Unimplemented",
			},
		},
	}

	handlers, _ := e.BuildRuntimeHandlers(nil)
	_, _, _, _, err := e.ExecuteWorkflow(context.Background(), state, req, handlers)
	if err == nil {
		t.Fatal("Expected error on unsupported action, got nil")
	}
}

func TestEngine_ExecuteStep_DynamicActionDefinition(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockAI := &mockDocumentProcessorServer{
		extractedJson: `{"classification": "CustomInvoice"}`,
	}
	pb.RegisterDocumentProcessorServer(grpcServer, mockAI)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	aiClient, err := ai_client.NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create AI client: %v", err)
	}
	defer aiClient.Close()

	e := NewEngine(aiClient, nil, nil)

	// Custom client-defined action name mapping to standard primitive
	actionDefs := []*pb.ActionDefinition{
		{
			ClientActionName:      "MY_CUSTOM_CLASSIFICATION",
			OrchestratorPrimitive: "PRIMITIVE_AI_STEP",
		},
	}

	handlers, err := e.BuildRuntimeHandlers(actionDefs)
	if err != nil {
		t.Fatalf("Expected BuildRuntimeHandlers to succeed, got %v", err)
	}

	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{
				ActionType:         "MY_CUSTOM_CLASSIFICATION",
				StepName:           "CustomClassStep",
				CustomInstructions: "Run classification",
				ExpectedSchema:     `{"type": "object"}`,
				WritesToStateKey:   "custom_class_info",
			},
		},
	}

	state := &ProcessingState{
		JobID:           "job_custom_action",
		RawTextContents: []string{"some content"},
		GlobalState:     make(map[string]interface{}),
	}

	_, lastActionType, lastStepName, _, err := e.ExecuteWorkflow(context.Background(), state, req, handlers)
	if err != nil {
		t.Fatalf("Expected successful execution, got error: %v", err)
	}

	if lastActionType != "MY_CUSTOM_CLASSIFICATION" || lastStepName != "CustomClassStep" {
		t.Errorf("Unexpected last step info: action=%s, name=%s", lastActionType, lastStepName)
	}

	rawRes, ok := state.GlobalState["custom_class_info"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected custom_class_info in GlobalState, got %T", state.GlobalState["custom_class_info"])
	}

	if rawRes["classification"] != "CustomInvoice" {
		t.Errorf("Expected classification 'CustomInvoice', got %v", rawRes["classification"])
	}
}
