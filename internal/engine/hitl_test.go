package engine

import (
	"context"
	"errors"
	"net"
	"testing"

	pb "doc_processor/shared_protos"
	"orchestrator_service/internal/ai_client"
	"google.golang.org/grpc"
)

func TestEngine_ValidateAIOutput(t *testing.T) {
	e := NewEngine(nil, nil, nil)

	tests := []struct {
		name        string
		jsonStr     string
		schema      string
		expectError bool
	}{
		{
			name:        "Valid JSON matching schema",
			jsonStr:     `{"classification": "invoice", "confidence": 0.95}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: false,
		},
		{
			name:        "Invalid JSON syntax",
			jsonStr:     `{"classification": "invoice"`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: true,
		},
		{
			name:        "Missing field from schema properties",
			jsonStr:     `{"confidence": 0.95}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: true,
		},
		{
			name:        "Empty field from schema properties",
			jsonStr:     `{"classification": "  "}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: true,
		},
		{
			name:        "Array of valid objects matching schema",
			jsonStr:     `[{"classification": "invoice"}, {"classification": "receipt"}]`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: false,
		},
		{
			name:        "Array of objects with missing schema field",
			jsonStr:     `[{"classification": "invoice"}, {"confidence": 0.9}]`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: true,
		},
		{
			name:        "Explicitly Flagged For Review",
			jsonStr:     `{"classification": "Flagged For Review"}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := e.validateAIOutput(tt.jsonStr, tt.schema)
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got %v", err)
			}
		})
	}
}

func TestEngine_Run_SkipCompletedSteps(t *testing.T) {
	eng := NewEngine(nil, nil, nil)

	// Set up steps
	steps := []WorkflowStep{
		{StepName: "Step-1", CustomInstructions: "Inst 1", Schema: "{}"},
		{StepName: "Step-2", CustomInstructions: "Inst 2", Schema: "{}"},
		{StepName: "Step-3", CustomInstructions: "Inst 3", Schema: "{}"},
	}

	state := &ProcessingState{
		JobID:            "job_abc",
		Steps:            steps,
		CurrentStepIndex: 2, // Tells it to resume directly from Step-3!
	}

	err := eng.Run(context.Background(), state)
	if err == nil {
		t.Fatalf("Expected error due to no documents, but got nil")
	}

	// Verify that Step-1 and Step-2 were not executed
	for _, executed := range state.ExecutedSteps {
		if executed == "Step-1" || executed == "Step-2" {
			t.Errorf("Step-1 and Step-2 should have been skipped!")
		}
	}
}

type mockDocumentProcessorServer struct {
	pb.UnimplementedDocumentProcessorServer
	extractedJson string
}

func (m *mockDocumentProcessorServer) ExtractDocumentData(ctx context.Context, req *pb.ExtractRequest) (*pb.ExtractResponse, error) {
	return &pb.ExtractResponse{
		ExtractedJson: m.extractedJson,
	}, nil
}

func TestEngine_BypassHumanFallback(t *testing.T) {
	// Start local mock AI server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockAI := &mockDocumentProcessorServer{
		extractedJson: `{"name": "No ID field here"}`, // Invalid output relative to schema below
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

	step := WorkflowStep{
		StepName:            "Extraction",
		CustomInstructions:  "Extract identity",
		Schema:              `{"type": "object", "properties": {"id": {"type": "string"}}}`,
		BypassHumanFallback: true, // Fail instantly without human task creation
	}

	state := &ProcessingState{
		JobID:           "job_bypass",
		RawTextContents: []string{"some content"},
		Steps:           []WorkflowStep{step},
	}

	err = e.executeGenericStep(context.Background(), state, step)
	if err == nil {
		t.Fatal("Expected error on invalid output, got nil")
	}

	if errors.Is(err, ErrPendingHumanValidation) {
		t.Errorf("Expected standard validation error, but got ErrPendingHumanValidation (which indicates fallback occurred)")
	}
}

func TestEngine_RequireHumanReview(t *testing.T) {
	// Start local mock AI server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockAI := &mockDocumentProcessorServer{
		extractedJson: `{"id": "valid_id"}`, // Fully valid output matching schema
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

	step := WorkflowStep{
		StepName:           "ReviewNeeded",
		CustomInstructions: "Extract identity",
		Schema:             `{"type": "object", "properties": {"id": {"type": "string"}}}`,
		RequireHumanReview: true, // Force manual review
	}

	state := &ProcessingState{
		JobID:           "job_require_review",
		RawTextContents: []string{"some content"},
		Steps:           []WorkflowStep{step},
	}

	err = e.executeGenericStep(context.Background(), state, step)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Must fail with ErrPendingHumanValidation to signal workflow suspension
	if !errors.Is(err, ErrPendingHumanValidation) {
		t.Errorf("Expected ErrPendingHumanValidation, got %v", err)
	}
}

type mockGenericActionServer struct {
	pb.UnimplementedGenericActionServiceServer
	lastPayload string
	success     bool
	message     string
	resultJson  string
}

func (m *mockGenericActionServer) ExecuteAction(ctx context.Context, req *pb.GenericActionRequest) (*pb.GenericActionResponse, error) {
	m.lastPayload = req.GetPayloadJson()
	return &pb.GenericActionResponse{
		Success:    m.success,
		Message:    m.message,
		ResultJson: m.resultJson,
	}, nil
}

func TestEngine_ExecuteExternalAction(t *testing.T) {
	// Start local mock gRPC server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	grpcServer := grpc.NewServer()
	mockServer := &mockGenericActionServer{
		success:    true,
		message:    "Action executed successfully",
		resultJson: `{"status": "processed", "id": 123}`,
	}
	pb.RegisterGenericActionServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	e := NewEngine(nil, nil, nil)

	step := WorkflowStep{
		StepName:            "ActionPhase",
		TargetActionAddress: addr,
	}

	state := &ProcessingState{
		JobID: "job_action",
	}

	// Call the generic action
	payload := `{"invoice_id": "inv_999"}`
	err = e.executeExternalGenericAction(context.Background(), state, step, payload)
	if err != nil {
		t.Fatalf("Failed to execute generic action: %v", err)
	}

	// Verify that the mock server received the payload
	if mockServer.lastPayload != payload {
		t.Errorf("Expected mock server to receive payload %s, got %s", payload, mockServer.lastPayload)
	}

	// Verify that state.PreviousStepResult was updated with the mock's result JSON
	if state.PreviousStepResult != mockServer.resultJson {
		t.Errorf("Expected state.PreviousStepResult to be updated with %s, got %s", mockServer.resultJson, state.PreviousStepResult)
	}
}

type multiStageMockAIServer struct {
	pb.UnimplementedDocumentProcessorServer
	callCount  int
	actionAddr string
}

func (m *multiStageMockAIServer) ExtractDocumentData(ctx context.Context, req *pb.ExtractRequest) (*pb.ExtractResponse, error) {
	m.callCount++
	var response string

	switch m.callCount {
	case 1:
		// First planning step: run DocumentExtractor
		response = `{
			"thought": "I need to extract the invoice details first.",
			"tool_name": "DocumentExtractor",
			"tool_input": "{\"pdf_index\": 0, \"prompt\": \"Extract invoice total\", \"schema\": \"{}\"}"
		}`
	case 2:
		// Tool execution of DocumentExtractor
		response = `{"total_amount": 150.0}`
	case 3:
		// Second planning step: execute external generic action with extracted info
		response = `{
			"thought": "Now I must save the invoice details to the external system.",
			"tool_name": "GenericAction",
			"tool_input": "{\"address\": \"` + m.actionAddr + `\", \"payload\": \"{\\\"invoice_total\\\": 150.0}\"}"
		}`
	case 4:
		// Third planning step: FINISH
		response = `{
			"thought": "Extraction and action complete. I am finished.",
			"tool_name": "FINISH",
			"tool_input": "{\"final_status\": \"completed\", \"amount\": 150.0}"
		}`
	}

	return &pb.ExtractResponse{
		Success:       true,
		ExtractedJson: response,
	}, nil
}

func TestEngine_ReActAgentWorkflow(t *testing.T) {
	// 1. Start local mock generic action server
	actionListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for action: %v", err)
	}
	defer actionListener.Close()

	actionServer := grpc.NewServer()
	mockAction := &mockGenericActionServer{
		success:    true,
		message:    "Action executed successfully",
		resultJson: `{"db_id": "inserted_abc"}`,
	}
	pb.RegisterGenericActionServiceServer(actionServer, mockAction)
	go func() {
		_ = actionServer.Serve(actionListener)
	}()
	defer actionServer.Stop()

	// 2. Start local mock AI planner/extractor server
	aiListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for AI: %v", err)
	}
	defer aiListener.Close()

	aiServer := grpc.NewServer()
	mockAI := &multiStageMockAIServer{
		actionAddr: actionListener.Addr().String(),
	}
	pb.RegisterDocumentProcessorServer(aiServer, mockAI)
	go func() {
		_ = aiServer.Serve(aiListener)
	}()
	defer aiServer.Stop()

	// 3. Connect the real AI client to the mock AI server
	aiClient, err := ai_client.NewClient(aiListener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create AI client: %v", err)
	}
	defer aiClient.Close()

	e := NewEngine(aiClient, nil, nil)

	// Create goal-oriented state
	state := &ProcessingState{
		JobID:           "react_agent_job",
		RawTextContents: []string{"Sample raw invoice content: $150"},
	}

	// 4. Run the Agent loop
	goal := "Extract total invoice amount and write it to database"
	err = e.RunReAct(context.Background(), state, goal, []string{"DocumentExtractor", "GenericAction"})
	if err != nil {
		t.Fatalf("Agent workflow failed: %v", err)
	}

	// 5. Assertions
	if mockAI.callCount != 4 {
		t.Errorf("Expected exactly 4 AI calls, got %d", mockAI.callCount)
	}

	expectedFinalResult := `{"final_status": "completed", "amount": 150.0}`
	if state.PreviousStepResult != expectedFinalResult {
		t.Errorf("Expected final result %s, got %s", expectedFinalResult, state.PreviousStepResult)
	}

	expectedActionPayload := `{"invoice_total": 150.0}`
	if mockAction.lastPayload != expectedActionPayload {
		t.Errorf("Expected generic action payload %s, got %s", expectedActionPayload, mockAction.lastPayload)
	}
}
