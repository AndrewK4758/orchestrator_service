package engine

import (
	"context"
	"errors"
	"net"
	"testing"

	pb "doc_processor/shared_protos"
	"google.golang.org/grpc"
	"orchestrator_service/internal/ai_client"
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
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}, "required": ["classification"]}`,
			expectError: false,
		},
		{
			name:        "Invalid JSON syntax",
			jsonStr:     `{"classification": "invoice"`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}, "required": ["classification"]}`,
			expectError: true,
		},
		{
			name:        "Missing field from schema properties",
			jsonStr:     `{"confidence": 0.95}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}, "required": ["classification"]}`,
			expectError: true,
		},
		{
			name:        "Empty field from schema properties",
			jsonStr:     `{"classification": "  "}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}, "required": ["classification"]}`,
			expectError: true,
		},
		{
			name:        "Explicitly Flagged For Review",
			jsonStr:     `{"classification": "Flagged For Review"}`,
			schema:      `{"type": "object", "properties": {"classification": {"type": "string"}}, "required": ["classification"]}`,
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

	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{
				ActionType:          "AI_EXTRACTION",
				StepName:            "Extraction",
				CustomInstructions:  "Extract identity",
				ExpectedSchema:      `{"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}`,
				BypassHumanFallback: true, // Fail instantly without human task creation
			},
		},
	}

	state := &ProcessingState{
		JobID:           "job_bypass",
		RawTextContents: []string{"some content"},
	}

	_, _, _, _, err = e.ExecuteWorkflow(context.Background(), state, req)
	if err == nil {
		t.Fatal("Expected error on invalid output, got nil")
	}

	if errors.Is(err, ErrPendingHumanValidation) {
		t.Errorf("Expected standard validation error, but got ErrPendingHumanValidation (which indicates fallback occurred)")
	}
}

func TestEngine_RequireHumanReview(t *testing.T) {
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

	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{
				ActionType:         "AI_EXTRACTION",
				StepName:           "ReviewNeeded",
				CustomInstructions: "Extract identity",
				ExpectedSchema:     `{"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}`,
				RequireHumanReview: true, // Force manual review
			},
		},
	}

	state := &ProcessingState{
		JobID:           "job_require_review",
		RawTextContents: []string{"some content"},
	}

	_, _, _, _, err = e.ExecuteWorkflow(context.Background(), state, req)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Must fail with ErrPendingHumanValidation to signal workflow suspension
	if !errors.Is(err, ErrPendingHumanValidation) {
		t.Errorf("Expected ErrPendingHumanValidation, got %v", err)
	}
}
