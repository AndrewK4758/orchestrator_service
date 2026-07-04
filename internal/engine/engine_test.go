package engine

import (
	"context"
	pb "doc_processor/shared_protos"
	"testing"
)

func TestEngine_UnsupportedAction(t *testing.T) {
	eng := NewEngine(nil, nil, nil)
	state := &ProcessingState{JobID: "job_123"}
	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{ActionType: "UNKNOWN_ACTION"},
		},
	}

	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, req)
	if err == nil {
		t.Fatalf("Expected error for unsupported action type, got nil")
	}
}
