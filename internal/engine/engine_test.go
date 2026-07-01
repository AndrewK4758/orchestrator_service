package engine

import (
	"context"
	"testing"
)

func TestEngine_Run_NoSteps(t *testing.T) {
	eng := NewEngine(nil, nil, nil)
	state := &ProcessingState{JobID: "job_123"}

	err := eng.Run(context.Background(), state)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}
