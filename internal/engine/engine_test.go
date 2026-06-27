package engine

import (
	"context"
	"fmt"
	"testing"
)

// MockAction allows us to test the Engine workflow execution
type MockAction struct {
	name        string
	shouldError bool
	executed    bool
}

func (m *MockAction) Name() string { return m.name }

func (m *MockAction) Execute(ctx context.Context, state *ProcessingState) error {
	m.executed = true
	if m.shouldError {
		return fmt.Errorf("mock action error")
	}
	return nil
}

func TestEngine_Run_Success(t *testing.T) {
	eng := NewEngine(nil)

	step1 := &MockAction{name: "Step1"}
	step2 := &MockAction{name: "Step2"}

	eng.Register(step1)
	eng.Register(step2)

	state := &ProcessingState{JobID: "job_123"}
	workflow := []string{"Step1", "Step2"}

	err := eng.Run(context.Background(), workflow, state)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !step1.executed {
		t.Error("Expected Step1 to be executed")
	}
	if !step2.executed {
		t.Error("Expected Step2 to be executed")
	}

	if len(state.ExecutedSteps) != 2 || state.ExecutedSteps[0] != "Step1" || state.ExecutedSteps[1] != "Step2" {
		t.Errorf("Expected ExecutedSteps [Step1, Step2], got %v", state.ExecutedSteps)
	}
}

func TestEngine_Run_ActionNotFound(t *testing.T) {
	eng := NewEngine(nil)

	state := &ProcessingState{JobID: "job_123"}
	workflow := []string{"UnknownStep"}

	err := eng.Run(context.Background(), workflow, state)
	if err == nil {
		t.Fatal("Expected error for unknown step, got nil")
	}
	if len(state.Errors) != 1 {
		t.Errorf("Expected 1 error in state, got %d", len(state.Errors))
	}
}

func TestEngine_Run_ActionFails(t *testing.T) {
	eng := NewEngine(nil)

	step1 := &MockAction{name: "Step1", shouldError: true}
	eng.Register(step1)

	state := &ProcessingState{JobID: "job_123"}
	workflow := []string{"Step1"}

	err := eng.Run(context.Background(), workflow, state)
	if err == nil {
		t.Fatal("Expected error when action fails, got nil")
	}
	if !step1.executed {
		t.Error("Expected Step1 to be executed")
	}
	if len(state.Errors) != 1 {
		t.Errorf("Expected 1 error in state, got %d", len(state.Errors))
	}
}
