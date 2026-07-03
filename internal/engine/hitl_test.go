package engine

import (
	"context"
	"testing"
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

	// Step-3 will execute but since there are no documents, it will return error immediately.
	// But it should NOT execute Step-1 or Step-2!
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
