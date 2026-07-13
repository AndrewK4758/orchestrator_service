package engine

import (
	"context"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
)

func TestGeneratePlan_RequireHumanReview(t *testing.T) {
	// 4. Comprehensive Coverage: Unit test for Human in the Loop bypass
	eng := NewEngine(nil, nil, nil)

	state := &pb.WorkflowContext{JobId: "hitl_job"}
	step := &pb.StepDefinition{
		Id:                 "hitl_step",
		RequireHumanReview: true,
		CustomInstructions: "Test manual review",
		ExpectedSchema:     `{"type": "object"}`,
	}

	plan, err := eng.generatePlan(context.Background(), state, step)
	if err != nil {
		t.Fatalf("generatePlan failed unexpectedly: %v", err)
	}

	if plan == nil || len(plan.Actions) != 1 {
		t.Fatalf("Expected exactly 1 action in plan, got: %+v", plan)
	}

	action := plan.Actions[0]
	if action.ActionType != "PRIMITIVE_HUMAN_STEP" {
		t.Errorf("Expected action type PRIMITIVE_HUMAN_STEP, got %s", action.ActionType)
	}
	if action.Prompt != "Test manual review" {
		t.Errorf("Expected prompt to match step instructions, got %s", action.Prompt)
	}
}
