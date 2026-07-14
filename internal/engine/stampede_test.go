package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
)

func TestExecuteWorkflow_CacheStampede(t *testing.T) {
	mockAI := &MockAIClient{
		PerformActionFunc: func(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error) {
			if strings.Contains(currentAction.Name, "Review") {
				return &pb.PerformActionResponse{ActionResultJson: `{"approved": true, "reason": "ok"}`}, nil
			} else if strings.Contains(currentAction.Name, "Plan") {
				planJSON := `{"actions": [{"action_type": "PRIMITIVE_AI_STEP", "prompt": "test", "schema": {}}]}`
				return &pb.PerformActionResponse{ActionResultJson: planJSON}, nil
			} else if strings.Contains(currentAction.Name, "PRIMITIVE_AI_STEP") {
				// Simulate slow AI inference (200ms)
				time.Sleep(200 * time.Millisecond)
				return &pb.PerformActionResponse{ActionResultJson: `{}`}, nil
			} else if strings.Contains(currentAction.Name, "Validate") {
				return &pb.PerformActionResponse{ActionResultJson: `{"valid": true, "reason": "ok"}`}, nil
			}
			return &pb.PerformActionResponse{ActionResultJson: "{}"}, nil
		},
	}

	mockCache := &MockCacheClient{
		CheckCacheFunc: func(ctx context.Context, text string, metadata map[string]string, threshold float32) (*pb.CheckCacheResponse, error) {
			// Always return miss for this test to force stampede behavior
			return &pb.CheckCacheResponse{Hit: false}, nil
		},
		StoreExtractionFunc: func(ctx context.Context, text string, metadata map[string]string, payload string) error {
			return nil
		},
	}
	mockPub := &MockTracePublisher{}

	eng := NewEngine(mockAI, mockCache, mockPub)

	step := &pb.StepDefinition{
		Id:                 "step-stampede",
		StepName:           "Stampede Step",
		CustomInstructions: "Test stampede",
		ExpectedSchema:     `{}`,
		ContextInput:       "same_cache_key_input",
		UseCache:           true,
	}

	var wg sync.WaitGroup
	startChan := make(chan struct{})

	// Launch 5 concurrent jobs with the exact same ContextInput
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(jobIdx int) {
			defer wg.Done()
			
			state := &pb.WorkflowContext{
				JobId:       "stampede_job_" + string(rune(jobIdx)),
				GlobalState: nil,
			}
			
			<-startChan // wait for sync start
			
			_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, step)
			if err != nil {
				t.Errorf("Job %d failed: %v", jobIdx, err)
			}
		}(i)
	}

	// Release all at once
	close(startChan)

	// Wait for all to finish
	wg.Wait()
}
