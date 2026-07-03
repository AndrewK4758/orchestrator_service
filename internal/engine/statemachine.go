package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	pb "doc_processor/shared_protos"
	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/callback_client"
	"orchestrator_service/internal/events"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrPendingHumanValidation = errors.New("pending human validation")

// WorkflowStep represents a single action step and its associated instructions.
type WorkflowStep struct {
	StepName            string `json:"step_name"`
	CustomInstructions  string `json:"custom_instructions"`
	Schema              string `json:"expected_schema"`
	CallbackAddress     string `json:"callback_address"`
	RequireHumanReview  bool   `json:"require_human_review"`
	BypassHumanFallback bool   `json:"bypass_human_fallback"`
	TargetActionAddress string `json:"target_action_address"`
}

// ProcessingState represents the global state of a single PDF job as it moves through the pipeline.
type ProcessingState struct {
	JobID           string   `json:"job_id"`
	RawPDFs         [][]byte `json:"pdf_contents"`
	RawTextContents []string `json:"raw_text_contents"`

	// Compliance & Traceability Metadata
	TenantID      string `json:"tenant_id"`
	UserID        string `json:"user_id"`
	AppID         string `json:"app_id"`
	DocumentType  string `json:"document_type"`
	CorrelationID string `json:"correlation_id"`

	// Dynamic parameters from the client
	Steps []WorkflowStep `json:"steps"`

	// State for the currently executing step
	CurrentStepIndex int

	// Thread-safe map to store results from concurrent AI extractions
	mu          sync.Mutex
	StepResults map[int]string

	// Result from the previous step (e.g. classification string to be passed to callbacks)
	PreviousStepResult string `json:"previous_step_result"`

	// Audit trail for visibility
	ExecutedSteps []string
	Errors        []error
}

// Lock locks the ProcessingState.
func (ps *ProcessingState) Lock() {
	ps.mu.Lock()
}

// Unlock unlocks the ProcessingState.
func (ps *ProcessingState) Unlock() {
	ps.mu.Unlock()
}

// TracePublisher allows decoupling the engine from the concrete JetStream publisher.
type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
}

// Engine executes generic steps iteratively.
type Engine struct {
	aiClient  *ai_client.Client
	pub       TracePublisher
	cbManager *callback_client.ClientManager
}

func NewEngine(aiClient *ai_client.Client, pub TracePublisher, cbManager *callback_client.ClientManager) *Engine {
	return &Engine{
		aiClient:  aiClient,
		pub:       pub,
		cbManager: cbManager,
	}
}

// Run executes the dynamically provided workflow steps.
func (e *Engine) Run(ctx context.Context, state *ProcessingState) error {
	log.Printf("INFO: [Engine.Run] Starting workflow for Job: %s", state.JobID)

	for i, step := range state.Steps {
		if i < state.CurrentStepIndex {
			log.Printf("INFO: [Engine.Run] Skipping Step %d: %s (already completed/resumed)", i+1, step.StepName)
			continue
		}
		state.CurrentStepIndex = i
		stepName := step.StepName
		if stepName == "" {
			stepName = fmt.Sprintf("Step-%d", i+1)
		}

		if err := ctx.Err(); err != nil {
			err = fmt.Errorf("job %s cancelled at step %s: %w", state.JobID, stepName, err)
			state.Errors = append(state.Errors, err)
			e.publishJobFailed(ctx, state, err.Error())
			return err
		}

		log.Printf("INFO: [Engine.Run] Executing Step %d: %s for Job: %s", i+1, stepName, state.JobID)

		e.publishTrace(ctx, state, events.EventJobStepStarted, fmt.Sprintf("Starting %s", stepName))

		err := e.executeGenericStep(ctx, state, step)
		if err != nil {
			if errors.Is(err, ErrPendingHumanValidation) {
				log.Printf("INFO: [Engine.Run] Job: %s, Step %s paused for Human Validation", state.JobID, stepName)
				e.publishTrace(ctx, state, events.EventJobStepPendingHuman, fmt.Sprintf("Step %s requires human validation", stepName))
				return err
			}
			err = fmt.Errorf("step %s failed: %w", stepName, err)
			log.Printf("ERROR: [Engine.Run] Job: %s, Step %s Failed: %v", state.JobID, stepName, err)
			state.Errors = append(state.Errors, err)

			e.publishTrace(ctx, state, events.EventJobStepFailed, fmt.Sprintf("Failed %s: %v", stepName, err))
			e.publishJobFailed(ctx, state, err.Error())
			return err
		}

		state.ExecutedSteps = append(state.ExecutedSteps, stepName)
		e.publishTrace(ctx, state, events.EventJobStepCompleted, fmt.Sprintf("Completed %s successfully", stepName))

		log.Printf("INFO: [Engine.Run] Step %s completed for Job: %s", stepName, state.JobID)
	}

	e.publishTrace(ctx, state, events.EventJobCompleted, "Workflow completed successfully", state.PreviousStepResult)
	log.Printf("INFO: [Engine.Run] Job %s completed successfully", state.JobID)

	return nil
}

func (e *Engine) executeGenericStep(ctx context.Context, state *ProcessingState, step WorkflowStep) error {
	prompt := step.CustomInstructions
	schema := step.Schema

	// 1. Resolve Callbacks
	if step.CallbackAddress != "" && e.cbManager != nil {
		// Pass PreviousStepResult as the "classification" or context to the callback
		rulesText, cbSchema, err := e.cbManager.GetRules(ctx, step.CallbackAddress, state.PreviousStepResult)
		if err != nil {
			log.Printf("Warning: failed to get rules from callback server at %s: %v", step.CallbackAddress, err)
			// fallback to the explicitly provided prompt and schema in the step
		} else {
			log.Printf("[GenericStep] Successfully fetched rules and schema via callback")
			prompt = rulesText
			schema = cbSchema
		}
	}

	if prompt == "" {
		return fmt.Errorf("no prompt provided and no callback rules found")
	}
	if schema == "" {
		return fmt.Errorf("no schema provided and no callback schema found")
	}

	// 2. Parallel Execution across full documents
	totalDocs := len(state.RawPDFs) + len(state.RawTextContents)
	if totalDocs == 0 {
		return fmt.Errorf("no documents available to process")
	}

	state.StepResults = make(map[int]string)
	var wg sync.WaitGroup
	errChan := make(chan error, totalDocs)

	for i, pdfBytes := range state.RawPDFs {
		wg.Add(1)
		go func(index int, data []byte) {
			defer wg.Done()

			e.publishChunkTrace(ctx, state, events.EventChunkStarted, fmt.Sprintf("Starting PDF chunk %d", index), index, totalDocs, "")

			resp, err := e.aiClient.ExtractChunk(ctx, data, "", prompt, schema)
			if err != nil {
				e.publishChunkTrace(ctx, state, events.EventChunkFailed, fmt.Sprintf("Failed PDF chunk %d", index), index, totalDocs, err.Error())
				errChan <- fmt.Errorf("failed to process PDF %d: %w", index, err)
				return
			}

			e.publishChunkTrace(ctx, state, events.EventChunkCompleted, fmt.Sprintf("Completed PDF chunk %d", index), index, totalDocs, "")

			state.mu.Lock()
			state.StepResults[index] = resp.GetExtractedJson()
			state.mu.Unlock()
		}(i, pdfBytes)
	}

	offset := len(state.RawPDFs)
	for i, textContent := range state.RawTextContents {
		wg.Add(1)
		go func(index int, text string) {
			defer wg.Done()

			e.publishChunkTrace(ctx, state, events.EventChunkStarted, fmt.Sprintf("Starting Text chunk %d", index), index, totalDocs, "")

			resp, err := e.aiClient.ExtractChunk(ctx, nil, text, prompt, schema)
			if err != nil {
				e.publishChunkTrace(ctx, state, events.EventChunkFailed, fmt.Sprintf("Failed Text chunk %d", index), index, totalDocs, err.Error())
				errChan <- fmt.Errorf("failed to process Text %d: %w", index, err)
				return
			}

			e.publishChunkTrace(ctx, state, events.EventChunkCompleted, fmt.Sprintf("Completed Text chunk %d", index), index, totalDocs, "")

			state.mu.Lock()
			state.StepResults[index] = resp.GetExtractedJson()
			state.mu.Unlock()
		}(offset+i, textContent)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// 3. Update PreviousStepResult for the next step
	// Combine results. If it's a single document, just save it.
	var finalJSON string
	if len(state.StepResults) == 1 {
		finalJSON = state.StepResults[0]
	} else {
		// Stitch into a JSON array string
		finalJSON = "["
		for i := 0; i < totalDocs; i++ {
			if result, ok := state.StepResults[i]; ok {
				if finalJSON != "[" {
					finalJSON += ","
				}
				finalJSON += result
			}
		}
		finalJSON += "]"
	}
	state.PreviousStepResult = finalJSON

	// 4. Validate output
	if step.RequireHumanReview {
		log.Printf("INFO: [Engine] Explicit human review requested for Step: %s. Triggering Human Validation.", step.StepName)
		taskID, err := e.triggerHumanValidation(ctx, state, step, finalJSON, "Explicit human review requested by workflow policy")
		if err != nil {
			log.Printf("ERROR: [Engine] Failed to trigger Human Validation: %v", err)
			return fmt.Errorf("failed to create explicit human validation task: %w", err)
		}
		log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobID)
		return ErrPendingHumanValidation
	}

	if valErr := e.validateAIOutput(finalJSON, schema); valErr != nil {
		if step.BypassHumanFallback {
			log.Printf("WARNING: [Engine] AI output invalid for Step: %s, but bypass_human_fallback is TRUE. Failing step instantly without fallback. Error: %v", step.StepName, valErr)
			return fmt.Errorf("ai output did not match requirements: %w", valErr)
		}

		log.Printf("WARNING: [Engine] AI output did not fit rules/requirements: %v. Triggering Human Validation.", valErr)

		// Create validation task
		taskID, err := e.triggerHumanValidation(ctx, state, step, finalJSON, valErr.Error())
		if err != nil {
			log.Printf("ERROR: [Engine] Failed to trigger Human Validation: %v", err)
			return fmt.Errorf("ai output invalid: %v, and failed to create human validation task: %w", valErr, err)
		}

		log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobID)
		return ErrPendingHumanValidation
	}

	// 5. Execute external generic action if target_action_address is specified
	if step.TargetActionAddress != "" {
		if err := e.executeExternalGenericAction(ctx, state, step, finalJSON); err != nil {
			log.Printf("ERROR: [Engine] Failed to execute external action at %s: %v", step.TargetActionAddress, err)
			return fmt.Errorf("external action failed: %w", err)
		}
	}

	return nil
}

func (e *Engine) publishTrace(ctx context.Context, state *ProcessingState, eventType events.EventType, message string, extractedData ...string) {
	stepName := "UnknownStep"
	if state.CurrentStepIndex >= 0 && state.CurrentStepIndex < len(state.Steps) {
		stepName = state.Steps[state.CurrentStepIndex].StepName
		if stepName == "" {
			stepName = fmt.Sprintf("Step-%d", state.CurrentStepIndex+1)
		}
	}

	var data string
	if len(extractedData) > 0 {
		data = extractedData[0]
	}

	if e.pub != nil {
		err := e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
			EventType:     eventType,
			Timestamp:     time.Now(),
			TenantID:      state.TenantID,
			UserID:        state.UserID,
			AppID:         state.AppID,
			JobID:         state.JobID,
			DocumentType:  state.DocumentType,
			CorrelationID: state.CorrelationID,
			JobAction:     stepName,
			Stage:         stepName,
			Message:       message,
			ExtractedData: data,
		})
		if err != nil {
			log.Printf("Warning: failed to publish trace event: %v", err)
		}
	}
}

func (e *Engine) publishChunkTrace(ctx context.Context, state *ProcessingState, eventType events.EventType, message string, chunkIndex int, totalChunks int, errorMsg string) {
	stepName := "UnknownStep"
	if state.CurrentStepIndex >= 0 && state.CurrentStepIndex < len(state.Steps) {
		stepName = state.Steps[state.CurrentStepIndex].StepName
		if stepName == "" {
			stepName = fmt.Sprintf("Step-%d", state.CurrentStepIndex+1)
		}
	}

	if e.pub != nil {
		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
			EventType:     eventType,
			Timestamp:     time.Now(),
			TenantID:      state.TenantID,
			UserID:        state.UserID,
			AppID:         state.AppID,
			JobID:         state.JobID,
			DocumentType:  state.DocumentType,
			CorrelationID: state.CorrelationID,
			JobAction:     stepName,
			Stage:         "ChunkProcessing",
			Message:       message,
			Error:         errorMsg,
			ChunkIndex:    chunkIndex + 1,
			TotalChunks:   totalChunks,
			ChunkID:       fmt.Sprintf("chunk_%d", chunkIndex+1),
		})
	}
}

func (e *Engine) publishJobFailed(ctx context.Context, state *ProcessingState, errMsg string) {
	if e.pub != nil {
		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
			EventType:     events.EventJobFailed,
			Timestamp:     time.Now(),
			TenantID:      state.TenantID,
			UserID:        state.UserID,
			AppID:         state.AppID,
			JobID:         state.JobID,
			DocumentType:  state.DocumentType,
			CorrelationID: state.CorrelationID,
			JobAction:     "DynamicWorkflow",
			Stage:         "JobFailed",
			Error:         errMsg,
			Message:       "Workflow failed",
		})
	}
}

func (e *Engine) validateAIOutput(jsonStr string, schema string) error {
	trimmed := strings.TrimSpace(jsonStr)
	if trimmed == "" {
		return fmt.Errorf("AI output is empty")
	}

	// 1. Must be valid JSON
	var raw interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return fmt.Errorf("AI output is not valid JSON: %w", err)
	}

	// 2. Check if the schema specifies required properties, and if they are empty
	var schemaMap map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &schemaMap); err == nil {
		if props, exists := schemaMap["properties"]; exists {
			if propsMap, ok := props.(map[string]interface{}); ok {
				// Let's check each property
				switch v := raw.(type) {
				case map[string]interface{}:
					for propName := range propsMap {
						val, exists := v[propName]
						if !exists {
							return fmt.Errorf("missing expected field from schema: %s", propName)
						}
						if val == nil {
							return fmt.Errorf("expected field is null: %s", propName)
						}
						if s, ok := val.(string); ok && strings.TrimSpace(s) == "" {
							return fmt.Errorf("expected field is empty: %s", propName)
						}
					}
				case []interface{}:
					for i, item := range v {
						itemMap, ok := item.(map[string]interface{})
						if !ok {
							continue
						}
						for propName := range propsMap {
							val, exists := itemMap[propName]
							if !exists {
								return fmt.Errorf("item %d missing expected field from schema: %s", i, propName)
							}
							if val == nil {
								return fmt.Errorf("item %d expected field is null: %s", i, propName)
							}
							if s, ok := val.(string); ok && strings.TrimSpace(s) == "" {
								return fmt.Errorf("item %d expected field is empty: %s", i, propName)
							}
						}
					}
				}
			}
		}
	}

	// 3. Custom flag check
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, `"flagged for review"`) || strings.Contains(lower, `"flagged_for_review"`) {
		return fmt.Errorf("AI output explicitly flagged for review")
	}

	return nil
}

func (e *Engine) triggerHumanValidation(ctx context.Context, state *ProcessingState, step WorkflowStep, failedOutput string, errMsg string) (string, error) {
	addr := os.Getenv("HUMAN_VALIDATION_SERVICE_ADDRESS")
	if addr == "" {
		addr = "localhost:50054"
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to dial human validation service: %w", err)
	}
	defer conn.Close()

	client := pb.NewHumanValidationServiceClient(conn)

	req := &pb.CreateTaskRequest{
		JobId:              state.JobID,
		StepName:           step.StepName,
		FailedAiOutput:     failedOutput,
		ExpectedSchema:     step.Schema,
		PromptInstructions: step.CustomInstructions,
		ErrorMessage:       errMsg,
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := client.CreateValidationTask(ctx, req)
	if err != nil {
		return "", fmt.Errorf("gRPC CreateValidationTask failed: %w", err)
	}

	return resp.GetTaskId(), nil
}

func (e *Engine) executeExternalGenericAction(ctx context.Context, state *ProcessingState, step WorkflowStep, payload string) error {
	log.Printf("INFO: [Engine] Dispatching generic action for Step: %s to Address: %s", step.StepName, step.TargetActionAddress)

	conn, err := grpc.Dial(step.TargetActionAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial target action server at %s: %w", step.TargetActionAddress, err)
	}
	defer conn.Close()

	client := pb.NewGenericActionServiceClient(conn)
	req := &pb.GenericActionRequest{
		JobId:       state.JobID,
		StepName:    step.StepName,
		PayloadJson: payload,
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := client.ExecuteAction(ctx, req)
	if err != nil {
		return fmt.Errorf("generic action call failed: %w", err)
	}

	if !resp.GetSuccess() {
		return fmt.Errorf("generic action returned execution failure: %s", resp.GetMessage())
	}

	log.Printf("INFO: [Engine] Successfully executed generic action. Response Message: %s, Result: %s", resp.GetMessage(), resp.GetResultJson())

	// If the external action returns a modified JSON, we save it as the step result
	if resp.GetResultJson() != "" {
		state.Lock()
		state.PreviousStepResult = resp.GetResultJson()
		state.Unlock()
	}

	return nil
}
