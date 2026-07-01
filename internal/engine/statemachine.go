package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/callback_client"
	"orchestrator_service/internal/events"
)

// WorkflowStep represents a single action step and its associated instructions.
type WorkflowStep struct {
	StepName           string `json:"step_name"`
	CustomInstructions string `json:"custom_instructions"`
	Schema             string `json:"expected_schema"`
	CallbackAddress    string `json:"callback_address"`
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

	e.publishTrace(ctx, state, events.EventJobCompleted, "Workflow completed successfully")
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

			resp, err := e.aiClient.ExtractChunk(ctx, data, "", prompt, schema)
			if err != nil {
				errChan <- fmt.Errorf("failed to process PDF %d: %w", index, err)
				return
			}

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

			resp, err := e.aiClient.ExtractChunk(ctx, nil, text, prompt, schema)
			if err != nil {
				errChan <- fmt.Errorf("failed to process Text %d: %w", index, err)
				return
			}

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
	if len(state.StepResults) == 1 {
		state.PreviousStepResult = state.StepResults[0]
	} else {
		// Stitch into a JSON array string
		finalJSON := "["
		for i := 0; i < totalDocs; i++ {
			if result, ok := state.StepResults[i]; ok {
				if finalJSON != "[" {
					finalJSON += ","
				}
				finalJSON += result
			}
		}
		finalJSON += "]"
		state.PreviousStepResult = finalJSON
	}

	return nil
}

func (e *Engine) publishTrace(ctx context.Context, state *ProcessingState, eventType events.EventType, message string) {
	stepName := "UnknownStep"
	if state.CurrentStepIndex >= 0 && state.CurrentStepIndex < len(state.Steps) {
		stepName = state.Steps[state.CurrentStepIndex].StepName
		if stepName == "" {
			stepName = fmt.Sprintf("Step-%d", state.CurrentStepIndex+1)
		}
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
		})
		if err != nil {
			log.Printf("Warning: failed to publish trace event: %v", err)
		}
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
			Message:       errMsg,
		})
	}
}
