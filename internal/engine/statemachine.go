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

type WorkflowStep struct {
	StepName            string `json:"step_name"`
	CustomInstructions  string `json:"custom_instructions"`
	Schema              string `json:"expected_schema"`
	CallbackAddress     string `json:"callback_address"`
	RequireHumanReview  bool   `json:"require_human_review"`
	BypassHumanFallback bool   `json:"bypass_human_fallback"`
	TargetActionAddress string `json:"target_action_address"`
	WritesToStateKey    string `json:"writes_to_state_key"`
	ReadsFromStateKey   string `json:"reads_from_state_key"`
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

	// New Goal-Oriented ReAct Agent fields
	OverarchingGoal string   `json:"overarching_goal"`
	EnabledTools    []string `json:"enabled_tools"`

	// State for the currently executing step
	CurrentStepIndex int

	// Thread-safe map to store results from concurrent AI extractions
	mu          sync.Mutex
	StepResults map[int]string

	// Result from the previous step (e.g., classification string to be passed to callbacks)
	PreviousStepResult string `json:"previous_step_result"`

	// GlobalState for LangGraph architecture
	GlobalState map[string]interface{} `json:"global_state"`

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
	if state.OverarchingGoal != "" {
		return e.RunReAct(ctx, state, state.OverarchingGoal, state.EnabledTools)
	}

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

	maxRetries := 3
	var valErr error
	var finalJSON string

	for attempt := 1; attempt <= maxRetries; attempt++ {
		state.StepResults = make(map[int]string)
		var wg sync.WaitGroup
		errChan := make(chan error, totalDocs)

		for i, pdfBytes := range state.RawPDFs {
			wg.Add(1)
			go func(index int, data []byte) {
				defer wg.Done()

				e.publishChunkTrace(ctx, state, events.EventChunkStarted, fmt.Sprintf("Starting PDF chunk %d (Attempt %d)", index, attempt), index, totalDocs, "")

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

				e.publishChunkTrace(ctx, state, events.EventChunkStarted, fmt.Sprintf("Starting Text chunk %d (Attempt %d)", index, attempt), index, totalDocs, "")

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

		var chunkErr error
		for err := range errChan {
			if err != nil {
				chunkErr = err
				break
			}
		}
		if chunkErr != nil {
			return chunkErr
		}

		// 3. Update PreviousStepResult for the next step
		if len(state.StepResults) == 1 {
			finalJSON = state.StepResults[0]
		} else {
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

		valErr = e.validateAIOutput(finalJSON, schema)
		if valErr == nil {
			break // Success!
		}

		if strings.Contains(valErr.Error(), "explicitly flagged for review") {
			break // Don't retry if the AI intentionally flagged it
		}

		if step.BypassHumanFallback {
			break // Fail instantly without retries if configured
		}

		log.Printf("WARNING: [Engine] AI output invalid for Step: %s. Attempt %d of %d. Error: %v", step.StepName, attempt, maxRetries, valErr)

		if attempt < maxRetries {
			prompt = fmt.Sprintf("%s\n\n[SYSTEM FEEDBACK FROM PREVIOUS ATTEMPT]: %v. Please carefully review the document and ensure this strictly required data is extracted exactly as specified in the schema.", prompt, valErr)
			log.Printf("INFO: [Engine] Retrying step %s due to validation failure.", step.StepName)
		}
	}

	if valErr != nil {
		if step.BypassHumanFallback {
			log.Printf("WARNING: [Engine] AI output invalid for Step: %s, but bypass_human_fallback is TRUE. Failing step instantly without fallback. Error: %v", step.StepName, valErr)
			return fmt.Errorf("ai output did not match requirements: %w", valErr)
		}

		log.Printf("WARNING: [Engine] AI output did not fit rules/requirements after %d attempts: %v. Triggering Human Validation.", maxRetries, valErr)

		// Create validation task
		taskID, err := e.triggerHumanValidation(ctx, state, step, finalJSON, valErr.Error())
		if err != nil {
			log.Printf("ERROR: [Engine] Failed to trigger Human Validation: %v", err)
			return fmt.Errorf("ai output invalid: %v, and failed to create human validation task: %w", valErr, err)
		}

		log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobID)
		return ErrPendingHumanValidation
	}

	// 5. Execute an external generic action if target_action_address is specified
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

	// 2. Check if the schema specifies required properties and if they are empty
	var schemaMap map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &schemaMap); err == nil {
		if requiredObj, exists := schemaMap["required"]; exists {
			if requiredList, ok := requiredObj.([]interface{}); ok {
				var requiredProps []string
				for _, r := range requiredList {
					if rStr, ok := r.(string); ok {
						requiredProps = append(requiredProps, rStr)
					}
				}

				switch v := raw.(type) {
				case map[string]interface{}:
					for _, propName := range requiredProps {
						val, exists := v[propName]
						if !exists {
							return fmt.Errorf("missing strictly required field from schema: %s", propName)
						}
						if val == nil {
							return fmt.Errorf("strictly required field is null: %s", propName)
						}
						if s, ok := val.(string); ok && strings.TrimSpace(s) == "" {
							return fmt.Errorf("strictly required field is empty: %s", propName)
						}
					}
				case []interface{}:
					for i, item := range v {
						itemMap, ok := item.(map[string]interface{})
						if !ok {
							continue
						}
						for _, propName := range requiredProps {
							val, exists := itemMap[propName]
							if !exists {
								return fmt.Errorf("item %d missing strictly required field from schema: %s", i, propName)
							}
							if val == nil {
								return fmt.Errorf("item %d strictly required field is null: %s", i, propName)
							}
							if s, ok := val.(string); ok && strings.TrimSpace(s) == "" {
								return fmt.Errorf("item %d strictly required field is empty: %s", i, propName)
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

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to dial human validation service: %w", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)

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

	conn, err := grpc.NewClient(step.TargetActionAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial target action server at %s: %w", step.TargetActionAddress, err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)

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

// RunReAct executes a goal-oriented workflow using the ReAct (Reason -> Act) loop
func (e *Engine) RunReAct(ctx context.Context, state *ProcessingState, goal string, enabledTools []string) error {
	log.Printf("INFO: [ReAct] Starting agentic loop for Job: %s with Goal: '%s'", state.JobID, goal)

	// Construct available tools documentation
	toolsDesc := `
Available Tools:
1. Tool: "DocumentExtractor"
   Description: Extract structured JSON from PDF or raw text documents using a prompt and expected schema.
   Input Schema: {"pdf_index": 0, "prompt": "extraction instructions", "schema": "target JSON schema"}

2. Tool: "HumanReview"
   Description: Suspends the job and prompts a human operator for manual correction, approval, or validation of data. Use this tool if the extracted data fails validation or requires manual review.
   Input Schema: {"step_name": "name", "failed_output": "raw data", "expected_schema": "target JSON schema", "instructions": "reason for review"}

3. Tool: "GenericAction"
   Description: Executes a deterministic business action by dispatching a validated JSON payload to an external gRPC service or HTTP webhook.
   Input Schema: {"address": "endpoint_address", "payload": "JSON string to send"}
`

	// Restore execution history if we are resuming from human review
	history := ""
	state.Lock()
	if state.PreviousStepResult != "" {
		history = state.PreviousStepResult
		state.PreviousStepResult = "" // Reset history buffer
	}
	state.Unlock()

	schema := `{
		"type": "object",
		"properties": {
			"thought": {"type": "string"},
			"tool_name": {"type": "string"},
			"tool_input": {"type": "string"}
		},
		"required": ["thought", "tool_name", "tool_input"]
	}`

	stepLimit := 10
	for loopStep := 1; loopStep <= stepLimit; loopStep++ {
		log.Printf("INFO: [ReAct] Loop Step %d for Job: %s", loopStep, state.JobID)

		prompt := fmt.Sprintf(`
You are a ReAct (Reason + Act) agent planner. Your overarching goal is: "%s"

%s

You must analyze the Goal, the available Tools, and the History of thoughts/actions/observations, and decide the NEXT logical action to take.
If the goal is fully accomplished, set "tool_name" to "FINISH" and provide the final aggregated results inside "tool_input".

Current Execution History:
%s

Output your thought and tool choice matching this strict JSON format.
`, goal, toolsDesc, history)

		// 1. Ask the AI Planner what to do next
		// Pass empty text since this is a planning step
		plannerResp, err := e.aiClient.ExtractChunk(ctx, nil, "Planning Context", prompt, schema)
		if err != nil {
			return fmt.Errorf("AI planner step failed: %w", err)
		}

		type ReActDecision struct {
			Thought   string `json:"thought"`
			ToolName  string `json:"tool_name"`
			ToolInput string `json:"tool_input"`
		}

		var dec ReActDecision
		if err := json.Unmarshal([]byte(plannerResp.GetExtractedJson()), &dec); err != nil {
			log.Printf("ERROR: [ReAct] Failed to parse AI planner decision: %v. Raw: %s", err, plannerResp.GetExtractedJson())
			return fmt.Errorf("failed to parse planner decision: %w", err)
		}

		log.Printf("INFO: [ReAct] Thought: %s", dec.Thought)
		log.Printf("INFO: [ReAct] Action: Call '%s' with Input: %s", dec.ToolName, dec.ToolInput)

		history += fmt.Sprintf("\nStep %d:\nThought: %s\nAction: Call '%s' with '%s'\n", loopStep, dec.Thought, dec.ToolName, dec.ToolInput)

		if strings.ToUpper(dec.ToolName) == "FINISH" {
			log.Printf("INFO: [ReAct] Goal achieved! Finishing workflow for Job: %s", state.JobID)
			state.Lock()
			state.PreviousStepResult = dec.ToolInput
			state.Unlock()
			e.publishTrace(ctx, state, events.EventJobCompleted, "Agent successfully achieved goal", dec.ToolInput)
			return nil
		}

		// 2. Execute the tool and get the Observation
		observation := ""
		switch dec.ToolName {
		case "DocumentExtractor":
			observation, err = e.runDocumentExtractorTool(ctx, state, dec.ToolInput)
		case "HumanReview":
			// Save the history so when we resume, we can load it!
			state.Lock()
			state.PreviousStepResult = history // Save history in PreviousStepResult so we resume with context!
			state.Unlock()
			err = e.runHumanReviewTool(ctx, state, dec.ToolInput)
		case "GenericAction":
			observation, err = e.runGenericActionTool(ctx, state, dec.ToolInput)
		default:
			observation = fmt.Sprintf("Error: Unknown tool '%s'", dec.ToolName)
		}

		if err != nil {
			if errors.Is(err, ErrPendingHumanValidation) {
				// Job is suspended for human validation, return cleanly to pause consumer thread!
				e.publishTrace(ctx, state, events.EventJobStepPendingHuman, "Agent workflow paused for human validation")
				return err
			}
			observation = fmt.Sprintf("Error during tool execution: %v", err)
		}

		log.Printf("INFO: [ReAct] Observation: %s", observation)
		history += fmt.Sprintf("Observation: %s\n", observation)
	}

	return fmt.Errorf("agent exceeded step limit of %d without achieving the goal", stepLimit)
}

func (e *Engine) runDocumentExtractorTool(ctx context.Context, state *ProcessingState, input string) (string, error) {
	type ExtractorInput struct {
		PdfIndex int    `json:"pdf_index"`
		Prompt   string `json:"prompt"`
		Schema   string `json:"schema"`
	}
	var in ExtractorInput
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		return "", fmt.Errorf("invalid input format for DocumentExtractor: %w", err)
	}

	// Boundary check
	totalDocs := len(state.RawPDFs) + len(state.RawTextContents)
	if in.PdfIndex < 0 || in.PdfIndex >= totalDocs {
		return "", fmt.Errorf("pdf_index %d out of bounds (total documents available: %d)", in.PdfIndex, totalDocs)
	}

	// Perform extraction
	var resp *pb.ExtractResponse
	var err error
	if in.PdfIndex < len(state.RawPDFs) {
		resp, err = e.aiClient.ExtractChunk(ctx, state.RawPDFs[in.PdfIndex], "", in.Prompt, in.Schema)
	} else {
		textIndex := in.PdfIndex - len(state.RawPDFs)
		resp, err = e.aiClient.ExtractChunk(ctx, nil, state.RawTextContents[textIndex], in.Prompt, in.Schema)
	}

	if err != nil {
		return "", fmt.Errorf("AI extraction failed: %w", err)
	}

	return resp.GetExtractedJson(), nil
}

func (e *Engine) runHumanReviewTool(ctx context.Context, state *ProcessingState, input string) error {
	type ReviewInput struct {
		StepName       string `json:"step_name"`
		FailedOutput   string `json:"failed_output"`
		ExpectedSchema string `json:"expected_schema"`
		Instructions   string `json:"instructions"`
	}
	var in ReviewInput
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		return fmt.Errorf("invalid input format for HumanReview: %w", err)
	}

	step := WorkflowStep{
		StepName:           in.StepName,
		CustomInstructions: in.Instructions,
		Schema:             in.ExpectedSchema,
	}

	taskID, err := e.triggerHumanValidation(ctx, state, step, in.FailedOutput, "Agent triggered manual validation")
	if err != nil {
		return fmt.Errorf("failed to trigger human validation: %w", err)
	}

	log.Printf("INFO: [ReAct] Successfully suspended agent workflow. Created task: %s", taskID)
	return ErrPendingHumanValidation
}

func (e *Engine) runGenericActionTool(ctx context.Context, state *ProcessingState, input string) (string, error) {
	type ActionInput struct {
		Address string `json:"address"`
		Payload string `json:"payload"`
	}
	var in ActionInput
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		return "", fmt.Errorf("invalid input format for GenericAction: %w", err)
	}

	step := WorkflowStep{
		StepName:            "AgentAction",
		TargetActionAddress: in.Address,
	}

	err := e.executeExternalGenericAction(ctx, state, step, in.Payload)
	if err != nil {
		return "", fmt.Errorf("generic action call failed: %w", err)
	}

	return "Success: External action executed successfully", nil
}
