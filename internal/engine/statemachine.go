package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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

const PlannerSchema = `{
  "type": "object",
  "properties": {
    "actions": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "action_type": { "type": "string", "enum": ["PRIMITIVE_AI_STEP", "PRIMITIVE_HUMAN_STEP"] },
          "prompt": { "type": "string" },
          "schema": { "type": "string" }
        },
        "required": ["action_type", "prompt", "schema"]
      }
    }
  },
  "required": ["actions"]
}`

type ActionItem struct {
	ActionType string `json:"action_type"`
	Prompt     string `json:"prompt"`
	Schema     string `json:"schema"`
}

type ActionPlan struct {
	Actions []ActionItem `json:"actions"`
}

// ProcessingState represents the global state of a single PDF job as it moves through the pipeline.
type ProcessingState struct {
	JobID           string                 `json:"job_id"`
	RawPDFs         [][]byte               `json:"pdf_contents"`
	RawTextContents []string               `json:"raw_text_contents"`
	GlobalState     map[string]interface{} `json:"global_state"`

	// Compliance & Traceability Metadata
	TenantID      string `json:"tenant_id"`
	UserID        string `json:"user_id"`
	AppID         string `json:"app_id"`
	DocumentType  string `json:"document_type"`
	CorrelationID string `json:"correlation_id"`

	// Current step execution tracking
	CurrentStepReq     *pb.SubmitStepRequest `json:"current_step_req"`
	CurrentStepID      string                `json:"current_step_id"`
	CurrentStepIndex   int                   `json:"current_step_index"`
	PendingActions     []ActionItem          `json:"pending_actions"`
	CurrentActionIndex int                   `json:"current_action_index"`

	mu sync.Mutex
}

func (ps *ProcessingState) Lock() {
	ps.mu.Lock()
}

func (ps *ProcessingState) Unlock() {
	ps.mu.Unlock()
}

// SyncMetadata updates the state's compliance metadata fields from the GlobalState if they exist.
func (ps *ProcessingState) SyncMetadata() {
	if ps.GlobalState == nil {
		return
	}

	// Helper to extract nested or flat string values
	getString := func(key string) (string, bool) {
		val, exists := ps.GlobalState[key]
		if !exists {
			return "", false
		}
		if s, ok := val.(string); ok {
			return s, true
		}
		// If it's a map (e.g. classification JSON is parsed as a nested object)
		if m, ok := val.(map[string]interface{}); ok {
			for _, k := range []string{"classification", "Classification", "document_type", "DocumentType", "type", "Type"} {
				if v, ok := m[k]; ok {
					if s, ok := v.(string); ok {
						return s, true
					}
				}
			}
		}
		return "", false
	}

	if s, ok := getString("document_type"); ok {
		ps.DocumentType = s
	} else if s, ok := getString("DocumentType"); ok {
		ps.DocumentType = s
	} else if s, ok := getString("ClassificationInfo"); ok {
		ps.DocumentType = s
	} else if s, ok := getString("classification_info"); ok {
		ps.DocumentType = s
	}

	if s, ok := getString("tenant_id"); ok {
		ps.TenantID = s
	} else if s, ok := getString("TenantID"); ok {
		ps.TenantID = s
	}

	if s, ok := getString("user_id"); ok {
		ps.UserID = s
	} else if s, ok := getString("UserID"); ok {
		ps.UserID = s
	}

	if s, ok := getString("app_id"); ok {
		ps.AppID = s
	} else if s, ok := getString("AppID"); ok {
		ps.AppID = s
	}

	if s, ok := getString("correlation_id"); ok {
		ps.CorrelationID = s
	} else if s, ok := getString("CorrelationID"); ok {
		ps.CorrelationID = s
	}
}

type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
}

type ActionFunc func(ctx context.Context, state *ProcessingState, step *pb.WorkflowStep) (string, error)

type Engine struct {
	aiClient          *ai_client.Client
	pub               TracePublisher
	cbManager         *callback_client.ClientManager
	primitiveHandlers map[string]ActionFunc
}

func NewEngine(aiClient *ai_client.Client, pub TracePublisher, cbManager *callback_client.ClientManager) *Engine {
	e := &Engine{
		aiClient:  aiClient,
		pub:       pub,
		cbManager: cbManager,
	}
	e.primitiveHandlers = map[string]ActionFunc{
		"PRIMITIVE_AI_STEP":    e.executeAIStep,
		"PRIMITIVE_HUMAN_STEP": e.executeHumanValidationStep,
	}
	return e
}

func (e *Engine) fetchRulesForStep(ctx context.Context, state *ProcessingState, step *pb.WorkflowStep, callbackAddress string) error {
	if callbackAddress == "" {
		return fmt.Errorf("callback address is required to fetch rules for step %s", step.StepName)
	}
	var ruleKey string
	if step.ReadsFromStateKey != "" {
		if val, ok := state.GlobalState[step.ReadsFromStateKey]; ok {
			if s, ok := val.(string); ok {
				ruleKey = s
			} else if m, ok := val.(map[string]interface{}); ok {
				if typeStr, ok := m["classification"].(string); ok {
					ruleKey = typeStr
				} else if typeStr, ok := m["DocumentType"].(string); ok {
					ruleKey = typeStr
				}
			}
		}
	}

	if ruleKey == "" {
		if state.DocumentType != "" {
			ruleKey = state.DocumentType
		} else {
			ruleKey = step.StepName
		}
	}

	if ruleKey == "" {
		return fmt.Errorf("could not determine a rule key (e.g. classification or step name) to fetch rules for step %s", step.StepName)
	}
	if e.cbManager == nil {
		return fmt.Errorf("callback manager is not initialized, cannot fetch rules")
	}

	rulesText, jsonSchema, err := e.cbManager.GetRules(ctx, callbackAddress, ruleKey)
	if err != nil {
		return fmt.Errorf("failed to fetch rules from client schema registry: %w", err)
	}

	step.CustomInstructions = rulesText
	step.ExpectedSchema = jsonSchema
	return nil
}

func (e *Engine) executeHumanValidationStep(ctx context.Context, state *ProcessingState, step *pb.WorkflowStep) (string, error) {
	taskID, err := e.triggerHumanValidation(ctx, state, step, step.ContextInput, "Client triggered manual validation")
	if err != nil {
		return "", fmt.Errorf("failed to create human validation task: %w", err)
	}
	log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobID)
	return "", ErrPendingHumanValidation
}

func (e *Engine) generatePlan(ctx context.Context, state *ProcessingState, step *pb.WorkflowStep) (*ActionPlan, error) {
	stateJson, _ := json.MarshalIndent(state.GlobalState, "", "  ")
	plannerPrompt := fmt.Sprintf("You are an AI planner for a document orchestrator. Based on the client instructions and current state, decide the linear sequence of actions to take.\n\nInstructions: %s\nTarget Schema: %s\nCurrent State: %s\n\nReturn the actions array.", step.CustomInstructions, step.ExpectedSchema, string(stateJson))

	resp, err := e.aiClient.PerformAction(ctx, state.AppID, state.TenantID, state.JobID, nil, " ", plannerPrompt, PlannerSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to generate plan: %w", err)
	}

	var plan ActionPlan
	if err := json.Unmarshal([]byte(resp.GetActionResultJson()), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse action plan: %w", err)
	}

	return &plan, nil
}

func (e *Engine) ExecuteWorkflow(ctx context.Context, state *ProcessingState, req *pb.SubmitStepRequest) (string, string, string, error) {
	if state.CurrentStepReq == nil || state.CurrentStepReq != req {
		state.CurrentStepReq = req
		state.CurrentStepIndex = 0
		state.PendingActions = nil
		state.CurrentActionIndex = 0
	}

	var lastStepName string
	lastStepId := state.CurrentStepID

	for state.CurrentStepIndex < len(req.Steps) {
		step := req.Steps[state.CurrentStepIndex]
		lastStepName = step.StepName

		if step.CustomInstructions == "" || step.ExpectedSchema == "" {
			if err := e.fetchRulesForStep(ctx, state, step, req.CallbackAddress); err != nil {
				return "", lastStepName, lastStepId, err
			}
		}

		if len(state.PendingActions) == 0 {
			plan, err := e.generatePlan(ctx, state, step)
			if err != nil {
				return "", lastStepName, lastStepId, err
			}
			state.PendingActions = plan.Actions
			state.CurrentActionIndex = 0
		}

		for state.CurrentActionIndex < len(state.PendingActions) {
			action := state.PendingActions[state.CurrentActionIndex]

			handler, exists := e.primitiveHandlers[action.ActionType]
			if !exists {
				return "", lastStepName, lastStepId, fmt.Errorf("unknown action type from planner: %s", action.ActionType)
			}

			prompt := action.Prompt
			if prompt == "" {
				prompt = step.CustomInstructions
			}
			schema := action.Schema
			if schema == "" {
				schema = step.ExpectedSchema
			}

			actionStep := &pb.WorkflowStep{
				StepName:           fmt.Sprintf("%s - %s", step.StepName, action.ActionType),
				CustomInstructions: prompt,
				ExpectedSchema:     schema,
				ContextInput:       step.ContextInput,
			}

			resJson, err := handler(ctx, state, actionStep)
			if err != nil {
				if errors.Is(err, ErrPendingHumanValidation) {
					return "", lastStepName, lastStepId, err
				}
				return "", lastStepName, lastStepId, err
			}

			if step.WritesToStateKey != "" && resJson != "" {
				var parsed interface{}
				if err := json.Unmarshal([]byte(resJson), &parsed); err == nil {
					if state.GlobalState == nil {
						state.GlobalState = make(map[string]interface{})
					}
					state.GlobalState[step.WritesToStateKey] = parsed
					state.SyncMetadata()
				}
			}

			state.CurrentActionIndex++
		}

		state.PendingActions = nil
		state.CurrentActionIndex = 0
		state.CurrentStepIndex++
	}

	// Final state json
	finalJSON, _ := json.Marshal(state.GlobalState)
	return string(finalJSON), lastStepName, lastStepId, nil
}

func (e *Engine) executeAIStep(ctx context.Context, state *ProcessingState, req *pb.WorkflowStep) (string, error) {
	prompt := req.CustomInstructions
	schema := req.ExpectedSchema

	if prompt == "" || schema == "" {
		return "", fmt.Errorf("prompt and schema must be provided for AI step")
	}

	if req.ContextInput != "" {
		prompt = fmt.Sprintf("%s\n\n[CONTEXT PROVIDED BY CLIENT]:\n%s\n", prompt, req.ContextInput)
	}

	if req.ReadsFromStateKey != "" {
		val, ok := state.GlobalState[req.ReadsFromStateKey]
		if ok {
			stateBytes, _ := json.MarshalIndent(val, "", "  ")
			prompt = fmt.Sprintf("%s\n\n[CURRENT GLOBAL STATE]:\n%s\n", prompt, string(stateBytes))
		}
	}

	totalDocs := len(state.RawPDFs) + len(state.RawTextContents)
	if totalDocs == 0 {
		return "", fmt.Errorf("no documents available to process")
	}

	stepResults := make(map[int]string)
	var wg sync.WaitGroup
	errChan := make(chan error, totalDocs)
	var resultMu sync.Mutex

	for i, pdfBytes := range state.RawPDFs {
		wg.Add(1)
		go func(index int, data []byte) {
			defer wg.Done()
			e.publishChunkTrace(ctx, state, req, index, "", "STARTED", "", "")
			resp, err := e.aiClient.PerformAction(ctx, state.AppID, state.TenantID, state.JobID, data, "", prompt, schema)
			if err != nil {
				e.publishChunkTrace(ctx, state, req, index, "", "FAILED", "", err.Error())
				errChan <- fmt.Errorf("failed to process PDF %d: %w", index, err)
				return
			}
			e.publishChunkTrace(ctx, state, req, index, "", "COMPLETED", resp.GetActionResultJson(), "")

			resultMu.Lock()
			stepResults[index] = resp.GetActionResultJson()
			resultMu.Unlock()
		}(i, pdfBytes)
	}

	offset := len(state.RawPDFs)
	for i, textContent := range state.RawTextContents {
		wg.Add(1)
		go func(index int, text string) {
			defer wg.Done()
			e.publishChunkTrace(ctx, state, req, index, text, "STARTED", "", "")
			resp, err := e.aiClient.PerformAction(ctx, state.AppID, state.TenantID, state.JobID, nil, text, prompt, schema)
			if err != nil {
				e.publishChunkTrace(ctx, state, req, index, text, "FAILED", "", err.Error())
				errChan <- fmt.Errorf("failed to process Text %d: %w", index, err)
				return
			}
			e.publishChunkTrace(ctx, state, req, index, text, "COMPLETED", resp.GetActionResultJson(), "")

			resultMu.Lock()
			stepResults[index] = resp.GetActionResultJson()
			resultMu.Unlock()
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
		return "", chunkErr
	}

	var finalJSON string
	if len(stepResults) == 1 {
		finalJSON = stepResults[0]
	} else {
		finalJSON = "["
		for i := 0; i < totalDocs; i++ {
			if result, ok := stepResults[i]; ok {
				if finalJSON != "[" {
					finalJSON += ","
				}
				finalJSON += result
			}
		}
		finalJSON += "]"
	}

	return finalJSON, nil
}

func (e *Engine) triggerHumanValidation(ctx context.Context, state *ProcessingState, req *pb.WorkflowStep, failedOutput string, errMsg string) (string, error) {
	addr := os.Getenv("HUMAN_VALIDATION_SERVICE_ADDRESS")
	if addr == "" {
		addr = "localhost:50054"
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to dial human validation service: %w", err)
	}
	defer conn.Close()

	client := pb.NewHumanValidationServiceClient(conn)

	createReq := &pb.CreateTaskRequest{
		JobId:              state.JobID,
		StepName:           req.StepName,
		FailedAiOutput:     failedOutput,
		ExpectedSchema:     req.ExpectedSchema,
		PromptInstructions: req.CustomInstructions,
		ErrorMessage:       errMsg,
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := client.CreateValidationTask(ctx, createReq)
	if err != nil {
		return "", fmt.Errorf("gRPC CreateValidationTask failed: %w", err)
	}

	return resp.GetTaskId(), nil
}

func (e *Engine) publishChunkTrace(ctx context.Context, state *ProcessingState, req *pb.WorkflowStep, chunkIndex int, chunkText string, status string, extractedJson string, valErr string) {
	if e.pub != nil {
		var evType events.EventType
		if status == "STARTED" {
			evType = events.EventChunkStarted
		} else if status == "FAILED" {
			evType = events.EventChunkFailed
		} else {
			evType = events.EventChunkCompleted
		}

		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
			EventType:     evType,
			Timestamp:     time.Now(),
			TenantID:      state.TenantID,
			UserID:        state.UserID,
			AppID:         state.AppID,
			JobID:         state.JobID,
			DocumentType:  state.DocumentType,
			CorrelationID: state.CorrelationID,
			JobAction:     req.StepName,
			Stage:         "ChunkProcessing",
			Message:       status,
			Error:         valErr,
			ChunkIndex:    chunkIndex + 1,
			ChunkID:       fmt.Sprintf("chunk_%d", chunkIndex+1),
		})
	}
}
