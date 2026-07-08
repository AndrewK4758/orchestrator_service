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

// ProcessingState represents the overarching state of a graph execution job.
type ProcessingState struct {
	JobID       string                 `json:"job_id"`
	GlobalState map[string]interface{} `json:"global_state"`

	// Generic Compliance & Traceability Metadata
	Metadata map[string]string `json:"metadata"`

	// Current step execution tracking
	CurrentStepReq     *pb.SubmitStepRequest  `json:"current_step_req"`
	ActiveExecution    *pb.ActiveJobExecution `json:"active_execution"`
	CurrentStepIndex   int                    `json:"current_step_index"`
	PendingActions     []ActionItem           `json:"pending_actions"`
	CurrentActionIndex int                    `json:"current_action_index"`

	mu sync.Mutex
}

func (ps *ProcessingState) Lock() {
	ps.mu.Lock()
}

func (ps *ProcessingState) Unlock() {
	ps.mu.Unlock()
}

type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
}

type ActionFunc func(ctx context.Context, state *ProcessingState, step *pb.StepDefinition) (string, error)

type Engine struct {
	aiClient          *ai_client.Client
	pub               TracePublisher
	primitiveHandlers map[string]ActionFunc
}

func NewEngine(aiClient *ai_client.Client, pub TracePublisher) *Engine {
	e := &Engine{
		aiClient: aiClient,
		pub:      pub,
	}
	e.primitiveHandlers = map[string]ActionFunc{
		"PRIMITIVE_AI_STEP":    e.executeAIStep,
		"PRIMITIVE_HUMAN_STEP": e.executeHumanValidationStep,
	}
	return e
}

func (e *Engine) executeHumanValidationStep(ctx context.Context, state *ProcessingState, step *pb.StepDefinition) (string, error) {
	taskID, err := e.triggerHumanValidation(ctx, state, step, step.ContextInput, "Client triggered manual validation")
	if err != nil {
		return "", fmt.Errorf("failed to create human validation task: %w", err)
	}
	log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobID)
	return "", ErrPendingHumanValidation
}

func (e *Engine) generatePlan(ctx context.Context, state *ProcessingState, step *pb.StepDefinition) (*ActionPlan, error) {
	log.Println("INFO: [Engine] Generating plan for step:", step.StepName)
	stateJson, _ := json.MarshalIndent(state.GlobalState, "", "  ")
	plannerPrompt := fmt.Sprintf("You are an AI planner for a document orchestrator. Based on the client instructions and current state, decide the linear sequence of actions to take.\n\nInstructions: %s\nTarget Schema: %s\nCurrent State: %s\n\nReturn the actions array.", step.CustomInstructions, step.ExpectedSchema, string(stateJson))
	log.Println("DEBUG: [Engine] Planner Prompt:", plannerPrompt)

	log.Println("INFO: [Engine] Sending planning request to AI service...")

	appID := ""
	tenantID := ""
	if state.Metadata != nil {
		appID = state.Metadata["app_id"]
		tenantID = state.Metadata["tenant_id"]
	}

	resp, err := e.aiClient.PerformAction(ctx, appID, tenantID, state.JobID, nil, " ", plannerPrompt, PlannerSchema)
	if err != nil {
		log.Printf("ERROR: [Engine] Failed to generate plan from AI service: %v", err)
		return nil, fmt.Errorf("failed to generate plan: %w", err)
	}
	log.Println("INFO: [Engine] Successfully received plan from AI service.")

	var plan ActionPlan
	if err := json.Unmarshal([]byte(resp.GetActionResultJson()), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse action plan: %w", err)
	}

	return &plan, nil
}

func (e *Engine) ExecuteWorkflow(ctx context.Context, state *ProcessingState, req *pb.SubmitStepRequest) (string, string, string, error) {
	if state.CurrentStepReq == nil || state.CurrentStepReq != req {
		state.CurrentStepReq = req
		state.ActiveExecution = req.ActiveExecution
		state.CurrentStepIndex = 0
		state.PendingActions = nil
		state.CurrentActionIndex = 0
	}

	var lastStepName string
	lastStepId := ""
	if state.ActiveExecution != nil {
		lastStepId = state.ActiveExecution.CurrentStepId
	}

	for state.CurrentStepIndex < len(req.Steps) {
		step := req.Steps[state.CurrentStepIndex]
		lastStepName = step.StepName
		lastStepId = step.Id

		if step.CustomInstructions == "" || step.ExpectedSchema == "" {
			return "", lastStepName, lastStepId, fmt.Errorf("step %s is missing required CustomInstructions or ExpectedSchema", step.StepName)
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

			actionStep := &pb.StepDefinition{
				Id:                 step.Id,
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

func (e *Engine) executeAIStep(ctx context.Context, state *ProcessingState, req *pb.StepDefinition) (string, error) {
	log.Printf("INFO: [Engine] Executing AI step: %s", req.StepName)
	prompt := req.CustomInstructions
	schema := req.ExpectedSchema

	if prompt == "" || schema == "" {
		return "", fmt.Errorf("prompt and schema must be provided for AI step")
	}

	if req.ContextInput != "" {
		prompt = fmt.Sprintf("%s\n\n[CONTEXT PROVIDED BY CLIENT]:\n%s\n", prompt, req.ContextInput)
	}

	statePayload := ""
	if req.ReadsFromStateKey != "" {
		val, ok := state.GlobalState[req.ReadsFromStateKey]
		if ok {
			stateBytes, _ := json.MarshalIndent(val, "", "  ")
			statePayload = string(stateBytes)
		}
	} else {
		stateBytes, _ := json.MarshalIndent(state.GlobalState, "", "  ")
		statePayload = string(stateBytes)
	}
	prompt = fmt.Sprintf("%s\n\n[CURRENT STATE]:\n%s\n", prompt, statePayload)

	log.Println("DEBUG: [Engine] AI Step Prompt:", prompt)

	appID := ""
	tenantID := ""
	if state.Metadata != nil {
		appID = state.Metadata["app_id"]
		tenantID = state.Metadata["tenant_id"]
	}

	e.publishStepTrace(ctx, state, req, "STARTED", "", "")

	// Single AI execution against the generic state payload
	resp, err := e.aiClient.PerformAction(ctx, appID, tenantID, state.JobID, nil, statePayload, prompt, schema)
	if err != nil {
		log.Printf("ERROR: [Engine] AI service failed for step %s: %v", req.StepName, err)
		e.publishStepTrace(ctx, state, req, "FAILED", "", err.Error())
		return "", fmt.Errorf("failed to process step %s: %w", req.StepName, err)
	}

	log.Printf("INFO: [Engine] AI service completed for step %s", req.StepName)
	e.publishStepTrace(ctx, state, req, "COMPLETED", resp.GetActionResultJson(), "")

	return resp.GetActionResultJson(), nil
}

func (e *Engine) triggerHumanValidation(ctx context.Context, state *ProcessingState, req *pb.StepDefinition, failedOutput string, errMsg string) (string, error) {
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

func (e *Engine) publishStepTrace(ctx context.Context, state *ProcessingState, req *pb.StepDefinition, status string, extractedJson string, valErr string) {
	if e.pub != nil {
		var evType events.EventType
		if status == "STARTED" {
			evType = events.EventChunkStarted
		} else if status == "FAILED" {
			evType = events.EventChunkFailed
		} else {
			evType = events.EventChunkCompleted
		}

		tenantID := ""
		userID := ""
		appID := ""
		documentType := ""
		correlationID := ""

		if state.Metadata != nil {
			tenantID = state.Metadata["tenant_id"]
			userID = state.Metadata["user_id"]
			appID = state.Metadata["app_id"]
			documentType = state.Metadata["document_type"]
			correlationID = state.Metadata["correlation_id"]
		}

		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
			EventType:     evType,
			Timestamp:     time.Now(),
			TenantID:      tenantID,
			UserID:        userID,
			AppID:         appID,
			JobID:         state.JobID,
			DocumentType:  documentType,
			CorrelationID: correlationID,
			JobAction:     req.StepName,
			Stage:         "ActionExecution",
			Message:       status,
			Error:         valErr,
			ChunkID:       req.StepName,
		})
	}
}
