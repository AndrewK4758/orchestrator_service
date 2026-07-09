package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
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
          "schema": { "type": "object" },
          "rules": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["action_type", "prompt", "schema"]
      }
    }
  },
  "required": ["actions"]
}`

const ReviewSchema = `{
  "type": "object",
  "properties": {
    "approved": { "type": "boolean" },
    "reason": { "type": "string" }
  },
  "required": ["approved", "reason"]
}`

const ValidationSchema = `{
  "type": "object",
  "properties": {
    "valid": { "type": "boolean" },
    "reason": { "type": "string" }
  },
  "required": ["valid", "reason"]
}`

type ActionItem struct {
	ActionType string          `json:"action_type"`
	Prompt     string          `json:"prompt"`
	Schema     json.RawMessage `json:"schema"`
	Rules      []string        `json:"rules"`
}

type ActionPlan struct {
	Actions []ActionItem `json:"actions"`
}

type TracePublisher interface {
	PublishTrace(ctx context.Context, event events.TraceabilityEvent) error
}

type ActionFunc func(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition) (string, error)

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

func (e *Engine) executeHumanValidationStep(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition) (string, error) {
	taskID, err := e.triggerHumanValidation(ctx, state, step, step.ContextInput, "Client triggered manual validation")
	if err != nil {
		return "", fmt.Errorf("failed to create human validation task: %w", err)
	}
	log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobId)
	return "", ErrPendingHumanValidation
}

func (e *Engine) generatePlan(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition) (*ActionPlan, error) {
	log.Println("INFO: [Engine] Generating plan for step:", step.StepName)

	statePayload := "{}"
	if state.GlobalState != nil {
		stateBytes, _ := state.GlobalState.MarshalJSON()
		statePayload = string(stateBytes)
	}

	plannerPrompt := fmt.Sprintf("You are an AI planner for a document orchestrator. Based on the client instructions and current state, decide the linear sequence of actions to take.\n\nClient Instructions: %s\nTarget Schema: %s\nCurrent State: %s\n\nOutput an actions array. For a standard extraction, output a single PRIMITIVE_AI_STEP with the client's instructions and schema.", step.CustomInstructions, step.ExpectedSchema, statePayload)
	log.Println("DEBUG: [Engine] Planner Prompt:", plannerPrompt)

	actionDef := &pb.ActionDefinition{
		Name:                  "Generate Plan",
		OrchestratorPrimitive: "PLANNING_PRIMITIVE",
		Prompt:                plannerPrompt,
		ExpectedOutputSchema:  PlannerSchema,
	}

	log.Println("INFO: [Engine] Sending planning request to AI service...")

	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, step.ContextInput)
	if err != nil {
		log.Printf("ERROR: [Engine] Failed to generate plan from AI service: %v", err)
		return nil, fmt.Errorf("failed to generate plan: %w", err)
	}
	log.Println("INFO: [Engine] Successfully received plan from AI service.")

	var plan ActionPlan
	if err := json.Unmarshal([]byte(resp.GetActionResultJson()), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse action plan: %w", err)
	}

	mergeIntoActionState(state, resp.GetActionResultJson())

	return &plan, nil
}

func (e *Engine) reviewPlan(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition, plan *ActionPlan) (bool, string, error) {
	reviewPrompt := fmt.Sprintf("You are a strict reviewer. Review the proposed plan found in the local Action State for the step.\n\nStep Instructions: %s\nExpected Output Schema: %s\n\nDoes this plan satisfy the step's goal and schema?", step.CustomInstructions, step.ExpectedSchema)

	actionDef := &pb.ActionDefinition{
		Name:                  "Review Plan",
		OrchestratorPrimitive: "REVIEW_PRIMITIVE",
		Prompt:                reviewPrompt,
		ExpectedOutputSchema:  ReviewSchema,
	}

	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, step.ContextInput)
	if err != nil {
		return false, "", fmt.Errorf("failed to review plan: %w", err)
	}

	var result struct {
		Approved bool   `json:"approved"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(resp.GetActionResultJson()), &result); err != nil {
		return false, "", fmt.Errorf("failed to parse review result: %w", err)
	}

	mergeIntoActionState(state, resp.GetActionResultJson())

	return result.Approved, result.Reason, nil
}

func (e *Engine) assembleStepOutput(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition) (string, error) {
	assemblyPrompt := fmt.Sprintf("You are an assembler. Combine the local Action State into a single JSON object that strictly matches the expected schema for the step.\n\nStep Instructions: %s\nExpected Output Schema: %s\n\nReturn ONLY the JSON object.", step.CustomInstructions, step.ExpectedSchema)

	actionDef := &pb.ActionDefinition{
		Name:                  "Assemble Output",
		OrchestratorPrimitive: "ASSEMBLY_PRIMITIVE",
		Prompt:                assemblyPrompt,
		ExpectedOutputSchema:  step.ExpectedSchema,
	}
	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, step.ContextInput)
	if err != nil {
		return "", fmt.Errorf("failed to assemble step output: %w", err)
	}
	return resp.GetActionResultJson(), nil
}

func (e *Engine) validateStepOutput(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition, assembledJson string) (bool, string, error) {
	validationPrompt := fmt.Sprintf("You are a strict validator. Validate that the assembled output strictly matches the expected schema for the step.\n\nStep Instructions: %s\nExpected Output Schema: %s\n\nAssembled Output:\n%s\n\nDoes the output satisfy the expected schema?", step.CustomInstructions, step.ExpectedSchema, assembledJson)

	actionDef := &pb.ActionDefinition{
		Name:                  "Validate Output",
		OrchestratorPrimitive: "VALIDATION_PRIMITIVE",
		Prompt:                validationPrompt,
		ExpectedOutputSchema:  ValidationSchema,
	}
	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, step.ContextInput)
	if err != nil {
		return false, "", fmt.Errorf("failed to validate step: %w", err)
	}

	var result struct {
		Valid  bool   `json:"valid"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(resp.GetActionResultJson()), &result); err != nil {
		return false, "", fmt.Errorf("failed to parse validation result: %w", err)
	}

	return result.Valid, result.Reason, nil
}

func (e *Engine) ExecuteWorkflow(ctx context.Context, state *pb.WorkflowContext, req *pb.SubmitStepRequest) (string, string, string, error) {
	if state.ActiveExecution == nil {
		state.ActiveExecution = req.Context.ActiveExecution
	}

	var lastStepName string
	lastStepId := ""
	if state.ActiveExecution != nil {
		lastStepId = state.ActiveExecution.CurrentStepId
	}

	for int(state.CurrentStepIndex) < len(req.Steps) {
		step := req.Steps[state.CurrentStepIndex]
		lastStepName = step.StepName
		lastStepId = step.Id

		if step.CustomInstructions == "" || step.ExpectedSchema == "" {
			return "", lastStepName, lastStepId, fmt.Errorf("step %s is missing required CustomInstructions or ExpectedSchema", step.StepName)
		}

		maxRetries := 3
		stepSuccess := false

		for attempt := 0; attempt <= maxRetries; attempt++ {
			// Initialize fresh Local ActionState for isolated execution scope across the step
			if state.ActionState == nil || len(state.PendingActions) == 0 {
				state.ActionState, _ = structpb.NewStruct(make(map[string]interface{}))
			}

			// PLANNING AND REVIEW LOOP
			if len(state.PendingActions) == 0 {
				// 1. Planning Phase
				plan, err := e.generatePlan(ctx, state, step)
				if err != nil {
					return "", lastStepName, lastStepId, err
				}

				// 2. Review Phase
				approved, reviewReason, err := e.reviewPlan(ctx, state, step, plan)
				if err != nil {
					return "", lastStepName, lastStepId, err
				}

				if !approved {
					log.Printf("INFO: Plan rejected during Review Phase: %s", reviewReason)
					if attempt == maxRetries {
						e.publishEvent(ctx, state, events.EventJobStepFailed, step.StepName, "Review rejected plan after retries", reviewReason)
						taskID, _ := e.triggerHumanValidation(ctx, state, step, "Plan Rejected: "+reviewReason, "Please correct the instructions or schema.")
						return "", lastStepName, lastStepId, fmt.Errorf("%w: %s", ErrPendingHumanValidation, taskID)
					}

					e.publishEvent(ctx, state, events.EventPlanRejectedRetry, step.StepName, fmt.Sprintf("Plan rejected, retrying (%d/%d): %s", attempt+1, maxRetries, reviewReason), "")
					continue // Retry Planning!
				}

				// 3. Record Phase
				e.publishEvent(ctx, state, events.EventPlanApproved, step.StepName, "Plan Approved", reviewReason)

				var pbActions []*pb.ActionDefinition
				for i, action := range plan.Actions {
					pbActions = append(pbActions, &pb.ActionDefinition{
						Id:                    fmt.Sprintf("act_%s_%d", step.Id, i),
						Name:                  action.ActionType,
						OrchestratorPrimitive: action.ActionType,
						Prompt:                action.Prompt,
						ExpectedOutputSchema:  string(action.Schema),
						Rules:                 action.Rules,
					})
				}
				state.PendingActions = pbActions
				state.CurrentActionIndex = 0
			}

			// 4. Execution Phase
			var actionError error
			for int(state.CurrentActionIndex) < len(state.PendingActions) {
				action := state.PendingActions[state.CurrentActionIndex]

				handler, exists := e.primitiveHandlers[action.OrchestratorPrimitive]
				if !exists {
					return "", lastStepName, lastStepId, fmt.Errorf("unknown action type from planner: %s", action.OrchestratorPrimitive)
				}

				e.publishEvent(ctx, state, events.EventActionStarted, action.Id, fmt.Sprintf("Started Action: %s", action.Name), "")

				actionStep := &pb.StepDefinition{
					Id:                 step.Id,
					StepName:           fmt.Sprintf("%s - %s", step.StepName, action.OrchestratorPrimitive),
					CustomInstructions: action.Prompt,
					ExpectedSchema:     action.ExpectedOutputSchema,
					ContextInput:       step.ContextInput,
				}

				resJson, err := handler(ctx, state, actionStep)
				if err != nil {
					actionError = err
					break // Break execution loop
				}

				// The AI handler produces mutated JSON reflecting the new ActionState
				if resJson != "" {
					mergeIntoActionState(state, resJson)
				}

				e.publishEvent(ctx, state, events.EventActionCompleted, action.Id, "Action Completed", "")
				state.CurrentActionIndex++
			}

			if actionError != nil {
				if errors.Is(actionError, ErrPendingHumanValidation) {
					// Hard block, human intervention required inside the action primitive
					e.publishEvent(ctx, state, events.EventJobStepPendingHuman, state.PendingActions[state.CurrentActionIndex].Id, "Action Paused for Human", "")
					return "", lastStepName, lastStepId, actionError
				}

				e.publishEvent(ctx, state, events.EventActionFailed, state.PendingActions[state.CurrentActionIndex].Id, "Action Failed", actionError.Error())

				if attempt == maxRetries {
					return "", lastStepName, lastStepId, actionError
				}

				// Force full Re-plan on next retry loop iteration
				state.PendingActions = nil
				state.CurrentActionIndex = 0
				continue
			}

			// 5. Assembly Phase
			assembledJson, err := e.assembleStepOutput(ctx, state, step)
			if err != nil {
				return "", lastStepName, lastStepId, err
			}

			// 6. Validation Phase
			valid, valReason, err := e.validateStepOutput(ctx, state, step, assembledJson)
			if err != nil {
				return "", lastStepName, lastStepId, err
			}

			if !valid {
				log.Printf("INFO: Step Validation Failed: %s", valReason)
				if attempt == maxRetries {
					e.publishEvent(ctx, state, events.EventStepValidationFailed, step.StepName, "Step Validation Failed after retries", valReason)
					taskID, _ := e.triggerHumanValidation(ctx, state, step, "Validation Failed: "+valReason, "The final output did not match the expected schema.")
					return "", lastStepName, lastStepId, fmt.Errorf("%w: %s", ErrPendingHumanValidation, taskID)
				}

				e.publishEvent(ctx, state, events.EventStepValidationFailedRetry, step.StepName, fmt.Sprintf("Validation failed, retrying (%d/%d): %s", attempt+1, maxRetries, valReason), "")

				// Force full Re-plan!
				state.PendingActions = nil
				state.CurrentActionIndex = 0
				continue
			}

			// 7. Success! Commit strictly to the Global State via a direct top-level merge.
			// The client has full control over the schema, so we just merge their exact JSON.
			if assembledJson != "" {
				var parsed map[string]interface{}
				if err := json.Unmarshal([]byte(assembledJson), &parsed); err == nil {
					if state.GlobalState == nil {
						state.GlobalState, _ = structpb.NewStruct(make(map[string]interface{}))
					}

					m := state.GlobalState.AsMap()
					for k, v := range parsed {
						m[k] = v
					}
					newState, _ := structpb.NewStruct(m)
					state.GlobalState = newState
				}
			}

			stepSuccess = true
			break
		}

		if !stepSuccess {
			// Redundant catch-all in case the loop exits without success and without a return
			return "", lastStepName, lastStepId, fmt.Errorf("step %s failed after %d retries", step.StepName, maxRetries)
		}

		// Cleanup inner scope tracking
		state.PendingActions = nil
		state.CurrentActionIndex = 0
		state.ActionState = nil
		state.CurrentStepIndex++
	}

	// Final state json
	var finalJSON []byte
	if state.GlobalState != nil {
		finalJSON, _ = state.GlobalState.MarshalJSON()
	} else {
		finalJSON = []byte("{}")
	}

	return string(finalJSON), lastStepName, lastStepId, nil
}

func (e *Engine) executeAIStep(ctx context.Context, state *pb.WorkflowContext, req *pb.StepDefinition) (string, error) {
	log.Printf("INFO: [Engine] Executing AI step: %s", req.StepName)
	prompt := req.CustomInstructions

	if prompt == "" || req.ExpectedSchema == "" {
		return "", fmt.Errorf("prompt and schema must be provided for AI step")
	}

	log.Println("DEBUG: [Engine] AI Step Prompt:", prompt)

	var currentAction *pb.ActionDefinition
	if state.PendingActions != nil && len(state.PendingActions) > int(state.CurrentActionIndex) {
		currentAction = state.PendingActions[state.CurrentActionIndex]
	}

	// The AI service natively extracts the schema and instructions from the ActionDefinition,
	// and automatically appends the GlobalState and ActionState from the workflowCtx.
	// We only need to pass the raw context input as the document text.
	resp, err := e.aiClient.PerformAction(ctx, state, currentAction, nil, req.ContextInput)
	if err != nil {
		log.Printf("ERROR: [Engine] AI service failed for step %s: %v", req.StepName, err)
		return "", fmt.Errorf("failed to process step %s: %w", req.StepName, err)
	}

	log.Printf("INFO: [Engine] AI service completed for step %s", req.StepName)

	return resp.GetActionResultJson(), nil
}

func (e *Engine) triggerHumanValidation(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition, failedOutput string, errMsg string) (string, error) {
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
		JobId:              state.JobId,
		StepName:           step.StepName,
		FailedAiOutput:     failedOutput,
		ExpectedSchema:     step.ExpectedSchema,
		PromptInstructions: step.CustomInstructions,
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

// publishEvent wraps the standard publisher logic for generic events
func (e *Engine) publishEvent(ctx context.Context, state *pb.WorkflowContext, evType events.EventType, chunkID string, message string, valErr string) {
	if e.pub != nil {
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
			JobID:         state.JobId,
			DocumentType:  documentType,
			CorrelationID: correlationID,
			Stage:         "ReActExecution",
			Message:       message,
			Error:         valErr,
			ChunkID:       chunkID,
		})
	}
}

func mergeIntoActionState(state *pb.WorkflowContext, jsonStr string) {
	if jsonStr == "" {
		return
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return
	}

	if state.ActionState == nil {
		state.ActionState, _ = structpb.NewStruct(parsed)
		return
	}

	currentMap := state.ActionState.AsMap()
	for k, v := range parsed {
		currentMap[k] = v
	}
	newState, _ := structpb.NewStruct(currentMap)
	state.ActionState = newState
}
