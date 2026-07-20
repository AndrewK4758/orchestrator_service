package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"orchestrator_service/internal/logger"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"

	"github.com/xeipuuv/gojsonschema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
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
	PublishCacheStore(ctx context.Context, data []byte) error
}

type EngineState struct {
	Req                *pb.ExecuteWorkflowNodeRequest
	JobId              string
	CurrentActionIndex int32
	PendingActions     []*pb.ActionDefinition
	ActionState        *structpb.Struct
}

type AIClient interface {
	PerformAction(ctx context.Context, workflowCtx *pb.ExecuteWorkflowNodeRequest, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error)
}

type ActionFunc func(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (string, error)

type Engine struct {
	aiClient          AIClient
	cacheClient       pb.SemanticCacheServiceClient
	pub               TracePublisher
	primitiveHandlers map[string]ActionFunc
	pendingCacheJobs  sync.Map
	chunkingClient    pb.DocumentProcessingServiceClient
}

func NewEngine(aiClient AIClient, cacheClient pb.SemanticCacheServiceClient, pub TracePublisher) *Engine {
	e := &Engine{
		aiClient:    aiClient,
		cacheClient: cacheClient,
		pub:         pub,
	}
	e.primitiveHandlers = make(map[string]ActionFunc)
	e.primitiveHandlers["PRIMITIVE_AI_STEP"] = e.executeAIStep
	e.primitiveHandlers["PRIMITIVE_HUMAN_STEP"] = e.executeHumanValidationStep
	e.primitiveHandlers["PRIMITIVE_ZERO_SHOT_CLASSIFICATION"] = e.executeZeroShotClassification
	e.primitiveHandlers["PRIMITIVE_SEMANTIC_CACHE_STORE"] = e.executeSemanticCacheStore
	e.primitiveHandlers["PRIMITIVE_DOCUMENT_CHUNKING"] = e.executeDocumentChunking

	return e
}

func (e *Engine) SetChunkingClient(client pb.DocumentProcessingServiceClient) {
	e.chunkingClient = client
}

func (e *Engine) executeHumanValidationStep(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (string, error) {
	taskID, err := e.triggerHumanValidation(ctx, state, req, "", "Client triggered manual validation")
	if err != nil {
		return "", fmt.Errorf("failed to create human validation task: %w", err)
	}
	logger.Info("Engine", "Successfully registered human validation task: %s for Job: %s", taskID, state.JobId)
	return "", ErrPendingHumanValidation
}

func (e *Engine) generatePlan(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (*ActionPlan, error) {
	logger.Info("Engine", "[JobID: %s | StepID: %s] Generating plan for step: %s\n", state.JobId, req.StepId, req.NodeConfig.NodeName)

	if req.NodeConfig.PrimitiveType != pb.OrchestratorPrimitive_PRIMITIVE_UNSPECIFIED {
		primitiveStr := req.NodeConfig.PrimitiveType.String()
		if primitiveStr != "" {
			logger.Info("Engine", "[JobID: %s | StepID: %s] Client explicitly defined primitive %s. Bypassing AI Planner.", state.JobId, req.StepId, primitiveStr)
			return &ActionPlan{
				Actions: []ActionItem{
					{
						ActionType: primitiveStr,
						Prompt:     req.NodeConfig.SystemInstructions,
						Schema:     []byte(req.NodeConfig.ExpectedSchema),
					},
				},
			}, nil
		}
	}

	if req.NodeConfig.HumanValidation != nil && req.NodeConfig.HumanValidation.Required {
		logger.Info("Engine", "[JobID: %s | StepID: %s] Step requires explicit human review. Bypassing AI Planner.", state.JobId, req.StepId)
		return &ActionPlan{
			Actions: []ActionItem{
				{
					ActionType: "PRIMITIVE_HUMAN_STEP",
					Prompt:     req.NodeConfig.SystemInstructions,
					Schema:     []byte(req.NodeConfig.ExpectedSchema),
				},
			},
		}, nil
	}

	planningPrompt := fmt.Sprintf(`You are the Orchestrator AI Planner. Your job is to sequence the workflow for this step.
Step Instructions: %s
Expected Schema: %s

Available Primitives:
- PRIMITIVE_AI_STEP: Use this for AI inference.
- PRIMITIVE_HUMAN_STEP: Use this if human validation is explicitly requested.

CRITICAL INSTRUCTIONS: Exercise 0 creativity. Strictly forbid guesswork, fallback rules, or app-specific logic. The workflow must be generated purely based on the explicit instructions and expected schema, without injecting external domain knowledge.

Return a JSON array of actions matching the PlannerSchema.`, req.NodeConfig.SystemInstructions, req.NodeConfig.ExpectedSchema)

	actionDef := &pb.ActionDefinition{
		Name:                  "Generate Plan",
		OrchestratorPrimitive: "PLANNING_PRIMITIVE",
		Prompt:                planningPrompt,
		ExpectedOutputSchema:  PlannerSchema,
	}

	resp, err := e.aiClient.PerformAction(ctx, req, actionDef, nil, "", pb.ModelChoice_MODEL_CHOICE_PLANNING)
	if err != nil {
		return nil, fmt.Errorf("action failed for job %s: failed to generate plan: %w", state.JobId, err)
	}

	rawJson := cleanJSON(resp.GetActionResultJson())

	// We still merge the plan into ActionState as if it was returned by the AI, for consistency
	if err := mergeIntoActionState(state, rawJson); err != nil {
		return nil, fmt.Errorf("action failed for job %s: failed to merge plan state: %w", state.JobId, err)
	}

	var plan ActionPlan
	if err := json.Unmarshal([]byte(cleanJSON(rawJson)), &plan); err != nil {
		return nil, fmt.Errorf("action failed for job %s: failed to parse plan result: %w", state.JobId, err)
	}

	return &plan, nil
}

func (e *Engine) reviewPlan(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest, plan *ActionPlan) (bool, string, error) {
	logger.Info("Engine", "[JobID: %s | StepID: %s] Reviewing plan dynamically.", state.JobId, req.StepId)

	var actionsStr string
	if plan != nil && len(plan.Actions) > 0 {
		for i, a := range plan.Actions {
			actionsStr += fmt.Sprintf("Action %d: %s\n", i, a.ActionType)
		}
	} else {
		actionsStr = "No actions proposed."
	}

	reviewPrompt := fmt.Sprintf(`You are the Orchestrator AI Reviewer. Your job is to review the proposed plan to ensure it is logical and satisfies the step instructions.
Step Instructions: %s
Expected Schema: %s

Proposed Actions:
%s

CRITICAL INSTRUCTIONS: Exercise 0 creativity. Strictly forbid guesswork, fallback rules, or app-specific logic. Ensure the proposed plan strictly adheres to the provided instructions without injecting external domain knowledge.

Review the actions. If they form a valid plan to achieve the instructions, approve it. If not, reject it with a reason.
Return a JSON object matching the expected schema.`, req.NodeConfig.SystemInstructions, req.NodeConfig.ExpectedSchema, actionsStr)

	actionDef := &pb.ActionDefinition{
		Name:                  "Review Plan",
		OrchestratorPrimitive: "REVIEW_PRIMITIVE",
		Prompt:                reviewPrompt,
		ExpectedOutputSchema:  ReviewSchema,
	}

	resp, err := e.aiClient.PerformAction(ctx, req, actionDef, nil, "", pb.ModelChoice_MODEL_CHOICE_CLASSIFICATION)
	if err != nil {
		return false, "", fmt.Errorf("action failed for job %s: failed to review plan: %w", state.JobId, err)
	}

	rawJson := cleanJSON(resp.GetActionResultJson())
	if err := mergeIntoActionState(state, rawJson); err != nil {
		return false, "", fmt.Errorf("action failed for job %s: failed to merge review state: %w", state.JobId, err)
	}

	var result struct {
		Approved bool   `json:"approved"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(cleanJSON(rawJson)), &result); err != nil {
		return false, "", fmt.Errorf("action failed for job %s: failed to parse review result: %w", state.JobId, err)
	}

	return result.Approved, result.Reason, nil
}

func (e *Engine) assembleStepOutput(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (string, error) {
	actionStateJson := "{}"
	if state.ActionState != nil {
		bytes, err := state.ActionState.MarshalJSON()
		if err == nil {
			actionStateJson = string(bytes)
		}
	}

	assemblyPrompt := "You are an assembler. Combine the local Action State into a single JSON object that strictly matches the expected schema for the step.\n\nCRITICAL INSTRUCTIONS: Exercise 0 creativity. Strictly forbid guesswork, fallback rules, or app-specific logic. Do not synthesize or hallucinate data.\n\nReturn ONLY the JSON object."

	actionDef := &pb.ActionDefinition{
		Name:                  "Assemble Output",
		OrchestratorPrimitive: "ASSEMBLY_PRIMITIVE",
		Prompt:                assemblyPrompt,
		ExpectedOutputSchema:  req.NodeConfig.ExpectedSchema,
	}

	inputText := fmt.Sprintf("Action State to Assemble:\n%s", actionStateJson)

	resp, err := e.aiClient.PerformAction(ctx, req, actionDef, nil, inputText, pb.ModelChoice_MODEL_CHOICE_EXTRACTION)
	if err != nil {
		return "", fmt.Errorf("failed to assemble step output: %w", err)
	}
	return resp.GetActionResultJson(), nil
}

func (e *Engine) validateStepOutput(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest, assembledJson string) (bool, string, error) {
	logger.Info("Engine", "[JobID: %s | StepID: %s] Validating assembled output deterministically against JSON schema", state.JobId, req.StepId)
	logger.Info("Engine", "[JobID: %s | StepID: %s] RAW AI OUTPUT:\n%s", state.JobId, req.StepId, assembledJson)

	schemaLoader := gojsonschema.NewStringLoader(req.NodeConfig.ExpectedSchema)
	documentLoader := gojsonschema.NewStringLoader(assembledJson)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		logger.Warn("Engine", "[JobID: %s | StepID: %s] JSON parsing failed during validation: %v", state.JobId, req.StepId, err)
		return false, fmt.Sprintf("The generated output is not valid JSON. Parse error: %v", err), nil
	}

	if result.Valid() {
		return true, "", nil
	}

	var errorMsgs []string
	for _, desc := range result.Errors() {
		errorMsgs = append(errorMsgs, desc.String())
	}

	reason := fmt.Sprintf("The output does not satisfy the expected schema. Please fix the following errors:\n- %s", strings.Join(errorMsgs, "\n- "))

	return false, reason, nil
}

func (e *Engine) ExecuteWorkflow(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (string, string, string, string, error) {
	if req == nil {
		return "", "", "", "", fmt.Errorf("ExecuteWorkflowRequest is nil")
	}
	if req.NodeConfig == nil {
		return "", "", "", "", fmt.Errorf("ExecuteWorkflowRequest.NodeConfig is nil")
	}

	jobId := req.JobId
	if req.Identity != nil && req.Identity.JobId != "" {
		jobId = req.Identity.JobId
	}
	state := &EngineState{
		Req:   req,
		JobId: jobId,
	}

	var lastStepName string
	lastStepId := ""

	lastStepName = req.NodeConfig.NodeName
	lastStepId = req.StepId

	if req.NodeConfig.SystemInstructions == "" || req.NodeConfig.ExpectedSchema == "" {
		return "", "", lastStepName, lastStepId, fmt.Errorf("step %s is missing required CustomInstructions or ExpectedSchema", req.NodeConfig.NodeName)
	}

	logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> STARTING WORKFLOW EXECUTION: %s", state.JobId, req.StepId, req.NodeConfig.NodeName)

	maxRetries := 3
	stepSuccess := false

	var finalAssembledJson string

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Initialize fresh Local ActionState for isolated execution scope across the step
		if state.ActionState == nil || len(state.PendingActions) == 0 {
			state.ActionState, _ = structpb.NewStruct(make(map[string]any))
		}

		// PLANNING AND REVIEW LOOP
		if len(state.PendingActions) == 0 {
			// 1. Planning Phase
			logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: GENERATE PLAN", state.JobId, req.StepId)
			plan, err := e.generatePlan(ctx, state, req)
			if err != nil {
				logger.Warn("Engine", "[JobID: %s | StepID: %s] Generate plan failed: %v", state.JobId, req.StepId, err)
				if attempt == maxRetries {
					return "", "", lastStepName, lastStepId, fmt.Errorf("failed to generate plan: %w", err)
				}
				e.publishEvent(ctx, state, events.EventPlanRejectedRetry, req.NodeConfig.NodeName, fmt.Sprintf("Plan generation failed, retrying (%d/%d): %v", attempt+1, maxRetries, err), "")
				continue
			}
			logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: PLAN GENERATED SUCCESSFULLY", state.JobId, req.StepId)

			// 2. Review Phase
			isExplicitPrimitive := (req.NodeConfig.PrimitiveType != pb.OrchestratorPrimitive_PRIMITIVE_UNSPECIFIED) ||
				(req.NodeConfig.HumanValidation != nil && req.NodeConfig.HumanValidation.Required)

			var approved bool
			var reviewReason string

			if isExplicitPrimitive {
				logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: REVIEW PLAN (BYPASSED)", state.JobId, req.StepId)
				approved = true
				reviewReason = "Auto-approved explicitly defined primitive"
			} else {
				logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: REVIEW PLAN", state.JobId, req.StepId)
				approved, reviewReason, err = e.reviewPlan(ctx, state, req, plan)
			}

			if err != nil {
				logger.Warn("Engine", "[JobID: %s | StepID: %s] Review plan failed: %v", state.JobId, req.StepId, err)
				if attempt == maxRetries {
					return "", "", lastStepName, lastStepId, fmt.Errorf("failed to review plan: %w", err)
				}
				e.publishEvent(ctx, state, events.EventPlanRejectedRetry, req.NodeConfig.NodeName, fmt.Sprintf("Plan review failed, retrying (%d/%d): %v", attempt+1, maxRetries, err), "")
				continue
			}

			if !approved {
				logger.Info("Engine", "[JobID: %s | StepID: %s] Plan rejected during Review Phase: %s", state.JobId, req.StepId, reviewReason)
				if attempt == maxRetries {
					e.publishEvent(ctx, state, events.EventJobStepFailed, req.NodeConfig.NodeName, "Review rejected plan after retries", reviewReason)
					taskID, _ := e.triggerHumanValidation(ctx, state, req, "Plan Rejected: "+reviewReason, "Please correct the instructions or schema.")
					return "", "", lastStepName, lastStepId, fmt.Errorf("%w: %s", ErrPendingHumanValidation, taskID)
				}

				e.publishEvent(ctx, state, events.EventPlanRejectedRetry, req.NodeConfig.NodeName, fmt.Sprintf("Plan rejected, retrying (%d/%d): %s", attempt+1, maxRetries, reviewReason), "")
				continue // Retry Planning!
			}

			// 3. Record Phase
			e.publishEvent(ctx, state, events.EventPlanApproved, req.NodeConfig.NodeName, "Plan Approved", reviewReason)

			var pbActions []*pb.ActionDefinition
			for i, action := range plan.Actions {
				primitiveType := pb.OrchestratorPrimitive_PRIMITIVE_UNSPECIFIED
				if val, ok := pb.OrchestratorPrimitive_value[action.ActionType]; ok {
					primitiveType = pb.OrchestratorPrimitive(val)
				}

				pbActions = append(pbActions, &pb.ActionDefinition{
					Id:                    fmt.Sprintf("act_%s_%d", req.StepId, i),
					Name:                  action.ActionType,
					OrchestratorPrimitive: action.ActionType,
					PrimitiveType:         primitiveType,
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
				return "", "", lastStepName, lastStepId, fmt.Errorf("unknown action type from planner: %s", action.OrchestratorPrimitive)
			}

			logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> EXECUTING ACTION: %s", state.JobId, req.StepId, action.Name)
			e.publishEvent(ctx, state, events.EventActionStarted, action.Id, fmt.Sprintf("Started Action: %s", action.Name), "")

			actionReq := &pb.ExecuteWorkflowNodeRequest{
				JobId:       req.JobId,
				StepId:      req.StepId,
				GlobalState: req.GlobalState,
				Metadata:    req.Metadata,
				NodeConfig: &pb.NodeConfiguration{
					NodeName:           fmt.Sprintf("%s - %s", req.NodeConfig.NodeName, action.OrchestratorPrimitive),
					SystemInstructions: action.Prompt,
					ExpectedSchema:     action.ExpectedOutputSchema,
					Cache:              req.NodeConfig.Cache,
					HumanValidation:    req.NodeConfig.HumanValidation,
				},
			}

			resJson, err := handler(ctx, state, actionReq)
			if err != nil {
				actionError = err
				break // Break execution loop
			}

			// The AI handler produces mutated JSON reflecting the new ActionState
			if resJson != "" {
				if err := mergeIntoActionState(state, resJson); err != nil {
					actionError = fmt.Errorf("failed to merge action state: %w", err)
					break // Break execution loop
				}
			}

			e.publishEvent(ctx, state, events.EventActionCompleted, action.Id, "Action Completed", "")
			state.CurrentActionIndex++
		}

		if actionError != nil {
			if errors.Is(actionError, ErrPendingHumanValidation) {
				// Hard block, human intervention required inside the action primitive
				e.publishEvent(ctx, state, events.EventJobStepPendingHuman, state.PendingActions[state.CurrentActionIndex].Id, "Action Paused for Human", "")
				return "", "", lastStepName, lastStepId, actionError
			}

			e.publishEvent(ctx, state, events.EventActionFailed, state.PendingActions[state.CurrentActionIndex].Id, "Action Failed", actionError.Error())

			if attempt == maxRetries {
				return "", "", lastStepName, lastStepId, actionError
			}

			// Force full Re-plan on next retry loop iteration
			state.PendingActions = nil
			state.CurrentActionIndex = 0
			continue
		}

		var assembledJson string
		var earlyTerminated bool

		if state.ActionState != nil {
			actionMap := state.ActionState.AsMap()
			term, _ := actionMap["early_termination"].(bool)
			ploadStr, hasPayload := actionMap["extracted_payload"].(string)

			if term && hasPayload {
				assembledJson = ploadStr
				earlyTerminated = true
				logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: EARLY TERMINATION DETECTED. SKIPPING ASSEMBLY AND VALIDATION.", state.JobId, req.StepId)
			}
		}

		if !earlyTerminated {
			// 5. Assembly Phase
			var err error
			if len(state.PendingActions) == 1 {
				logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: ASSEMBLE OUTPUT (BYPASSED - SINGLE ACTION)", state.JobId, req.StepId)
				if state.ActionState != nil {
					bytes, marshalErr := state.ActionState.MarshalJSON()
					if marshalErr == nil {
						assembledJson = string(bytes)
					} else {
						err = fmt.Errorf("failed to marshal single action state: %w", marshalErr)
					}
				} else {
					assembledJson = "{}"
				}
			} else {
				logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: ASSEMBLE OUTPUT", state.JobId, req.StepId)
				assembledJson, err = e.assembleStepOutput(ctx, state, req)
			}

			if err != nil {
				return "", "", lastStepName, lastStepId, fmt.Errorf("failed to assemble step output: %w", err)
			}

			// 6. Validation Phase
			logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> PHASE: VALIDATE OUTPUT", state.JobId, req.StepId)
			valid, valReason, err := e.validateStepOutput(ctx, state, req, assembledJson)
			if err != nil {
				return "", "", lastStepName, lastStepId, fmt.Errorf("failed to validate step output: %w", err)
			}

			if !valid {
				logger.Info("Engine", "[JobID: %s | StepID: %s] Step Validation Failed: %s", state.JobId, req.StepId, valReason)
				if attempt == maxRetries {
					e.publishEvent(ctx, state, events.EventStepValidationFailed, req.NodeConfig.NodeName, "Step Validation Failed after retries", valReason)
					taskID, _ := e.triggerHumanValidation(ctx, state, req, "Validation Failed: "+valReason, "The final output did not match the expected schema.")
					return "", "", lastStepName, lastStepId, fmt.Errorf("%w: %s", ErrPendingHumanValidation, taskID)
				}

				e.publishEvent(ctx, state, events.EventStepValidationFailedRetry, req.NodeConfig.NodeName, fmt.Sprintf("Validation failed, retrying (%d/%d): %s", attempt+1, maxRetries, valReason), "")

				// Force full Re-plan!
				state.PendingActions = nil
				state.CurrentActionIndex = 0
				continue
			}
		}

		// We now rely on server.go to merge the final assembled output into the GlobalState
		// and apply the envelope pattern via MergeStateContext.

		stepSuccess = true
		finalAssembledJson = assembledJson
		break
	}

	if !stepSuccess {
		// Redundant catch-all in case the loop exits without success and without a return
		return "", "", lastStepName, lastStepId, fmt.Errorf("step %s failed after %d retries", req.NodeConfig.NodeName, maxRetries)
	}

	// Cleanup inner scope tracking
	state.PendingActions = nil
	state.CurrentActionIndex = 0
	state.ActionState = nil

	// Final state json
	var finalJSON []byte
	finalJSON, _ = req.GlobalState.MarshalJSON()

	logger.Debug("Engine", "[EXECUTION MAP] [JobID: %s | StepID: %s] -> WORKFLOW EXECUTION SUCCESS: %s", state.JobId, req.StepId, req.NodeConfig.NodeName)
	return string(finalJSON), finalAssembledJson, lastStepName, lastStepId, nil
}

func (e *Engine) executeAIStep(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (string, error) {
	var currentAction *pb.ActionDefinition
	var actionName string
	if state.PendingActions != nil && len(state.PendingActions) > int(state.CurrentActionIndex) {
		currentAction = state.PendingActions[state.CurrentActionIndex]
		actionName = currentAction.Name
	}

	logger.Info("Engine", "[JobID: %s | StepID: %s | Action: %s] Executing AI step: %s", state.JobId, req.StepId, actionName, req.NodeConfig.NodeName)
	prompt := req.NodeConfig.SystemInstructions

	if prompt == "" || req.NodeConfig.ExpectedSchema == "" {
		return "", fmt.Errorf("prompt and schema must be provided for AI step")
	}

	if state.ActionState != nil {
		actionStateMap := state.ActionState.AsMap()
		hit, _ := actionStateMap["cache_hit"].(bool)
		payload, hasPayload := actionStateMap["extracted_payload"]

		if hit && hasPayload {
			logger.Info("Engine", "[JobID: %s | StepID: %s] Cache hit detected. Skipping AI inference.", state.JobId, req.StepId)
			bytes, _ := json.Marshal(payload)
			return string(bytes), nil
		}
	}

	logger.Debug("Engine", "[JobID: %s | StepID: %s | Action: %s] AI Step Prompt: %s\n", state.JobId, req.StepId, actionName, prompt)

	// The AI service natively extracts the schema and instructions from the ActionDefinition,
	// and automatically appends the GlobalState and ActionState from the workflowCtx.
	// We only need to pass the raw context input as the document text.
	resp, err := e.aiClient.PerformAction(ctx, req, currentAction, nil, "", pb.ModelChoice_MODEL_CHOICE_EXTRACTION)
	if err != nil {
		logger.Error("Engine", "[JobID: %s | StepID: %s | Action: %s] AI service failed for step %s: %v", state.JobId, req.StepId, actionName, req.NodeConfig.NodeName, err)
		return "", fmt.Errorf("failed to process step %s: %w", req.NodeConfig.NodeName, err)
	}

	logger.Info("Engine", "[JobID: %s | StepID: %s | Action: %s] AI service completed for step %s", state.JobId, req.StepId, actionName, req.NodeConfig.NodeName)

	rawJson := cleanJSON(resp.GetActionResultJson())
	return rawJson, nil
}

func (e *Engine) executeSemanticCacheStore(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (string, error) {
	logger.Cache("Engine", "[JobID: %s | StepID: %s] Executing Cache Store Step: %s", state.JobId, req.StepId, req.NodeConfig.NodeName)

	if e.pub == nil {
		return "", fmt.Errorf("publisher is not configured")
	}

	if state.ActionState != nil {
		actionStateMap := state.ActionState.AsMap()
		hit, _ := actionStateMap["cache_hit"].(bool)
		if hit {
			logger.Cache("Engine", "[JobID: %s | StepID: %s] Cache hit was detected previously. Skipping Store.", state.JobId, req.StepId)
			return "{}", nil
		}
	}

	// We need to fetch the JSON payload to store. We assume it's currently in the GlobalState,
	// or passed down via some assembled step. For now, we take the entire GlobalState as the cached representation.
	payload := "{}"
	if req.GlobalState != nil {
		bytes, _ := req.GlobalState.MarshalJSON()
		payload = string(bytes)
	}

	useCache := false
	if req.NodeConfig != nil && req.NodeConfig.Cache != nil {
		useCache = req.NodeConfig.Cache.Enabled
	}
	cacheInput := ""

	collectionName := "default_collection"

	if req.NodeConfig != nil && req.NodeConfig.Cache != nil {
		cacheInput = req.NodeConfig.Cache.LookupInput
		if req.NodeConfig.Cache.CollectionName != "" {
			collectionName = req.NodeConfig.Cache.CollectionName
		}
	}

	if useCache {
		metadataStruct, _ := structpb.NewStruct(make(map[string]any))
		// We'd map req.Metadata to structpb.Struct here if needed, but for now just pass empty Struct
		// or whatever we have. To be perfectly compliant with the new proto:
		for k, v := range req.Metadata {
			metadataStruct.Fields[k] = structpb.NewStringValue(v)
		}

		reqStore := &pb.StoreExtractionRequest{
			CollectionName:   collectionName,
			Text:             cacheInput,
			Identity:         state.Req.Identity,
			ExtractedPayload: payload,
		}

		correlationID := fmt.Sprintf("%s_%s", state.JobId, req.StepId)

		msg := &pb.CacheStoreMessage{
			CorrelationId: correlationID,
			Request:       reqStore,
		}

		bytes, err := protojson.Marshal(msg)
		if err != nil {
			logger.Error("Engine", "[JobID: %s] Failed to marshal cache store msg: %v", state.JobId, err)
		} else {
			err = e.pub.PublishCacheStore(ctx, bytes)
			if err != nil {
				logger.Error("Engine", "[JobID: %s | StepID: %s] Cache store publish failed: %v", state.JobId, req.StepId, err)
			}
		}
	}

	// Clear the stampede lock immediately!
	if useCache {
		hashBytes := sha256.Sum256([]byte(cacheInput))
		hashKey := hex.EncodeToString(hashBytes[:])
		if ch, loaded := e.pendingCacheJobs.LoadAndDelete(hashKey); loaded {
			close(ch.(chan struct{}))
		}
	}

	return "{\"cache_stored\": true}", nil
}

func (e *Engine) triggerHumanValidation(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest, failedOutput string, errMsg string) (string, error) {
	addr := os.Getenv("HUMAN_VALIDATION_SERVICE_ADDRESS")
	if addr == "" {
		addr = "localhost:50056"
	}

	maxMsgSizeMb := 100
	if envVal := os.Getenv("GRPC_MAX_MESSAGE_SIZE_MB"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxMsgSizeMb = parsed
		}
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		return "", fmt.Errorf("failed to dial human validation service: %w", err)
	}
	defer conn.Close()

	client := pb.NewHumanValidationServiceClient(conn)

	jobName := ""
	if req.NodeConfig != nil {
		jobName = req.NodeConfig.NodeName
	}

	globalStateJSON := "{}"
	if req.GlobalState != nil {
		bytes, _ := req.GlobalState.MarshalJSON()
		globalStateJSON = string(bytes)
	}

	createReq := &pb.CreateTaskRequest{
		Identity:           req.Identity,
		StepName:           req.NodeConfig.NodeName,
		FailedAiOutput:     failedOutput,
		ExpectedSchema:     req.NodeConfig.ExpectedSchema,
		PromptInstructions: req.NodeConfig.SystemInstructions,
		ErrorMessage:       errMsg,
		GlobalStateJson:    globalStateJSON,
		JobName:            jobName,
		StepId:             req.StepId,
		StepContextInput:   "",
		Metadata:           req.Metadata,
	}

	timeoutSecs := 10
	if envVal := os.Getenv("GRPC_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			timeoutSecs = parsed
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	resp, err := client.CreateValidationTask(ctx, createReq)
	if err != nil {
		return "", fmt.Errorf("gRPC CreateValidationTask failed: %w", err)
	}

	return resp.GetTaskId(), nil
}

// publishEvent wraps the standard publisher logic for generic events
func (e *Engine) publishEvent(ctx context.Context, state *EngineState, evType events.EventType, chunkID string, message string, valErr string) {
	if e.pub != nil {
		tenantID := ""
		appID := ""
		if state.Req.Identity != nil {
			tenantID = state.Req.Identity.TenantId
			appID = state.Req.Identity.AppId
		}

		eventMetadata := make(map[string]any)
		if state.Req.Metadata != nil {
			for k, v := range state.Req.Metadata {
				eventMetadata[k] = v
			}
		}

		_ = e.pub.PublishTrace(ctx, events.TraceabilityEvent{
			EventID:   fmt.Sprintf("evt_%d", time.Now().UnixNano()),
			EventType: evType,
			Timestamp: time.Now(),
			TenantID:  tenantID,
			AppID:     appID,
			JobID:     state.JobId,
			Stage:     "ReActExecution",
			Message:   message,
			Error:     valErr,
			ChunkID:   chunkID,
			Metadata:  eventMetadata,
		})
	}
}

func deepMerge(dst map[string]any, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	for k, vSrc := range src {
		vDst, exists := dst[k]
		if exists {
			mapDst, okDst := vDst.(map[string]any)
			mapSrc, okSrc := vSrc.(map[string]any)
			if okDst && okSrc {
				deepMerge(mapDst, mapSrc)
				continue
			}
		}
		dst[k] = vSrc
	}
}

func mergeIntoActionState(state *EngineState, jsonStr string) error {
	if jsonStr == "" {
		return nil
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(cleanJSON(jsonStr)), &parsed); err != nil {
		return fmt.Errorf("malformed JSON in action state merge: %w", err)
	}

	if state.ActionState == nil {
		newState, err := structpb.NewStruct(parsed)
		if err != nil {
			return fmt.Errorf("failed to create new struct for action state: %w", err)
		}
		state.ActionState = newState
		return nil
	}

	currentMap := state.ActionState.AsMap()
	deepMerge(currentMap, parsed)

	newState, err := structpb.NewStruct(currentMap)
	if err != nil {
		return fmt.Errorf("failed to update struct for action state: %w", err)
	}
	state.ActionState = newState
	return nil
}

// MergeStateContext handles deep merging step JSON output into the GlobalState
// and enforces the standard Context envelope pattern.
func MergeStateContext(req *pb.ExecuteWorkflowNodeRequest, jsonStr string, targetKey string) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}

	if jsonStr != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
			return fmt.Errorf("malformed JSON in global state merge: %w", err)
		}

		if req.GlobalState == nil {
			req.GlobalState, _ = structpb.NewStruct(make(map[string]any))
		}

		m := req.GlobalState.AsMap()
		if targetKey != "" {
			if existing, ok := m[targetKey].(map[string]any); ok {
				deepMerge(existing, parsed)
				m[targetKey] = existing
			} else {
				m[targetKey] = parsed
			}
		} else {
			deepMerge(m, parsed)
		}

		newState, err := structpb.NewStruct(m)
		if err != nil {
			return fmt.Errorf("failed to update struct for global state: %w", err)
		}
		req.GlobalState = newState
	}

	return nil
}

func (e *Engine) executeZeroShotClassification(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (respStr string, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Engine", "[JobID: %s | StepID: %s] Cache check PANICKED: %v. Falling back to AI execution.", state.JobId, req.StepId, r)
			respStr, err = e.executeAIStep(ctx, state, req)
		}
	}()

	logger.Cache("Engine", "[JobID: %s | StepID: %s] Executing Zero Shot Classification Step: %s", state.JobId, req.StepId, req.NodeConfig.NodeName)

	if e.cacheClient == nil {
		return "", fmt.Errorf("cache client is not configured")
	}

	var resp *pb.CheckCacheResponse
	useCache := false
	if req.NodeConfig != nil && req.NodeConfig.Cache != nil {
		useCache = req.NodeConfig.Cache.Enabled
	}
	cacheInput := ""
	threshold := float32(0.99)
	collectionName := "default_collection"

	if req.NodeConfig != nil && req.NodeConfig.Cache != nil {
		cacheInput = req.NodeConfig.Cache.LookupInput
		if req.NodeConfig.Cache.CollectionName != "" {
			collectionName = req.NodeConfig.Cache.CollectionName
		}
		threshold = req.NodeConfig.Cache.ConfidenceThreshold
	}

	if useCache {

		hashBytes := sha256.Sum256([]byte(cacheInput))
		hashKey := hex.EncodeToString(hashBytes[:])

		if ch, loaded := e.pendingCacheJobs.Load(hashKey); loaded {
			logger.Cache("Engine", "[JobID: %s] CACHE STAMPEDE DELAY! Waiting for identical job...", state.JobId)
			select {
			case <-ch.(chan struct{}):
			case <-time.After(30 * time.Second):
			case <-ctx.Done():
				return "", fmt.Errorf("action failed for job %s: context cancelled while waiting for stampede lock: %w", state.JobId, ctx.Err())
			}
		}

		var err error
		metadataStruct, _ := structpb.NewStruct(make(map[string]any))
		for k, v := range req.Metadata {
			metadataStruct.Fields[k] = structpb.NewStringValue(v)
		}

		reqCheck := &pb.CheckCacheRequest{
			CollectionName: collectionName,
			Text:           cacheInput,
			Identity:       state.Req.Identity,
			Threshold:      threshold,
		}

		resp, err = e.cacheClient.CheckCache(ctx, reqCheck)
		if err != nil {
			logger.Error("Engine", "[JobID: %s | StepID: %s] Cache check failed: %v", state.JobId, req.StepId, err)
			resp = &pb.CheckCacheResponse{Hit: false}
		}

		logText := cacheInput
		if len(logText) > 100 {
			logText = logText[:100] + "..."
		}

		metaBytes, _ := json.Marshal(req.Metadata)
		logger.Cache("Engine", "ZERO-SHOT CACHE EVALUATION [JOBID: %s] - STATE SUBMITTED: [TEXT: %s | METADATA: %s] | OUTCOME (HIT): %t | COSINE SIMILARITY SCORE: %f", state.JobId, logText, string(metaBytes), resp.Hit, resp.Confidence)
		logger.Debug("Engine", "FULL ZERO-SHOT CACHE INPUT TEXT [JOBID: %s]:\n%s\n", state.JobId, cacheInput)

		if !resp.Hit {
			ch := make(chan struct{})
			_, loaded := e.pendingCacheJobs.LoadOrStore(hashKey, ch)
			if !loaded {
				go func() {
					time.Sleep(60 * time.Second)
					if ch, loaded := e.pendingCacheJobs.LoadAndDelete(hashKey); loaded {
						close(ch.(chan struct{}))
					}
				}()
			}
		}
	} else {
		resp = &pb.CheckCacheResponse{Hit: false}
	}

	if resp.Hit && resp.ExtractedPayload != "" {
		logger.Cache("Engine", "[JobID: %s] Zero-Shot Cache hit! Extracted Payload: %s", state.JobId, resp.ExtractedPayload)

		// Fast-fail the remaining actions in the plan to simulate immediate step completion!
		state.PendingActions = nil
		state.CurrentActionIndex = 0

		payloadEscaped, _ := json.Marshal(resp.ExtractedPayload)
		return fmt.Sprintf(`{"cache_hit": true, "early_termination": true, "extracted_payload": %s}`, payloadEscaped), nil
	}

	logger.Cache("Engine", "[JobID: %s] Cache miss. Falling back to autonomous AI inference execution.", state.JobId)

	aiRespStr, err := e.executeAIStep(ctx, state, req)
	if err != nil {
		return "", fmt.Errorf("autonomous fallback to inference failed: %w", err)
	}

	return aiRespStr, nil
}

func cleanJSON(input string) string {
	input = strings.TrimSpace(input)

	if start := strings.Index(input, "```"); start != -1 {
		if end := strings.LastIndex(input, "```"); end > start+2 {
			content := input[start+3 : end]
			content = strings.TrimSpace(content)
			if strings.HasPrefix(strings.ToLower(content), "json") {
				content = strings.TrimSpace(content[4:])
			}
			input = content
		}
	}

	firstBrace := strings.Index(input, "{")
	firstBracket := strings.Index(input, "[")

	first := -1
	if firstBrace != -1 && firstBracket != -1 {
		if firstBrace < firstBracket {
			first = firstBrace
		} else {
			first = firstBracket
		}
	} else if firstBrace != -1 {
		first = firstBrace
	} else if firstBracket != -1 {
		first = firstBracket
	}

	if first == -1 {
		return input
	}

	lastBrace := strings.LastIndex(input, "}")
	lastBracket := strings.LastIndex(input, "]")

	last := -1
	if lastBrace != -1 && lastBracket != -1 {
		if lastBrace > lastBracket {
			last = lastBrace
		} else {
			last = lastBracket
		}
	} else if lastBrace != -1 {
		last = lastBrace
	} else if lastBracket != -1 {
		last = lastBracket
	}

	if last != -1 && last > first {
		return input[first : last+1]
	}

	return input
}
