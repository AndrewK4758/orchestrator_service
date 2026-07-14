package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"strings"
	"time"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/encoding/protojson"
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
          "action_type": { "type": "string", "enum": ["PRIMITIVE_AI_STEP", "PRIMITIVE_HUMAN_STEP", "PRIMITIVE_SEMANTIC_CACHE_STORE", "PRIMITIVE_ZERO_SHOT_CLASSIFICATION"] },
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
	PublishCacheStore(ctx context.Context, data []byte) error
}

type AIClient interface {
	PerformAction(ctx context.Context, workflowCtx *pb.WorkflowContext, currentAction *pb.ActionDefinition, inputBytes []byte, inputText string, modelChoice pb.ModelChoice) (*pb.PerformActionResponse, error)
}

type ActionFunc func(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition) (string, error)

type Engine struct {
	aiClient          AIClient
	cacheClient       pb.SemanticCacheServiceClient
	pub               TracePublisher
	primitiveHandlers map[string]ActionFunc
	pendingCacheJobs  sync.Map
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
	log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Generating plan for step: %s\n", state.JobId, step.Id, step.StepName)

	if step.RequireHumanReview {
		log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Step requires explicit human review. Bypassing AI Planner.", state.JobId, step.Id)
		return &ActionPlan{
			Actions: []ActionItem{
				{
					ActionType: "PRIMITIVE_HUMAN_STEP",
					Prompt:     step.CustomInstructions,
					Schema:     []byte(step.ExpectedSchema),
				},
			},
		}, nil
	}

	planningPrompt := fmt.Sprintf(`You are the Orchestrator AI Planner. Your job is to sequence the workflow for this step.
Step Instructions: %s
Expected Schema: %s

Available Primitives:
- PRIMITIVE_ZERO_SHOT_CLASSIFICATION: Use this FIRST if the step involves checking a document against known templates in a cache. If this cache hits, the step will terminate early and succeed.
- PRIMITIVE_AI_STEP: Use this for AI inference. If used after PRIMITIVE_ZERO_SHOT_CLASSIFICATION, it will act as a fallback in case the cache misses.
- PRIMITIVE_SEMANTIC_CACHE_STORE: Use this to store successful extractions in the cache.
- PRIMITIVE_HUMAN_STEP: Use this if human validation is explicitly requested.

Return a JSON array of actions matching the PlannerSchema.`, step.CustomInstructions, step.ExpectedSchema)

	actionDef := &pb.ActionDefinition{
		Name:                  "Generate Plan",
		OrchestratorPrimitive: "PLANNING_PRIMITIVE",
		Prompt:                planningPrompt,
		ExpectedOutputSchema:  PlannerSchema,
	}

	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, "", pb.ModelChoice_MODEL_CHOICE_PLANNING)
	if err != nil {
		return nil, fmt.Errorf("action failed for job %s: failed to generate plan: %w", state.JobId, err)
	}

	rawJson := cleanJSON(resp.GetActionResultJson())
	
	// We still merge the plan into ActionState as if it was returned by the AI, for consistency
	mergeIntoActionState(state, rawJson)

	var plan ActionPlan
	if err := json.Unmarshal([]byte(rawJson), &plan); err != nil {
		return nil, fmt.Errorf("action failed for job %s: failed to parse plan: %w", state.JobId, err)
	}

	return &plan, nil
}

// cleanJSON removes common markdown wrappers if the AI hallucinated them.
func cleanJSON(input string) string {
	cleaned := strings.TrimSpace(input)
	if strings.HasPrefix(cleaned, "```json") {
		cleaned = strings.TrimPrefix(cleaned, "```json")
	} else if strings.HasPrefix(cleaned, "```") {
		cleaned = strings.TrimPrefix(cleaned, "```")
	}
	if strings.HasSuffix(cleaned, "```") {
		cleaned = strings.TrimSuffix(cleaned, "```")
	}
	return strings.TrimSpace(cleaned)
}

func (e *Engine) reviewPlan(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition, plan *ActionPlan) (bool, string, error) {
	log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Reviewing plan dynamically.", state.JobId, step.Id)

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

Review the actions. If they form a valid plan to achieve the instructions, approve it. If not, reject it with a reason.
Return a JSON object matching the expected schema.`, step.CustomInstructions, step.ExpectedSchema, actionsStr)

	actionDef := &pb.ActionDefinition{
		Name:                  "Review Plan",
		OrchestratorPrimitive: "REVIEW_PRIMITIVE",
		Prompt:                reviewPrompt,
		ExpectedOutputSchema:  ReviewSchema,
	}

	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, "", pb.ModelChoice_MODEL_CHOICE_CLASSIFICATION)
	if err != nil {
		return false, "", fmt.Errorf("action failed for job %s: failed to review plan: %w", state.JobId, err)
	}

	rawJson := cleanJSON(resp.GetActionResultJson())
	mergeIntoActionState(state, rawJson)

	var result struct {
		Approved bool   `json:"approved"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(rawJson), &result); err != nil {
		return false, "", fmt.Errorf("action failed for job %s: failed to parse review result: %w", state.JobId, err)
	}

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
	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, step.ContextInput, pb.ModelChoice_MODEL_CHOICE_EXTRACTION)
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
	resp, err := e.aiClient.PerformAction(ctx, state, actionDef, nil, "", pb.ModelChoice_MODEL_CHOICE_CLASSIFICATION)
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

func (e *Engine) ExecuteWorkflow(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition) (string, string, string, string, error) {
	var lastStepName string
	lastStepId := ""
	if state.ActiveExecution != nil {
		lastStepId = state.ActiveExecution.CurrentStepId
	}

	lastStepName = step.StepName
	lastStepId = step.Id

	if step.CustomInstructions == "" || step.ExpectedSchema == "" {
		return "", "", lastStepName, lastStepId, fmt.Errorf("step %s is missing required CustomInstructions or ExpectedSchema", step.StepName)
	}

	maxRetries := 3
	stepSuccess := false

	var finalAssembledJson string

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
				log.Printf("WARN: [Engine] [JobID: %s | StepID: %s] Generate plan failed: %v", state.JobId, step.Id, err)
				if attempt == maxRetries {
					return "", "", lastStepName, lastStepId, err
				}
				e.publishEvent(ctx, state, events.EventPlanRejectedRetry, step.StepName, fmt.Sprintf("Plan generation failed, retrying (%d/%d): %v", attempt+1, maxRetries, err), "")
				continue
			}

			// 2. Review Phase
			approved, reviewReason, err := e.reviewPlan(ctx, state, step, plan)
			if err != nil {
				log.Printf("WARN: [Engine] [JobID: %s | StepID: %s] Review plan failed: %v", state.JobId, step.Id, err)
				if attempt == maxRetries {
					return "", "", lastStepName, lastStepId, err
				}
				e.publishEvent(ctx, state, events.EventPlanRejectedRetry, step.StepName, fmt.Sprintf("Plan review failed, retrying (%d/%d): %v", attempt+1, maxRetries, err), "")
				continue
			}

			if !approved {
				log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Plan rejected during Review Phase: %s", state.JobId, step.Id, reviewReason)
				if attempt == maxRetries {
					e.publishEvent(ctx, state, events.EventJobStepFailed, step.StepName, "Review rejected plan after retries", reviewReason)
					taskID, _ := e.triggerHumanValidation(ctx, state, step, "Plan Rejected: "+reviewReason, "Please correct the instructions or schema.")
					return "", "", lastStepName, lastStepId, fmt.Errorf("%w: %s", ErrPendingHumanValidation, taskID)
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
				return "", "", lastStepName, lastStepId, fmt.Errorf("unknown action type from planner: %s", action.OrchestratorPrimitive)
			}

			e.publishEvent(ctx, state, events.EventActionStarted, action.Id, fmt.Sprintf("Started Action: %s", action.Name), "")

			actionStep := &pb.StepDefinition{
				Id:                 step.Id,
				StepName:           fmt.Sprintf("%s - %s", step.StepName, action.OrchestratorPrimitive),
				CustomInstructions: action.Prompt,
				ExpectedSchema:     action.ExpectedOutputSchema,
				ContextInput:       step.ContextInput,
				UseCache:           step.UseCache,
				CacheKeyInput:      step.CacheKeyInput,
				CacheConfig:        step.CacheConfig,
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

		// 5. Assembly Phase
		assembledJson, err := e.assembleStepOutput(ctx, state, step)
		if err != nil {
			return "", "", lastStepName, lastStepId, err
		}

		// 6. Validation Phase
		valid, valReason, err := e.validateStepOutput(ctx, state, step, assembledJson)
		if err != nil {
			return "", "", lastStepName, lastStepId, err
		}

		if !valid {
			log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Step Validation Failed: %s", state.JobId, step.Id, valReason)
			if attempt == maxRetries {
				e.publishEvent(ctx, state, events.EventStepValidationFailed, step.StepName, "Step Validation Failed after retries", valReason)
				taskID, _ := e.triggerHumanValidation(ctx, state, step, "Validation Failed: "+valReason, "The final output did not match the expected schema.")
				return "", "", lastStepName, lastStepId, fmt.Errorf("%w: %s", ErrPendingHumanValidation, taskID)
			}

			e.publishEvent(ctx, state, events.EventStepValidationFailedRetry, step.StepName, fmt.Sprintf("Validation failed, retrying (%d/%d): %s", attempt+1, maxRetries, valReason), "")

			// Force full Re-plan!
			state.PendingActions = nil
			state.CurrentActionIndex = 0
			continue
		}

		// 7. Success! Commit strictly to the Global State via a direct top-level merge.
		// If WritesToStateKey is provided, nest the output safely.
		mergeIntoGlobalState(state, assembledJson, step.WritesToStateKey)

		stepSuccess = true
		finalAssembledJson = assembledJson
		break
	}

	if !stepSuccess {
		// Redundant catch-all in case the loop exits without success and without a return
		return "", "", lastStepName, lastStepId, fmt.Errorf("step %s failed after %d retries", step.StepName, maxRetries)
	}

	// Cleanup inner scope tracking
	state.PendingActions = nil
	state.CurrentActionIndex = 0
	state.ActionState = nil

	// Final state json
	var finalJSON []byte
	if state.GlobalState != nil {
		finalJSON, _ = state.GlobalState.MarshalJSON()
	} else {
		finalJSON = []byte("{}")
	}

	return string(finalJSON), finalAssembledJson, lastStepName, lastStepId, nil
}

func (e *Engine) executeAIStep(ctx context.Context, state *pb.WorkflowContext, req *pb.StepDefinition) (string, error) {
	var currentAction *pb.ActionDefinition
	var actionName string
	if state.PendingActions != nil && len(state.PendingActions) > int(state.CurrentActionIndex) {
		currentAction = state.PendingActions[state.CurrentActionIndex]
		actionName = currentAction.Name
	}

	log.Printf("INFO: [Engine] [JobID: %s | StepID: %s | Action: %s] Executing AI step: %s", state.JobId, req.Id, actionName, req.StepName)
	prompt := req.CustomInstructions

	if prompt == "" || req.ExpectedSchema == "" {
		return "", fmt.Errorf("prompt and schema must be provided for AI step")
	}

	if state.ActionState != nil {
		actionStateMap := state.ActionState.AsMap()
		if hit, ok := actionStateMap["cache_hit"].(bool); ok && hit {
			if payload, ok := actionStateMap["extracted_payload"]; ok {
				log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Cache hit detected. Skipping AI inference.", state.JobId, req.Id)
				bytes, _ := json.Marshal(payload)
				return string(bytes), nil
			}
		}
	}

	log.Printf("DEBUG: [Engine] [JobID: %s | StepID: %s | Action: %s] AI Step Prompt: %s\n", state.JobId, req.Id, actionName, prompt)

	// The AI service natively extracts the schema and instructions from the ActionDefinition,
	// and automatically appends the GlobalState and ActionState from the workflowCtx.
	// We only need to pass the raw context input as the document text.
	resp, err := e.aiClient.PerformAction(ctx, state, currentAction, nil, req.ContextInput, pb.ModelChoice_MODEL_CHOICE_EXTRACTION)
	if err != nil {
		log.Printf("ERROR: [Engine] [JobID: %s | StepID: %s | Action: %s] AI service failed for step %s: %v", state.JobId, req.Id, actionName, req.StepName, err)
		return "", fmt.Errorf("failed to process step %s: %w", req.StepName, err)
	}

	log.Printf("INFO: [Engine] [JobID: %s | StepID: %s | Action: %s] AI service completed for step %s", state.JobId, req.Id, actionName, req.StepName)

	rawJson := cleanJSON(resp.GetActionResultJson())
	return rawJson, nil
}


func (e *Engine) executeSemanticCacheStore(ctx context.Context, state *pb.WorkflowContext, req *pb.StepDefinition) (string, error) {
	log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Executing Cache Store Step: %s", state.JobId, req.Id, req.StepName)

	if e.pub == nil {
		return "", fmt.Errorf("publisher is not configured")
	}

	if state.ActionState != nil {
		actionStateMap := state.ActionState.AsMap()
		if hit, ok := actionStateMap["cache_hit"].(bool); ok && hit {
			log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Cache hit was detected previously. Skipping Store.", state.JobId, req.Id)
			return "{}", nil
		}
	}

	// We need to fetch the JSON payload to store. We assume it's currently in the GlobalState,
	// or passed down via some assembled step. For now, we take the entire GlobalState as the cached representation.
	payload := "{}"
	if state.GlobalState != nil {
		bytes, _ := state.GlobalState.MarshalJSON()
		payload = string(bytes)
	}

	useCache := req.UseCache
	cacheInput := req.ContextInput
	if req.CacheKeyInput != "" {
		cacheInput = req.CacheKeyInput
	}
	collectionName := "default_collection"

	if req.CacheConfig != nil {
		useCache = req.CacheConfig.UseCache
		cacheInput = req.CacheConfig.CacheInput
		if req.CacheConfig.CollectionName != "" {
			collectionName = req.CacheConfig.CollectionName
		}
	}

	if useCache {
		metadataStruct, _ := structpb.NewStruct(make(map[string]interface{}))
		// We'd map state.Metadata to structpb.Struct here if needed, but for now just pass empty Struct
		// or whatever we have. To be perfectly compliant with the new proto:
		for k, v := range state.Metadata {
			metadataStruct.Fields[k] = structpb.NewStringValue(v)
		}

		reqStore := &pb.StoreExtractionRequest{
			CollectionName:   collectionName,
			Text:             cacheInput,
			Metadata:         metadataStruct,
			ExtractedPayload: payload,
		}

		correlationID := fmt.Sprintf("%s_%s", state.JobId, req.Id)
		
		msg := &pb.CacheStoreMessage{
			CorrelationId: correlationID,
			Request:       reqStore,
		}

		bytes, err := protojson.Marshal(msg)
		if err != nil {
			log.Printf("ERROR: [Engine] [JobID: %s] Failed to marshal cache store msg: %v", state.JobId, err)
		} else {
			err = e.pub.PublishCacheStore(ctx, bytes)
			if err != nil {
				log.Printf("ERROR: [Engine] [JobID: %s | StepID: %s] Cache store publish failed: %v", state.JobId, req.Id, err)
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

func (e *Engine) triggerHumanValidation(ctx context.Context, state *pb.WorkflowContext, step *pb.StepDefinition, failedOutput string, errMsg string) (string, error) {
	addr := os.Getenv("HUMAN_VALIDATION_SERVICE_ADDRESS")
	if addr == "" {
		addr = "localhost:50054"
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
	if state.JobDefinition != nil {
		jobName = fmt.Sprintf("%d", state.JobDefinition.Name)
	}

	globalStateJSON := "{}"
	if state.GlobalState != nil {
		bytes, _ := state.GlobalState.MarshalJSON()
		globalStateJSON = string(bytes)
	}

	createReq := &pb.CreateTaskRequest{
		JobId:              state.JobId,
		StepName:           step.StepName,
		FailedAiOutput:     failedOutput,
		ExpectedSchema:     step.ExpectedSchema,
		PromptInstructions: step.CustomInstructions,
		ErrorMessage:       errMsg,
		GlobalStateJson:    globalStateJSON,
		JobName:            jobName,
		StepId:             step.Id,
		StepContextInput:   step.ContextInput,
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

func mergeIntoGlobalState(state *pb.WorkflowContext, jsonStr string, targetKey string) {
	if jsonStr == "" {
		return
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return
	}

	if state.GlobalState == nil {
		state.GlobalState, _ = structpb.NewStruct(make(map[string]interface{}))
	}

	m := state.GlobalState.AsMap()
	if targetKey != "" {
		m[targetKey] = parsed
	} else {
		for k, v := range parsed {
			m[k] = v
		}
	}

	newState, _ := structpb.NewStruct(m)
	state.GlobalState = newState
}

func (e *Engine) executeZeroShotClassification(ctx context.Context, state *pb.WorkflowContext, req *pb.StepDefinition) (string, error) {
	log.Printf("INFO: [Engine] [JobID: %s | StepID: %s] Executing Zero Shot Classification Step: %s", state.JobId, req.Id, req.StepName)

	if e.cacheClient == nil {
		return "", fmt.Errorf("cache client is not configured")
	}

	var resp *pb.CheckCacheResponse
	useCache := req.UseCache
	cacheInput := req.CacheKeyInput
	threshold := float32(0.99)
	collectionName := "default_collection"

	if req.CacheConfig != nil {
		useCache = req.CacheConfig.UseCache
		cacheInput = req.CacheConfig.CacheInput
		if req.CacheConfig.Threshold > 0 {
			threshold = req.CacheConfig.Threshold
		}
		if req.CacheConfig.CollectionName != "" {
			collectionName = req.CacheConfig.CollectionName
		}
	} else if envVal := os.Getenv("SEMANTIC_CACHE_THRESHOLD"); envVal != "" {
		if parsed, err := strconv.ParseFloat(envVal, 32); err == nil {
			threshold = float32(parsed)
		}
	}

	if useCache {

		hashBytes := sha256.Sum256([]byte(cacheInput))
		hashKey := hex.EncodeToString(hashBytes[:])

		if ch, loaded := e.pendingCacheJobs.Load(hashKey); loaded {
			log.Printf("INFO: [Engine] [JobID: %s] CACHE STAMPEDE DELAY! Waiting for identical job...", state.JobId)
			select {
			case <-ch.(chan struct{}):
			case <-time.After(30 * time.Second):
			case <-ctx.Done():
				return "", fmt.Errorf("action failed for job %s: context cancelled while waiting for stampede lock: %w", state.JobId, ctx.Err())
			}
		}

		var err error
		metadataStruct, _ := structpb.NewStruct(make(map[string]interface{}))
		for k, v := range state.Metadata {
			metadataStruct.Fields[k] = structpb.NewStringValue(v)
		}
		
		reqCheck := &pb.CheckCacheRequest{
			CollectionName: collectionName,
			Text:           cacheInput,
			Metadata:       metadataStruct,
			Threshold:      threshold,
		}

		resp, err = e.cacheClient.CheckCache(ctx, reqCheck)
		if err != nil {
			log.Printf("ERROR: [Engine] [JobID: %s | StepID: %s] Cache check failed: %v", state.JobId, req.Id, err)
			resp = &pb.CheckCacheResponse{Hit: false}
		}

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
		log.Printf("INFO: [Engine] [JobID: %s] Zero-Shot Cache hit! Returning payload immediately.", state.JobId)
		
		mergeIntoGlobalState(state, resp.ExtractedPayload, req.WritesToStateKey)

		// Fast-fail the remaining actions in the plan to simulate immediate step completion!
		state.PendingActions = nil
		state.CurrentActionIndex = 0

		return `{"cache_hit": true, "early_termination": true}`, nil
	}

	return `{"cache_hit": false}`, nil
}
