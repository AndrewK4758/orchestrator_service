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

	// Current step execution tracking
	CurrentStepReq *pb.SubmitStepRequest `json:"current_step_req"`
	CurrentStepID  string                `json:"current_step_id"`

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

func (e *Engine) ExecuteSingleStep(ctx context.Context, state *ProcessingState, req *pb.SubmitStepRequest) (string, error) {
	if req.ActionType == "AI_EXTRACTION" {
		return e.executeAIExtraction(ctx, state, req)
	} else if req.ActionType == "HUMAN_VALIDATION" {
		// Used when the client explicitly routes a failed step back for human review
		taskID, err := e.triggerHumanValidation(ctx, state, req, req.ContextInput, "Client triggered manual validation")
		if err != nil {
			return "", fmt.Errorf("failed to create human validation task: %w", err)
		}
		log.Printf("INFO: [Engine] Successfully registered human validation task: %s for Job: %s", taskID, state.JobID)
		return "", ErrPendingHumanValidation
	}

	return "", fmt.Errorf("unsupported action type: %s", req.ActionType)
}

func (e *Engine) executeAIExtraction(ctx context.Context, state *ProcessingState, req *pb.SubmitStepRequest) (string, error) {
	prompt := req.CustomInstructions
	schema := req.ExpectedSchema

	if prompt == "" || schema == "" {
		return "", fmt.Errorf("prompt and schema must be provided for AI_EXTRACTION")
	}

	if req.ContextInput != "" {
		prompt = fmt.Sprintf("%s\n\n[CONTEXT PROVIDED BY CLIENT]:\n%s\n", prompt, req.ContextInput)
	}

	totalDocs := len(state.RawPDFs) + len(state.RawTextContents)
	if totalDocs == 0 {
		return "", fmt.Errorf("no documents available to process")
	}

	maxRetries := 3
	var valErr error
	var finalJSON string

	for attempt := 1; attempt <= maxRetries; attempt++ {
		stepResults := make(map[int]string)
		var wg sync.WaitGroup
		errChan := make(chan error, totalDocs)
		var resultMu sync.Mutex

		for i, pdfBytes := range state.RawPDFs {
			wg.Add(1)
			go func(index int, data []byte) {
				defer wg.Done()
				e.publishChunkTrace(ctx, state, req, events.EventChunkStarted, fmt.Sprintf("Starting PDF chunk %d (Attempt %d)", index, attempt), index, totalDocs, "")
				resp, err := e.aiClient.ExtractChunk(ctx, data, "", prompt, schema)
				if err != nil {
					e.publishChunkTrace(ctx, state, req, events.EventChunkFailed, fmt.Sprintf("Failed PDF chunk %d", index), index, totalDocs, err.Error())
					errChan <- fmt.Errorf("failed to process PDF %d: %w", index, err)
					return
				}
				e.publishChunkTrace(ctx, state, req, events.EventChunkCompleted, fmt.Sprintf("Completed PDF chunk %d", index), index, totalDocs, "")

				resultMu.Lock()
				stepResults[index] = resp.GetExtractedJson()
				resultMu.Unlock()
			}(i, pdfBytes)
		}

		offset := len(state.RawPDFs)
		for i, textContent := range state.RawTextContents {
			wg.Add(1)
			go func(index int, text string) {
				defer wg.Done()
				e.publishChunkTrace(ctx, state, req, events.EventChunkStarted, fmt.Sprintf("Starting Text chunk %d (Attempt %d)", index, attempt), index, totalDocs, "")
				resp, err := e.aiClient.ExtractChunk(ctx, nil, text, prompt, schema)
				if err != nil {
					e.publishChunkTrace(ctx, state, req, events.EventChunkFailed, fmt.Sprintf("Failed Text chunk %d", index), index, totalDocs, err.Error())
					errChan <- fmt.Errorf("failed to process Text %d: %w", index, err)
					return
				}
				e.publishChunkTrace(ctx, state, req, events.EventChunkCompleted, fmt.Sprintf("Completed Text chunk %d", index), index, totalDocs, "")

				resultMu.Lock()
				stepResults[index] = resp.GetExtractedJson()
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

		if req.RequireHumanReview {
			log.Printf("INFO: [Engine] Explicit human review requested for Step: %s. Triggering Human Validation.", req.StepName)
			taskID, err := e.triggerHumanValidation(ctx, state, req, finalJSON, "Explicit human review requested by workflow policy")
			if err != nil {
				return "", fmt.Errorf("failed to create explicit human validation task: %w", err)
			}
			log.Printf("INFO: [Engine] Successfully registered human validation task: %s", taskID)
			return "", ErrPendingHumanValidation
		}

		valErr = e.validateAIOutput(finalJSON, schema)
		if valErr == nil {
			return finalJSON, nil // Success!
		}

		if strings.Contains(valErr.Error(), "explicitly flagged for review") || req.BypassHumanFallback {
			break
		}

		log.Printf("WARNING: [Engine] AI output invalid for Step: %s. Attempt %d of %d. Error: %v", req.StepName, attempt, maxRetries, valErr)
		if attempt < maxRetries {
			prompt = fmt.Sprintf("%s\n\n[SYSTEM FEEDBACK FROM PREVIOUS ATTEMPT]: %v. Please carefully review the document and ensure this strictly required data is extracted exactly as specified.", prompt, valErr)
		}
	}

	if valErr != nil {
		if req.BypassHumanFallback {
			return "", fmt.Errorf("ai output did not match requirements: %w", valErr)
		}

		log.Printf("WARNING: [Engine] AI output invalid after %d attempts: %v. Triggering Human Validation.", maxRetries, valErr)
		taskID, err := e.triggerHumanValidation(ctx, state, req, finalJSON, valErr.Error())
		if err != nil {
			return "", fmt.Errorf("ai output invalid: %v, and failed to create human validation task: %w", valErr, err)
		}

		log.Printf("INFO: [Engine] Successfully registered human validation task: %s", taskID)
		return "", ErrPendingHumanValidation
	}

	return "", fmt.Errorf("unexpected execution path")
}

func (e *Engine) validateAIOutput(jsonStr string, schema string) error {
	trimmed := strings.TrimSpace(jsonStr)
	if trimmed == "" {
		return fmt.Errorf("AI output is empty")
	}

	var raw interface{}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return fmt.Errorf("AI output is not valid JSON: %w", err)
	}

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
				}
			}
		}
	}

	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, `"flagged for review"`) || strings.Contains(lower, `"flagged_for_review"`) {
		return fmt.Errorf("AI output explicitly flagged for review")
	}

	return nil
}

func (e *Engine) triggerHumanValidation(ctx context.Context, state *ProcessingState, req *pb.SubmitStepRequest, failedOutput string, errMsg string) (string, error) {
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

func (e *Engine) publishChunkTrace(ctx context.Context, state *ProcessingState, req *pb.SubmitStepRequest, eventType events.EventType, message string, chunkIndex int, totalChunks int, errorMsg string) {
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
			JobAction:     req.StepName,
			Stage:         "ChunkProcessing",
			Message:       message,
			Error:         errorMsg,
			ChunkIndex:    chunkIndex + 1,
			TotalChunks:   totalChunks,
			ChunkID:       fmt.Sprintf("chunk_%d", chunkIndex+1),
		})
	}
}
