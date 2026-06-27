package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"
	"orchestrator_service/internal/pdf"
)

// ProcessingState represents the global state of a single PDF job as it moves through the pipeline.
type ProcessingState struct {
	JobID           string
	RawPDFs         [][]byte
	RawTextContents []string

	// Compliance & Traceability Metadata
	TenantID      string
	UserID        string
	AppID         string
	DocumentType  string
	CorrelationID string

	// Dynamic parameters from the client
	Action                 string
	Schema                 string
	CustomInstructions     string
	AllowedClassifications []string

	Chunks []pdf.Chunk

	// Thread-safe map to store results from concurrent AI extractions
	mu           sync.Mutex
	ChunkResults map[int]string

	// Audit trail for visibility
	ExecutedSteps []string
	Errors        []error
}

// Action represents a single discrete node in the DAG/State Machine.
type Action interface {
	Name() string
	Execute(ctx context.Context, state *ProcessingState) error
}

// Engine manages the registration and execution of Actions.
type Engine struct {
	registry map[string]Action
	pub      *events.Publisher
}

func NewEngine(pub *events.Publisher) *Engine {
	return &Engine{
		registry: make(map[string]Action),
		pub:      pub,
	}
}

// Register adds a new action to the engine's capability list.
func (e *Engine) Register(action Action) {
	e.registry[action.Name()] = action
}

// Run executes a defined sequential workflow.
// Idempotency: Given the same state, it produces the same results.
func (e *Engine) Run(ctx context.Context, workflow []string, state *ProcessingState) error {
	log.Printf("INFO: [Engine.Run] Starting workflow for Job: %s", state.JobID)

	for _, step := range workflow {
		if err := ctx.Err(); err != nil {
			err = fmt.Errorf("job %s cancelled at step %s: %w", state.JobID, step, err)
			state.Errors = append(state.Errors, err)
			e.publishJobFailed(ctx, state, err.Error())
			return err
		}

		action, exists := e.registry[step]
		if !exists {
			err := fmt.Errorf("critical error: unknown action '%s' in workflow", step)
			state.Errors = append(state.Errors, err)
			e.publishJobFailed(ctx, state, err.Error())
			return err
		}

		log.Printf("INFO: [Engine.Run] Executing Step: %s", step)
		if err := action.Execute(ctx, state); err != nil {
			wrappedErr := fmt.Errorf("action '%s' failed: %w", step, err)
			state.Errors = append(state.Errors, wrappedErr)
			e.publishJobFailed(ctx, state, wrappedErr.Error())
			return wrappedErr
		}

		state.ExecutedSteps = append(state.ExecutedSteps, step)
	}

	log.Printf("INFO: [Engine.Run] Successfully completed workflow for Job: %s", state.JobID)

	err := e.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType:     events.EventJobCompleted,
		Timestamp:     time.Now(),
		TenantID:      state.TenantID,
		UserID:        state.UserID,
		AppID:         state.AppID,
		JobID:         state.JobID,
		DocumentType:  state.DocumentType,
		CorrelationID: state.CorrelationID,
	})
	if err != nil {
		log.Printf("ERROR: Failed to publish trace event: %v", err)
	}
	return nil
}

func (e *Engine) publishJobFailed(ctx context.Context, state *ProcessingState, errorMsg string) {
	err := e.pub.PublishTrace(ctx, events.TraceabilityEvent{
		EventID:       fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType:     events.EventJobFailed,
		Timestamp:     time.Now(),
		TenantID:      state.TenantID,
		UserID:        state.UserID,
		AppID:         state.AppID,
		JobID:         state.JobID,
		DocumentType:  state.DocumentType,
		CorrelationID: state.CorrelationID,
		Error:         errorMsg,
	})
	if err != nil {
		log.Printf("ERROR: Failed to publish failure trace event: %v", err)
	}
}

// --- CORE ACTIONS ---

// ChunkAction handles splitting the PDF into physical byte chunks.
type ChunkAction struct {
	chunker *pdf.Chunker
}

func NewChunkAction(chunker *pdf.Chunker) *ChunkAction {
	return &ChunkAction{chunker: chunker}
}

func (a *ChunkAction) Name() string { return "ChunkPDF" }

func (a *ChunkAction) Execute(ctx context.Context, state *ProcessingState) error {
	// If we are classifying, we just need the first page. Otherwise, standard chunking.
	pagesPerChunk := 20
	if envPages := os.Getenv("ORCHESTRATOR_PAGES_PER_CHUNK"); envPages != "" {
		if p, err := strconv.Atoi(envPages); err == nil && p > 0 {
			pagesPerChunk = p
		}
	}
	if state.Action == "CLASSIFY" {
		pagesPerChunk = 1
	}

	var allChunks []pdf.Chunk
	for _, raw := range state.RawPDFs {
		chunks, err := a.chunker.SplitPDF(ctx, raw, pagesPerChunk)
		if err != nil {
			return err // Error is already wrapped cleanly by the chunker
		}
		allChunks = append(allChunks, chunks...)
	}

	for _, text := range state.RawTextContents {
		allChunks = append(allChunks, pdf.Chunk{
			StartIndex:  1,
			EndIndex:    1,
			IsText:      true,
			TextContent: text,
		})
	}

	// If classifying, we ONLY want the very first chunk (first page)
	if state.Action == "CLASSIFY" && len(allChunks) > 0 {
		allChunks = allChunks[:1]
	}

	state.Chunks = allChunks
	return nil
}

// ParallelExtractAction fires the chunks to the AI service concurrently.
type ParallelExtractAction struct {
	aiClient *ai_client.Client
	pub      *events.Publisher
}

func NewParallelExtractAction(client *ai_client.Client, pub *events.Publisher) *ParallelExtractAction {
	return &ParallelExtractAction{aiClient: client, pub: pub}
}

func (a *ParallelExtractAction) Name() string { return "ParallelExtract" }

func (a *ParallelExtractAction) Execute(ctx context.Context, state *ProcessingState) error {
	if len(state.Chunks) == 0 {
		return fmt.Errorf("cannot extract: no chunks available in state")
	}

	state.ChunkResults = make(map[int]string)

	var wg sync.WaitGroup
	errChan := make(chan error, len(state.Chunks))

	var prompt string
	if state.Action == "CLASSIFY" {
		prompt = state.CustomInstructions
		if prompt == "" {
			prompt = os.Getenv("ORCHESTRATOR_DEFAULT_CLASSIFY_PROMPT")
			if prompt == "" {
				prompt = "Analyze this page and determine the classification type of this document."
			}
		}

		// Dynamic Restricted Classification
		if len(state.AllowedClassifications) > 0 {
			allowedList := strings.Join(state.AllowedClassifications, ", ")
			prompt += fmt.Sprintf(" You must strictly choose one of the following classification types: [%s]. If the document cannot be parsed into any of these types, return exactly 'FLAGGED FOR REVIEW'.", allowedList)
		}

		// Force a strict JSON schema for classification so the client can reliably parse it
		if state.Schema == "" {
			// Build the enum list
			enumValues := ""
			if len(state.AllowedClassifications) > 0 {
				for _, allowed := range state.AllowedClassifications {
					enumValues += fmt.Sprintf(`"%s", `, allowed)
				}
			}
			enumValues += `"FLAGGED FOR REVIEW"`

			state.Schema = fmt.Sprintf(`{
				"type": "object",
				"properties": {
					"classification_type": { 
						"type": "string",
						"enum": [%s]
					}
				},
				"required": ["classification_type"]
			}`, enumValues)
		}
	} else {
		prompt = state.CustomInstructions
		if prompt == "" {
			prompt = os.Getenv("ORCHESTRATOR_DEFAULT_PARSE_PROMPT")
			if prompt == "" {
				prompt = "Extract data from this document according to the requested schema."
			}
		}
	}

	for i, chunk := range state.Chunks {
		wg.Add(1)
		go func(idx int, c pdf.Chunk) {
			defer wg.Done()

			err := a.pub.PublishTrace(ctx, events.TraceabilityEvent{
				EventID:       fmt.Sprintf("evt_chunk_%s_%d", state.JobID, idx),
				EventType:     events.EventChunkStarted,
				Timestamp:     time.Now(),
				TenantID:      state.TenantID,
				UserID:        state.UserID,
				AppID:         state.AppID,
				JobID:         state.JobID,
				DocumentType:  state.DocumentType,
				CorrelationID: state.CorrelationID,
				ChunkIndex:    idx,
				TotalChunks:   len(state.Chunks),
			})
			if err != nil {
				log.Printf("ERROR: Failed to publish trace event: %v", err)
			}

			resp, err := a.aiClient.ExtractChunk(ctx, c.PDFBytes, c.TextContent, prompt, state.Schema)
			if err != nil {
				err := a.pub.PublishTrace(ctx, events.TraceabilityEvent{
					EventID:       fmt.Sprintf("evt_chunk_fail_%s_%d", state.JobID, idx),
					EventType:     events.EventChunkFailed,
					Timestamp:     time.Now(),
					TenantID:      state.TenantID,
					UserID:        state.UserID,
					AppID:         state.AppID,
					JobID:         state.JobID,
					DocumentType:  state.DocumentType,
					CorrelationID: state.CorrelationID,
					ChunkIndex:    idx,
					TotalChunks:   len(state.Chunks),
					Error:         err.Error(),
				})
				if err != nil {
					log.Printf("ERROR: Failed to publish failure trace event: %v", err)
				}
				errChan <- fmt.Errorf("chunk %d extraction failed: %w", idx, err)
				return
			}

			err = a.pub.PublishTrace(ctx, events.TraceabilityEvent{
				EventID:       fmt.Sprintf("evt_chunk_comp_%s_%d", state.JobID, idx),
				EventType:     events.EventChunkCompleted,
				Timestamp:     time.Now(),
				TenantID:      state.TenantID,
				UserID:        state.UserID,
				AppID:         state.AppID,
				JobID:         state.JobID,
				DocumentType:  state.DocumentType,
				CorrelationID: state.CorrelationID,
				ChunkIndex:    idx,
				TotalChunks:   len(state.Chunks),
			})
			if err != nil {
				log.Printf("ERROR: Failed to publish trace event: %v", err)
			}

			state.mu.Lock()
			state.ChunkResults[idx] = resp.ExtractedJson
			state.mu.Unlock()

		}(i, chunk)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
	}

	return nil
}
