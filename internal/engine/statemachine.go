package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/pdf"
)

// ProcessingState represents the global state of a single PDF job as it moves through the pipeline.
type ProcessingState struct {
	JobID  string
	RawPDF []byte

	// Dynamic parameters from the client
	Action             string
	Schema             string
	CustomInstructions string

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
}

func NewEngine() *Engine {
	return &Engine{
		registry: make(map[string]Action),
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
		// Check for cancellation before starting the next step (Resiliency)
		if err := ctx.Err(); err != nil {
			err = fmt.Errorf("job %s cancelled at step %s: %w", state.JobID, step, err)
			state.Errors = append(state.Errors, err)
			return err
		}

		action, exists := e.registry[step]
		if !exists {
			err := fmt.Errorf("critical error: unknown action '%s' in workflow", step)
			state.Errors = append(state.Errors, err)
			return err
		}

		log.Printf("INFO: [Engine.Run] Executing Step: %s", step)
		if err := action.Execute(ctx, state); err != nil {
			// Wrap and record the error for full visibility
			wrappedErr := fmt.Errorf("action '%s' failed: %w", step, err)
			state.Errors = append(state.Errors, wrappedErr)
			return wrappedErr
		}

		state.ExecutedSteps = append(state.ExecutedSteps, step)
	}

	log.Printf("INFO: [Engine.Run] Successfully completed workflow for Job: %s", state.JobID)
	return nil
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

	chunks, err := a.chunker.SplitPDF(ctx, state.RawPDF, pagesPerChunk)
	if err != nil {
		return err // Error is already wrapped cleanly by the chunker
	}

	// If classifying, we ONLY want the very first chunk (first page)
	if state.Action == "CLASSIFY" && len(chunks) > 0 {
		chunks = chunks[:1]
	}

	state.Chunks = chunks
	return nil
}

// ParallelExtractAction fires the chunks to the AI service concurrently.
type ParallelExtractAction struct {
	aiClient *ai_client.Client
}

func NewParallelExtractAction(client *ai_client.Client) *ParallelExtractAction {
	return &ParallelExtractAction{aiClient: client}
}

func (a *ParallelExtractAction) Name() string { return "ParallelExtract" }

func (a *ParallelExtractAction) Execute(ctx context.Context, state *ProcessingState) error {
	if len(state.Chunks) == 0 {
		return fmt.Errorf("cannot extract: no chunks available in state")
	}

	state.ChunkResults = make(map[int]string)

	// We use an errgroup pattern for safe concurrent execution with fast-failure
	var wg sync.WaitGroup
	errChan := make(chan error, len(state.Chunks))

	prompt := state.CustomInstructions
	if prompt == "" {
		if state.Action == "CLASSIFY" {
			prompt = os.Getenv("ORCHESTRATOR_DEFAULT_CLASSIFY_PROMPT")
			if prompt == "" {
				prompt = "Analyze this page and determine the classification type of this document. Return ONLY a single string matching the document type."
			}
		} else {
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

			resp, err := a.aiClient.ExtractChunk(ctx, c.PDFBytes, prompt, state.Schema)
			if err != nil {
				errChan <- fmt.Errorf("chunk %d extraction failed: %w", idx, err)
				return
			}

			state.mu.Lock()
			state.ChunkResults[idx] = resp.ExtractedJson
			state.mu.Unlock()

		}(i, chunk)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Collect any errors
	for err := range errChan {
		// If even one chunk fails, we return the error.
		// Idempotency allows the client to retry safely since we don't mutate external state yet.
		return err
	}

	return nil
}
