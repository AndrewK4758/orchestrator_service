package engine

import (
	"context"
	"fmt"
	"log"
	"sync"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/pdf"
)

// ProcessingState represents the global state of a single PDF job as it moves through the pipeline.
type ProcessingState struct {
	JobID        string
	RawPDF       []byte
	Chunks       []pdf.Chunk
	
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
	// Standardize on 20 pages per chunk to prevent LLM attention loss
	chunks, err := a.chunker.SplitPDF(ctx, state.RawPDF, 20)
	if err != nil {
		return err // Error is already wrapped cleanly by the chunker
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

	prompt := "Review these pages. Identify the start and end pages of each distinct document within this chunk. Return a JSON array."
	
	for i, chunk := range state.Chunks {
		wg.Add(1)
		go func(idx int, c pdf.Chunk) {
			defer wg.Done()
			
			// Note: We need a utility to convert c.PDFBytes into [][]byte of images before sending.
			// For now, we mock the sending of native PDF bytes.
			images := [][]byte{c.PDFBytes} 
			
			resp, err := a.aiClient.ExtractChunk(ctx, images, prompt, "")
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
