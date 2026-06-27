package engine

import (
	"context"
	"testing"
	"orchestrator_service/internal/pdf"
)

func TestChunkAction_Execute_NoClassify(t *testing.T) {
	chunker := pdf.NewChunker()
	action := NewChunkAction(chunker)
	
	// Because pdfcpu requires a fully valid PDF to parse page count, we can't easily 
	// unit test the full pipeline here without a real PDF, but we can verify text chunks!
	
	stateWithText := &ProcessingState{
		JobID:           "job_456",
		Action:          "PARSE",
		RawTextContents: []string{"Hello world"},
	}
	
	err := action.Execute(context.Background(), stateWithText)
	if err != nil {
		t.Fatalf("Expected no error for text chunking, got %v", err)
	}
	if len(stateWithText.Chunks) != 1 {
		t.Errorf("Expected 1 chunk for text input, got %d", len(stateWithText.Chunks))
	}
	if stateWithText.Chunks[0].TextContent != "Hello world" {
		t.Errorf("Expected chunk text 'Hello world', got '%s'", stateWithText.Chunks[0].TextContent)
	}
}

func TestChunkAction_Execute_ClassifyLimitsToFirstPage(t *testing.T) {
	chunker := pdf.NewChunker()
	action := NewChunkAction(chunker)

	state := &ProcessingState{
		JobID:           "job_456",
		Action:          "CLASSIFY",
		RawTextContents: []string{"Page 1", "Page 2", "Page 3"},
	}

	err := action.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// CLASSIFY should limit chunks to the very first one!
	if len(state.Chunks) != 1 {
		t.Errorf("Expected exactly 1 chunk when action is CLASSIFY, got %d", len(state.Chunks))
	}
	if state.Chunks[0].TextContent != "Page 1" {
		t.Errorf("Expected first chunk to be 'Page 1', got '%s'", state.Chunks[0].TextContent)
	}
}
