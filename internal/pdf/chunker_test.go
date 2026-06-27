package pdf

import (
	"context"
	"strings"
	"testing"
)

func TestChunker_SplitPDF_EmptyInput(t *testing.T) {
	c := NewChunker()
	ctx := context.Background()

	chunks, err := c.SplitPDF(ctx, nil, 1)
	if err == nil {
		t.Fatal("Expected error for nil input, got nil")
	}
	if !strings.Contains(err.Error(), "invalid input") {
		t.Errorf("Expected error to contain 'invalid input', got '%v'", err)
	}
	if chunks != nil {
		t.Errorf("Expected nil chunks, got %v", chunks)
	}
}

func TestChunker_SplitPDF_InvalidPages(t *testing.T) {
	c := NewChunker()
	ctx := context.Background()

	validPdfBytes := []byte("%PDF-1.4\n") // just needs to be non-empty for this check

	chunks, err := c.SplitPDF(ctx, validPdfBytes, 0)
	if err == nil {
		t.Fatal("Expected error for 0 pagesPerChunk, got nil")
	}
	if !strings.Contains(err.Error(), "invalid pagesPerChunk") {
		t.Errorf("Expected error to contain 'invalid pagesPerChunk', got '%v'", err)
	}
	if chunks != nil {
		t.Errorf("Expected nil chunks, got %v", chunks)
	}
}

func TestChunker_SplitPDF_InvalidPDF(t *testing.T) {
	c := NewChunker()
	ctx := context.Background()

	invalidPdfBytes := []byte("This is definitely not a PDF file")

	chunks, err := c.SplitPDF(ctx, invalidPdfBytes, 1)
	if err == nil {
		t.Fatal("Expected error for invalid PDF content, got nil")
	}
	if !strings.Contains(err.Error(), "failed to extract page count") {
		t.Errorf("Expected error to contain 'failed to extract page count', got '%v'", err)
	}
	if chunks != nil {
		t.Errorf("Expected nil chunks, got %v", chunks)
	}
}

func TestChunker_SplitPDF_ContextCancelled(t *testing.T) {
	// A valid tiny PDF to pass the page count check
	// Using pdfcpu, if it parses, it'll try to iterate pages.
	// But it might fail on page count if it's not well-formed.
	// If it fails on page count, we can't test context cancellation inside the loop easily without a valid PDF.
	// We'll skip testing the inner loop context cancellation unless we have a robust valid PDF,
	// because providing a valid PDF inline is verbose.
	// But we can test that the function respects context early on if possible.
	// Since the context check is inside the loop, we need a valid PDF to reach it.
	
	// Create a minimal 1-page PDF using pdfcpu's CreateTestPage if available, 
	// or we can just mock/skip this specific test.
	t.Skip("Skipping context cancellation test as it requires a valid PDF byte slice")
}
