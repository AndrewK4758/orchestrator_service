package pdf

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/pdfcpu/pdfcpu/pkg/api"
)

// Chunk represents a subset of pages from the original PDF.
type Chunk struct {
	StartIndex int
	EndIndex   int
	// PDFBytes contains the raw bytes of this specific chunk
	PDFBytes []byte
}

// Chunker is responsible for taking a large PDF and safely splitting it into manageable pieces.
// Designed with idempotency in mind: given the same PDF and size, it yields identical chunks.
type Chunker struct {
	// Future configuration fields can go here (e.g., max memory limits)
}

func NewChunker() *Chunker {
	return &Chunker{}
}

// SplitPDF safely extracts chunks of pages from the raw PDF bytes.
// It follows strict error handling and visibility practices.
func (c *Chunker) SplitPDF(ctx context.Context, rawPDF []byte, pagesPerChunk int) ([]Chunk, error) {
	if len(rawPDF) == 0 {
		// Log and return a clear error (Visibility & Client Communication)
		err := fmt.Errorf("invalid input: rawPDF byte slice is empty")
		log.Printf("ERROR: [Chunker.SplitPDF] %v", err)
		return nil, err
	}

	if pagesPerChunk <= 0 {
		err := fmt.Errorf("invalid pagesPerChunk: must be greater than 0, got %d", pagesPerChunk)
		log.Printf("ERROR: [Chunker.SplitPDF] %v", err)
		return nil, err
	}

	// 1. Get the total page count
	reader := bytes.NewReader(rawPDF)
	pageCount, err := api.PageCount(reader, nil)
	if err != nil {
		// Wrap the error with deep context for visibility
		wrappedErr := fmt.Errorf("failed to extract page count from PDF (likely corrupt or invalid format): %w", err)
		log.Printf("ERROR: [Chunker.SplitPDF] %v", wrappedErr)
		return nil, wrappedErr
	}

	log.Printf("INFO: [Chunker.SplitPDF] Successfully read PDF with %d pages", pageCount)

	var chunks []Chunk

	// 2. Safely iterate and extract page batches
	for i := 1; i <= pageCount; i += pagesPerChunk {
		// Check context cancellation to ensure graceful degradation if the client drops
		if err := ctx.Err(); err != nil {
			wrappedErr := fmt.Errorf("context cancelled during PDF chunking at page %d: %w", i, err)
			log.Printf("WARN: [Chunker.SplitPDF] %v", wrappedErr)
			return nil, wrappedErr
		}

		endPage := i + pagesPerChunk - 1
		if endPage > pageCount {
			endPage = pageCount
		}

		// Extract the specific page range
		pageSelection := []string{fmt.Sprintf("%d-%d", i, endPage)}

		// Reset reader for extraction
		reader.Seek(0, 0)
		var chunkBuffer bytes.Buffer

		err := api.Trim(reader, &chunkBuffer, pageSelection, nil)
		if err != nil {
			wrappedErr := fmt.Errorf("failed to extract pages %d to %d from PDF: %w", i, endPage, err)
			log.Printf("ERROR: [Chunker.SplitPDF] %v", wrappedErr)
			return nil, wrappedErr
		}

		chunks = append(chunks, Chunk{
			StartIndex: i,
			EndIndex:   endPage,
			PDFBytes:   chunkBuffer.Bytes(),
		})

		log.Printf("INFO: [Chunker.SplitPDF] Successfully created chunk for pages %d-%d (%d bytes)", i, endPage, chunkBuffer.Len())
	}

	return chunks, nil
}
