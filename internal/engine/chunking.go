package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"orchestrator_service/internal/events"
	"orchestrator_service/internal/logger"

	pb "github.com/AndrewK4758/shared_protos"
)

func (e *Engine) executeDocumentChunking(ctx context.Context, state *EngineState, req *pb.ExecuteWorkflowNodeRequest) (string, error) {
	logger.Info("Engine", "[JobID: %s | StepID: %s] Executing Document Chunking", state.JobId, req.StepId)

	if e.chunkingClient == nil {
		return "", fmt.Errorf("document processing service client is not initialized")
	}

	if state.ActionState == nil || state.ActionState.Fields == nil {
		return "", fmt.Errorf("ActionState or Fields is nil")
	}

	stream, err := e.chunkingClient.ChunkDocument(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to open chunking stream: %w", err)
	}

	err = stream.Send(&pb.ChunkDocumentRequest{
		Identity:         state.Req.Identity,
		StepId:           req.StepId,
		DocumentChunking: req.DocumentChunking,
		OriginalRequest:  req,
	})
	if err != nil {
		return "", fmt.Errorf("failed to send chunking request: %w", err)
	}

	if err := stream.CloseSend(); err != nil {
		return "", fmt.Errorf("failed to close chunking stream: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("failed to call chunking service: %w", err)
	}

	if !resp.Success {
		return "", fmt.Errorf("chunking service failed: %s", resp.ErrorMessage)
	}

	for _, trace := range resp.TraceEvents {
		e.publishEvent(ctx, state, events.EventChunkTrace, req.StepId, trace.Message, "")
	}

	// We format the output as JSON so it matches the expected schema payload from AI steps
	// This JSON must match whatever "ExpectedSchema" the client passed for this step
	// For simplicity, we wrap the processed documents into a JSON array string.
	// We'll use a basic JSON marshaler to merge it back to state.
	outBytes, _ := json.Marshal(map[string]any{
		"processed_documents": resp.ProcessedDocuments,
	})

	return string(outBytes), nil
}
