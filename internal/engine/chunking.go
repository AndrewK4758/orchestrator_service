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

	zipPath := ""
	if state.ActionState != nil && state.ActionState.Fields != nil {
		if val, ok := state.ActionState.Fields["zip_file_path"]; ok {
			zipPath = val.GetStringValue()
		}
	}

	if zipPath == "" {
		return "", fmt.Errorf("zip_file_path is missing from ActionState")
	}

	resp, err := e.chunkingClient.ChunkDocument(ctx, &pb.ChunkDocumentRequest{
		JobId:           state.JobId,
		StepId:          req.StepId,
		ZipFilePath:     zipPath,
		OriginalRequest: req,
	})

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
