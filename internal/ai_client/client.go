package ai_client

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	aipb "doc_processor/shared_protos"
)

// Client wraps the gRPC connection to the AI Service.
type Client struct {
	conn      *grpc.ClientConn
	apiClient aipb.DocumentProcessorClient
}

// NewClient connects to the AI Service at the given address (e.g., "localhost:50051").
func NewClient(aiServiceAddress string) (*Client, error) {
	log.Printf("Connecting to AI Service at %s...", aiServiceAddress)

	// Use insecure credentials for internal microservice communication
	conn, err := grpc.NewClient(aiServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ai_service: %w", err)
	}

	apiClient := aipb.NewDocumentProcessorClient(conn)

	return &Client{
		conn:      conn,
		apiClient: apiClient,
	}, nil
}

// Close gracefully closes the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ExtractChunk synchronously calls the AI Service to process a chunk of PDF pages or raw text.
func (c *Client) ExtractChunk(ctx context.Context, chunkPdf []byte, chunkText string, prompt string, jsonSchema string) (*aipb.ExtractResponse, error) {
	req := &aipb.ExtractRequest{
		ChunkPdf:    chunkPdf,
		ChunkText:   chunkText,
		Prompt:      prompt,
		JsonSchema:  jsonSchema,
		ModelChoice: aipb.ModelChoice_MODEL_CHOICE_LOCAL,
	}

	// This is a blocking, synchronous gRPC call
	resp, err := c.apiClient.ExtractDocumentData(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ai_service ExtractDocumentData call failed: %w", err)
	}

	return resp, nil
}
