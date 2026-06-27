package registry_client

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "doc_processor/shared_protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	addr string
	conn *grpc.ClientConn
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial Artemis registry %s: %v", addr, err)
	}
	return &Client{addr: addr, conn: conn}, nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) GetRules(ctx context.Context, classification string) (string, string, error) {
	client := pb.NewSchemaRegistryServiceClient(c.conn)
	req := &pb.GetRulesRequest{
		ClassificationName: classification,
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := client.GetRulesForClassification(ctx, req)
	if err != nil {
		return "", "", fmt.Errorf("gRPC call failed: %w", err)
	}

	if !resp.Success {
		return "", "", fmt.Errorf("Artemis returned error: %s", resp.ErrorMessage)
	}

	log.Printf("Successfully fetched rules for %s from Artemis", classification)
	return resp.RulesText, resp.JsonSchema, nil
}
