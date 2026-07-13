package cache_client

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	CheckCache(ctx context.Context, text string, metadata map[string]string, threshold float32) (*pb.CheckCacheResponse, error)
	StoreExtraction(ctx context.Context, text string, metadata map[string]string, payload string) error
}

type grpcClient struct {
	conn   *grpc.ClientConn
	client pb.SemanticCacheServiceClient
}

func NewClient(addr string) (Client, error) {
	if addr == "" {
		addr = "localhost:50055"
	}

	maxMsgSizeMb := 100
	if envVal := os.Getenv("GRPC_MAX_MESSAGE_SIZE_MB"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxMsgSizeMb = parsed
		}
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSizeMb*1024*1024)))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to semantic cache service at %s: %w", addr, err)
	}

	return &grpcClient{
		conn:   conn,
		client: pb.NewSemanticCacheServiceClient(conn),
	}, nil
}

func (c *grpcClient) CheckCache(ctx context.Context, text string, metadata map[string]string, threshold float32) (*pb.CheckCacheResponse, error) {
	timeoutSecs := 10
	if envVal := os.Getenv("GRPC_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			timeoutSecs = parsed
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	req := &pb.CheckCacheRequest{
		Text:      text,
		Metadata:  metadata,
		Threshold: threshold,
	}

	return c.client.CheckCache(ctx, req)
}

func (c *grpcClient) StoreExtraction(ctx context.Context, text string, metadata map[string]string, payload string) error {
	timeoutSecs := 10
	if envVal := os.Getenv("GRPC_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			timeoutSecs = parsed
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	req := &pb.StoreExtractionRequest{
		Text:             text,
		Metadata:         metadata,
		ExtractedPayload: payload,
	}

	resp, err := c.client.StoreExtraction(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("cache service returned success=false")
	}

	return nil
}

func (c *grpcClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
