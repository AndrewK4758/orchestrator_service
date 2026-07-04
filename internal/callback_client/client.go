package callback_client

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	pb "doc_processor/shared_protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientManager struct {
	defaultAddr string
	conns       map[string]*grpc.ClientConn
	mu          sync.Mutex
}

func NewClientManager() *ClientManager {
	defaultAddr := os.Getenv("DEFAULT_CALLBACK_ADDRESS")
	return &ClientManager{
		defaultAddr: defaultAddr,
		conns:       make(map[string]*grpc.ClientConn),
	}
}

func (m *ClientManager) getConn(addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		addr = m.defaultAddr
	}
	if addr == "" {
		return nil, fmt.Errorf("no callback address provided and DEFAULT_CALLBACK_ADDRESS is not set")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if conn, exists := m.conns[addr]; exists {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial callback server at %s: %v", addr, err)
	}

	m.conns[addr] = conn
	return conn, nil
}

func (m *ClientManager) GetRules(ctx context.Context, callbackAddr string, classification string) (string, string, error) {
	conn, err := m.getConn(callbackAddr)
	if err != nil {
		return "", "", err
	}

	client := pb.NewSchemaRegistryServiceClient(conn)
	req := &pb.GetRulesRequest{
		ClassificationName: classification,
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := client.GetRulesForClassification(ctx, req)
	if err != nil {
		return "", "", fmt.Errorf("gRPC callback failed: %w", err)
	}

	if !resp.Success {
		return "", "", fmt.Errorf("callback server returned error: %s", resp.ErrorMessage)
	}

	log.Printf("Successfully fetched rules for %s from callback server %s", classification, callbackAddr)
	return resp.RulesText, resp.JsonSchema, nil
}

func (m *ClientManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for addr, conn := range m.conns {
		conn.Close()
		delete(m.conns, addr)
	}
}
