package server

import (
	"context"
	"fmt"
	"os"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

func (s *OrchestratorServer) processOrchestratorResponses(ctx context.Context, msg jetstream.Msg) error {
	subj := msg.Subject()
	parts := strings.Split(subj, ".")
	// orchestrator.response.<app>.<tenant>.<job>.<step>
	if len(parts) < 6 {
		logger.Error("System", "Invalid response subject format: %s", subj)
		return nil // Drop it
	}

	appId := parts[2]
	tenantId := parts[3]
	jobId := parts[4]
	stepId := parts[5]

	var resp pb.PerformActionResponse
	if err := proto.Unmarshal(msg.Data(), &resp); err != nil {
		logger.Error("System", "Failed to unmarshal worker response: %v", err)
		return nil
	}

	inputJSON := protojson.MarshalOptions{Multiline: false}.Format(&resp)
	logger.Info("System", "Received worker response (input) for Job: %s, Step: %s, Payload: %s", jobId, stepId, inputJSON)

	addr := s.getCallbackAddress(appId)

	s.webhookConnMu.RLock()
	conn, ok := s.webhookConns[addr]
	s.webhookConnMu.RUnlock()

	if !ok {
		s.webhookConnMu.Lock()
		conn, ok = s.webhookConns[addr]
		if !ok {
			var err error
			conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				s.webhookConnMu.Unlock()
				return fmt.Errorf("failed to dial callback address %s: %w", addr, err)
			}
			s.webhookConns[addr] = conn
		}
		s.webhookConnMu.Unlock()
	}

	client := pb.NewOrchestratorCallbackServiceClient(conn)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			AppId:    appId,
			TenantId: tenantId,
			JobId:    jobId,
		},
		StepId:  stepId,
		Payload: resp.ActionResult,
	}

	outputJSON := protojson.MarshalOptions{Multiline: false}.Format(req)
	logger.Info("System", "Pushing callback to client (output): %s", outputJSON)

	if !resp.Success {
		if resp.IsTransientError {
			_, err := client.OnJobSuspended(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to call OnJobSuspended: %w", err)
			}
		} else {
			_, err := client.OnJobFailed(ctx, req)
			if err != nil {
				return fmt.Errorf("failed to call OnJobFailed: %w", err)
			}
		}
		return nil
	}

	stream, err := client.OnTaskExecutionComplete(ctx)
	if err != nil {
		return fmt.Errorf("failed to open OnTaskExecutionComplete stream: %w", err)
	}

	if err := stream.Send(req); err != nil {
		return fmt.Errorf("failed to send on stream: %w", err)
	}

	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close stream: %w", err)
	}

	// Wait for ack
	if _, err := stream.Recv(); err != nil {
		return fmt.Errorf("failed to receive ack: %w", err)
	}

	logger.Info("System", "Successfully relayed TaskCompleted event for Job: %s, Step: %s to %s", jobId, stepId, addr)
	return nil
}

func (s *OrchestratorServer) getCallbackAddress(appId string) string {
	if appId == "CLI_TEST_APP" {
		return "localhost:11115"
	}
	addr := os.Getenv("ARTEMIS_CALLBACK_ADDRESS")
	if addr == "" {
		return "localhost:5134"
	}
	return addr
}
