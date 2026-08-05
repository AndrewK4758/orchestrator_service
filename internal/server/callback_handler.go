package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func (s *OrchestratorServer) processOrchestratorResponses(ctx context.Context, msg jetstream.Msg) error {
	subj := msg.Subject()
	parts := strings.Split(subj, ".")
	// orchestrator.response.<app>.<tenant>.<job>.<step>.<base64-callback>
	if len(parts) < 7 {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: invalid response subject format: %s", subj))
	}

	appId := parts[2]
	tenantId := parts[3]
	jobId := parts[4]
	stepId := parts[5]

	if appId == "" || tenantId == "" || jobId == "" {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: missing identity parts in subject"))
	}
	if strings.TrimSpace(stepId) == "" {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: missing required property stepId"))
	}

	var resp pb.PerformActionResponse
	if err := proto.Unmarshal(msg.Data(), &resp); err != nil {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: corrupted protobuf: %v", err))
	}

	inputJSON := protojson.MarshalOptions{Multiline: false}.Format(&resp)
	logger.Info("System", "Received worker response (input) for Job: %s, Step: %s, Payload: %s", jobId, stepId, inputJSON)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			AppId:    appId,
			TenantId: tenantId,
			JobId:    jobId,
		},
		StepId:  stepId,
		Payload: resp.ActionResult,
	}

	encodedURL := parts[6]
	dec, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(encodedURL)
	if err != nil {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: invalid base64 callback address in subject: %v", err))
	}
	addr := string(dec)

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

	if !resp.Success {
		if resp.IsTransientError {
			suspendReq := &pb.JobSuspendedRequest{
				Identity: req.Identity,
				StepId:   req.StepId,
				Error:    resp.ErrorContract,
			}
			outputJSON := protojson.MarshalOptions{Multiline: false}.Format(suspendReq)
			logger.Info("System", "Pushing callback to client (output): %s", outputJSON)
			_, err := client.OnJobSuspended(ctx, suspendReq)
			if err != nil {
				return fmt.Errorf("failed to call OnJobSuspended: %w", err)
			}
		} else {
			failReq := &pb.JobFailedRequest{
				Identity: req.Identity,
				StepId:   req.StepId,
				Error:    resp.ErrorContract,
			}
			outputJSON := protojson.MarshalOptions{Multiline: false}.Format(failReq)
			logger.Info("System", "Pushing callback to client (output): %s", outputJSON)
			_, err := client.OnJobFailed(ctx, failReq)
			if err != nil {
				return fmt.Errorf("failed to call OnJobFailed: %w", err)
			}
		}
		return nil
	}

	outputJSON := protojson.MarshalOptions{Multiline: false}.Format(req)
	logger.Info("System", "Pushing callback to client (output): %s", outputJSON)

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
