package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type OrchestratorResponseSubject struct {
	Identity *pb.InfrastructureIdentity
	StepID   string
	Callback string
}

func parseOrchestratorSubject(subj string) (*OrchestratorResponseSubject, error) {
	parts := strings.Split(subj, ".")
	if len(parts) < 7 {
		return nil, fmt.Errorf("invalid response subject format: %s", subj)
	}
	return &OrchestratorResponseSubject{
		Identity: &pb.InfrastructureIdentity{
			AppId:    parts[2],
			TenantId: parts[3],
			JobId:    parts[4],
		},
		StepID:   parts[5],
		Callback: parts[6],
	}, nil
}

func (s *OrchestratorServer) processOrchestratorResponses(ctx context.Context, msg jetstream.Msg) error {
	subj := msg.Subject()
	respSubj, err := parseOrchestratorSubject(subj)
	if err != nil {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: %w", err))
	}

	if respSubj.Identity.AppId == "" || respSubj.Identity.TenantId == "" || respSubj.Identity.JobId == "" {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: missing identity parts in subject"))
	}
	if strings.TrimSpace(respSubj.StepID) == "" {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: missing required property stepId"))
	}

	var resp pb.PerformActionResponse
	if err := proto.Unmarshal(msg.Data(), &resp); err != nil {
		return fmt.Errorf("action failed for job processOrchestratorResponses: corrupted protobuf: %w", err)
	}

	inputJSON := protojson.MarshalOptions{Multiline: false}.Format(&resp)
	logger.Info("System", "Received worker response (input) for Job: %s, Step: %s, Payload: %s", respSubj.Identity.JobId, respSubj.StepID, inputJSON)

	req := &pb.ExecuteTaskRequest{
		Identity: respSubj.Identity,
		StepId:  respSubj.StepID,
		Payload: resp.ActionResult,
	}

	dec, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(respSubj.Callback)
	if err != nil {
		return fmt.Errorf("action failed for job processOrchestratorResponses: invalid base64 callback address in subject: %w", err)
	}
	addr := string(dec)

	s.webhookConnMu.Lock()
	entry, ok := s.webhookConns[addr]
	if !ok {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			s.webhookConnMu.Unlock()
			return fmt.Errorf("failed to dial callback address %s: %w", addr, err)
		}
		entry = &clientConnEntry{
			conn:       conn,
			lastAccess: time.Now(),
		}
		s.webhookConns[addr] = entry
	} else {
		entry.lastAccess = time.Now()
	}
	client := pb.NewOrchestratorCallbackServiceClient(entry.conn)
	s.webhookConnMu.Unlock()

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
			return nil
		}
		
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

	logger.Info("System", "Successfully relayed TaskCompleted event for Job: %s, Step: %s to %s", req.Identity.JobId, req.StepId, addr)
	return nil
}
