package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"

	"github.com/nats-io/nats.go/jetstream"
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

	logger.Info("System", "Received worker response (input) for Job: %s, Step: %s, Success: %v", respSubj.Identity.JobId, respSubj.StepID, resp.Success)

	dec, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(respSubj.Callback)
	if err != nil {
		return fmt.Errorf("action failed for job processOrchestratorResponses: invalid base64 callback address in subject: %w", err)
	}
	addr := string(dec)
	if strings.TrimSpace(addr) == "" {
		panic(fmt.Errorf("action failed for job processOrchestratorResponses: missing required property CallbackAddress"))
	}

	req := &pb.ExecuteTaskRequest{
		Identity:        respSubj.Identity,
		StepId:          respSubj.StepID,
		Payload:         resp.ActionResult,
		CallbackAddress: addr,
	}

	client, err := s.getCallbackClient(addr)
	if err != nil {
		return err
	}

	if !resp.Success {
		if resp.IsTransientError {
			suspendReq := &pb.JobSuspendedRequest{
				Identity: req.Identity,
				StepId:   req.StepId,
				Error:    resp.ErrorContract,
			}
			logger.Info("System", "Pushing callback to client (output suspend request) for Job: %s", req.Identity.JobId)
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
		logger.Info("System", "Pushing callback to client (output fail request) for Job: %s", req.Identity.JobId)
		_, err := client.OnJobFailed(ctx, failReq)
		if err != nil {
			return fmt.Errorf("failed to call OnJobFailed: %w", err)
		}
		return nil
	}

	logger.Info("System", "Pushing callback to client (output success request) for Job: %s, Step: %s", req.Identity.JobId, req.StepId)

	if err := sendTaskExecutionComplete(ctx, client, req); err != nil {
		return err
	}

	logger.Info("System", "Successfully relayed TaskCompleted event for Job: %s, Step: %s to %s", req.Identity.JobId, req.StepId, addr)
	return nil
}
