package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/envutil"
	_ "github.com/AndrewK4758/shared_utils/identityparser"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/workflowparser"

	"orchestrator_service/internal/engine"
	"orchestrator_service/internal/events"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer
	eng *engine.Engine
	pub JobPublisher

	webhookConnMu sync.RWMutex
	webhookConns  map[string]*grpc.ClientConn
	grpcMaxMsgMb  int
	grpcTimeout   int
}

type JobPublisher interface {
	PublishWebhook(ctx context.Context, payload *events.WebhookEvent) error
	ConsumeWebhooks(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error
	ConsumeOrchestratorResponses(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error
	PurgeQueues(ctx context.Context) error
}

func NewServer(ctx context.Context, pub JobPublisher, traceClient pb.TraceabilityQueryClient) *OrchestratorServer {
	eng, err := engine.NewEngine(ctx)
	if err != nil {
		logger.Fatal("System", "Failed to initialize engine: %v", err)
	}

	srv := &OrchestratorServer{
		eng:          eng,
		pub:          pub,
		webhookConns: make(map[string]*grpc.ClientConn),
		grpcMaxMsgMb: envutil.MustGetInt("GRPC_MAX_MESSAGE_SIZE_MB"),
		grpcTimeout:  envutil.MustGetInt("GRPC_TIMEOUT_SECONDS"),
	}

	if err := srv.pub.ConsumeWebhooks(ctx, srv.processWebhookMsg); err != nil {
		logger.Error("System", "Failed to start webhooks consumer: %v", err)
	}

	if err := srv.pub.ConsumeOrchestratorResponses(ctx, srv.processOrchestratorResponses); err != nil {
		logger.Error("System", "Failed to start orchestrator responses consumer: %v", err)
	}

	return srv
}

func (s *OrchestratorServer) ExecuteTask(ctx context.Context, req *pb.ExecuteTaskRequest) (*pb.ExecuteTaskResponse, error) {
	if req == nil {
		panic(fmt.Errorf("action failed for job ExecuteTask: missing required property request"))
	}
	if req.Identity == nil {
		panic(fmt.Errorf("action failed for job ExecuteTask: missing required property Identity"))
	}

	jobID := req.Identity.JobId
	if jobID == "" || req.Identity.AppId == "" || req.Identity.TenantId == "" {
		panic(fmt.Errorf("action failed for job ExecuteTask: incomplete Identity"))
	}

	if req.TaskSubject == "" {
		panic(fmt.Errorf("action failed for job %s: missing required property TaskSubject", jobID))
	}

	logger.Info("System", "Received ExecuteTask for JobID: %s, StepID: %s, TaskSubject: %s", jobID, req.StepId, req.TaskSubject)
	reqJSON := protojson.MarshalOptions{Multiline: false}.Format(req)
	logger.Info("System", "Incoming ExecuteTask request payload: %s", reqJSON)

	err := s.eng.ExecuteTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute task: %w", err)
	}

	return &pb.ExecuteTaskResponse{
		Success: true,
		Message: "Task queued for execution",
		StepId:  req.StepId,
	}, nil
}

func (s *OrchestratorServer) processWebhookMsg(ctx context.Context, msg jetstream.Msg) error {
	var evt events.WebhookEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		panic(fmt.Errorf("action failed for job processWebhookMsg: corrupted JSON: %v", err))
	}

	if evt.Request == nil {
		panic(fmt.Errorf("action failed for job processWebhookMsg: missing required property request"))
	}
	if evt.Request.Identity == nil {
		panic(fmt.Errorf("action failed for job processWebhookMsg: invalid identity"))
	}
	if evt.Request.Identity.TenantId == "" || evt.Request.Identity.AppId == "" || evt.Request.Identity.JobId == "" {
		panic(fmt.Errorf("action failed for job processWebhookMsg: invalid identity"))
	}

	addr := evt.Request.TaskSubject
	if addr == "" {
		addr = workflowparser.NewTaskRequestParser(evt.Request).MustGetCallbackAddress(evt.Request.Identity.JobId)
	}

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

	if evt.ErrorContract != nil {
		if evt.IsSuspended {
			_, err := client.OnJobSuspended(ctx, &pb.JobSuspendedRequest{
				Identity: evt.Request.Identity,
				StepId:   evt.Request.StepId,
				Error:    evt.ErrorContract,
			})
			if err != nil {
				return fmt.Errorf("failed to call OnJobSuspended: %w", err)
			}
		} else {
			_, err := client.OnJobFailed(ctx, &pb.JobFailedRequest{
				Identity: evt.Request.Identity,
				StepId:   evt.Request.StepId,
				Error:    evt.ErrorContract,
			})
			if err != nil {
				return fmt.Errorf("failed to call OnJobFailed: %w", err)
			}
		}
		return nil
	}

	if evt.IsSuspended {
		_, err := client.OnJobSuspended(ctx, &pb.JobSuspendedRequest{
			Identity: evt.Request.Identity,
			StepId:   evt.Request.StepId,
		})
		if err != nil {
			return fmt.Errorf("failed to call OnJobSuspended: %w", err)
		}
		return nil
	}

	stream, err := client.OnTaskExecutionComplete(ctx)
	if err != nil {
		return fmt.Errorf("failed to open OnTaskExecutionComplete stream: %w", err)
	}

	if err := stream.Send(evt.Request); err != nil {
		return fmt.Errorf("failed to send on stream: %w", err)
	}

	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close stream: %w", err)
	}

	if _, err := stream.Recv(); err != nil {
		return fmt.Errorf("failed to receive ack: %w", err)
	}

	logger.Info("System", "Successfully relayed webhook callback for Job: %s, Step: %s to %s", evt.Request.Identity.JobId, evt.Request.StepId, addr)
	return nil
}

func (s *OrchestratorServer) PurgeSystemState(ctx context.Context, req *pb.PurgeSystemStateRequest) (*pb.PurgeSystemStateResponse, error) {
	if req != nil && req.PurgeQueues && s.pub != nil {
		if err := s.pub.PurgeQueues(ctx); err != nil {
			logger.Error("System", "Failed to purge queues: %v", err)
			return &pb.PurgeSystemStateResponse{Success: false}, err
		}
	}
	return &pb.PurgeSystemStateResponse{Success: true}, nil
}
