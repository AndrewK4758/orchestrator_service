package server

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"sync"
	"testing"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func init() {
	os.Setenv("SEMANTIC_CACHE_SERVICE_ADDRESS", "localhost:11115")
	os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", "4")
	os.Setenv("CACHE_TIMEOUT_SECONDS", "5")
	os.Setenv("GRPC_TIMEOUT_SECONDS", "30")
	os.Setenv("NATS_TIMEOUT_SECONDS", "30")
	os.Setenv("WEBHOOKS_MAX_CONCURRENCY", "10")
}

type mockPublisher struct {
	mu               sync.Mutex
	publishedWebhook *events.WebhookEvent
	publishedMsgs    [][]byte
}

func (m *mockPublisher) PublishWebhook(ctx context.Context, payload *events.WebhookEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedWebhook = payload
	return nil
}

func (m *mockPublisher) ConsumeWebhooks(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}

func (m *mockPublisher) ConsumeOrchestratorResponses(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}

func (m *mockPublisher) PurgeQueues(ctx context.Context) error {
	return nil
}

type mockMsg struct {
	jetstream.Msg
	data []byte
}

func (m mockMsg) Data() []byte {
	return m.data
}

func TestOrchestratorServer_ExecuteTask(t *testing.T) {
	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:          pub,
		webhookConns: make(map[string]*grpc.ClientConn),
		grpcMaxMsgMb: 4,
		grpcTimeout:  30,
	}

	t.Run("ExecuteTask missing identity returns error", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{}
		_, err := srv.ExecuteTask(context.Background(), req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "InfrastructureIdentity is strictly required")
	})

	t.Run("ExecuteTask missing jobId returns error", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1"},
		}
		_, err := srv.ExecuteTask(context.Background(), req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "InfrastructureIdentity.JobId is strictly required")
	})

	t.Run("ExecuteTask integration requires NATS", func(t *testing.T) {
		t.Skip("Skipping integration test requiring NATS")
	})
}

type mockCallbackServer struct {
	pb.UnimplementedOrchestratorCallbackServiceServer
	mu                      sync.Mutex
	receivedSuccess         *pb.ExecuteTaskRequest
	receivedFail            *pb.ExecuteTaskRequest
	receivedSuspend         *pb.ExecuteTaskRequest
	receivedSuccessRequests []*pb.ExecuteTaskRequest
	receivedFailRequests    []*pb.ExecuteTaskRequest
	receivedSuspendRequests []*pb.ExecuteTaskRequest
	onSuccessCount          int
	onFailCount             int
	onSuspendCount          int
	streamError             error
	failError               error
	suspendError            error
}

func (s *mockCallbackServer) OnTaskExecutionComplete(stream grpc.BidiStreamingServer[pb.ExecuteTaskRequest, pb.TaskExecutionCompleteResponse]) error {
	s.mu.Lock()
	if s.streamError != nil {
		err := s.streamError
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.receivedSuccess = req
	s.receivedSuccessRequests = append(s.receivedSuccessRequests, req)
	s.onSuccessCount++
	s.mu.Unlock()
	return stream.Send(&pb.TaskExecutionCompleteResponse{Success: true})
}

func (s *mockCallbackServer) OnJobFailed(ctx context.Context, req *pb.ExecuteTaskRequest) (*pb.JobFailedResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failError != nil {
		return nil, s.failError
	}
	s.receivedFail = req
	s.receivedFailRequests = append(s.receivedFailRequests, req)
	s.onFailCount++
	return &pb.JobFailedResponse{Success: true}, nil
}

func (s *mockCallbackServer) OnJobSuspended(ctx context.Context, req *pb.ExecuteTaskRequest) (*pb.JobSuspendedResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.suspendError != nil {
		return nil, s.suspendError
	}
	s.receivedSuspend = req
	s.receivedSuspendRequests = append(s.receivedSuspendRequests, req)
	s.onSuspendCount++
	return &pb.JobSuspendedResponse{Success: true}, nil
}

func (s *mockCallbackServer) GetSuccessRequests() []*pb.ExecuteTaskRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.ExecuteTaskRequest, len(s.receivedSuccessRequests))
	copy(cp, s.receivedSuccessRequests)
	return cp
}

func (s *mockCallbackServer) GetFailRequests() []*pb.ExecuteTaskRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.ExecuteTaskRequest, len(s.receivedFailRequests))
	copy(cp, s.receivedFailRequests)
	return cp
}

func (s *mockCallbackServer) GetSuspendRequests() []*pb.ExecuteTaskRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.ExecuteTaskRequest, len(s.receivedSuspendRequests))
	copy(cp, s.receivedSuspendRequests)
	return cp
}

func (s *mockCallbackServer) OnSuccessCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.onSuccessCount
}

func (s *mockCallbackServer) OnFailCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.onFailCount
}

func (s *mockCallbackServer) OnSuspendCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.onSuspendCount
}

func (s *mockCallbackServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receivedSuccess = nil
	s.receivedFail = nil
	s.receivedSuspend = nil
	s.receivedSuccessRequests = nil
	s.receivedFailRequests = nil
	s.receivedSuspendRequests = nil
	s.onSuccessCount = 0
	s.onFailCount = 0
	s.onSuspendCount = 0
	s.streamError = nil
	s.failError = nil
	s.suspendError = nil
}

func setupMockCallbackServer(t *testing.T) (*mockCallbackServer, string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	grpcServer := grpc.NewServer()
	cbMock := &mockCallbackServer{}
	pb.RegisterOrchestratorCallbackServiceServer(grpcServer, cbMock)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return cbMock, lis.Addr().String(), cleanup
}

func TestOrchestratorServer_PurgeSystemState(t *testing.T) {
	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub: pub,
	}

	purgeReq := &pb.PurgeSystemStateRequest{
		Identity:    &pb.InfrastructureIdentity{AppId: "app1"},
		PurgeQueues: true,
	}

	res, err := srv.PurgeSystemState(context.Background(), purgeReq)
	assert.NoError(t, err)
	assert.True(t, res.Success)
}

type webhookRoutingTestCase struct {
	name            string
	jobID           string
	stepID          string
	isSuspended     bool
	errorContract   *pb.ErrorContract
	overrideAddr    string
	expectError     bool
	expectedSuccess bool
	expectOnSuccess int
	expectOnFail    int
	expectOnSuspend int
	validatePayload func(t *testing.T, req *pb.ExecuteTaskRequest)
}

func TestServer_ProcessWebhookMsg_ClientCallbackRouting_TableDriven(t *testing.T) {
	cbMock, cbAddr, cleanup := setupMockCallbackServer(t)
	defer cleanup()

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:          pub,
		webhookConns: make(map[string]*grpc.ClientConn),
		grpcTimeout:  30,
		grpcMaxMsgMb: 4,
	}

	tests := []webhookRoutingTestCase{
		{
			name:            "Success_InvokesOnNodeExecutionComplete",
			jobID:           "job_success_100",
			stepID:          "step_parse_doc",
			isSuspended:     false,
			errorContract:   nil,
			expectError:     false,
			expectedSuccess: true,
			expectOnSuccess: 1,
			expectOnFail:    0,
			expectOnSuspend: 0,
			validatePayload: func(t *testing.T, req *pb.ExecuteTaskRequest) {
				assert.Equal(t, "job_success_100", req.Identity.JobId)
				assert.Equal(t, "step_parse_doc", req.StepId)
			},
		},
		{
			name:        "Failure_InvokesOnJobFailed",
			jobID:       "job_failed_200",
			stepID:      "step_llm_inference",
			isSuspended: false,
			errorContract: &pb.ErrorContract{
				Code:    "ERR_LLM_TIMEOUT",
				Message: "LLM service timed out after 30s",
			},
			expectError:     false,
			expectedSuccess: false,
			expectOnSuccess: 0,
			expectOnFail:    1,
			expectOnSuspend: 0,
			validatePayload: func(t *testing.T, req *pb.ExecuteTaskRequest) {
				assert.Equal(t, "job_failed_200", req.Identity.JobId)
				assert.Equal(t, "step_llm_inference", req.StepId)
			},
		},
		{
			name:            "Suspended_InvokesOnJobSuspended",
			jobID:           "job_suspended_300",
			stepID:          "step_human_validation",
			isSuspended:     true,
			errorContract:   nil,
			expectError:     false,
			expectedSuccess: false,
			expectOnSuccess: 0,
			expectOnFail:    0,
			expectOnSuspend: 1,
			validatePayload: func(t *testing.T, req *pb.ExecuteTaskRequest) {
				assert.Equal(t, "job_suspended_300", req.Identity.JobId)
				assert.Equal(t, "step_human_validation", req.StepId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cbMock.Reset()

			targetAddr := cbAddr
			if tt.overrideAddr != "" {
				targetAddr = tt.overrideAddr
			}

			evt := &events.WebhookEvent{
				Request: &pb.ExecuteTaskRequest{
					Identity: &pb.InfrastructureIdentity{
						TenantId: "tenant_test",
						AppId:    "app_test",
						JobId:    tt.jobID,
					},
					StepId:      tt.stepID,
					TaskSubject: targetAddr,
				},
				ErrorContract: tt.errorContract,
				IsSuspended:   tt.isSuspended,
			}

			evtBytes, err := json.Marshal(evt)
			assert.NoError(t, err)

			msg := mockMsg{data: evtBytes}
			err = srv.processWebhookMsg(context.Background(), msg)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectOnSuccess, cbMock.OnSuccessCount(), "OnSuccess count mismatch")
				assert.Equal(t, tt.expectOnFail, cbMock.OnFailCount(), "OnFail count mismatch")
				assert.Equal(t, tt.expectOnSuspend, cbMock.OnSuspendCount(), "OnSuspend count mismatch")
				if tt.validatePayload != nil {
					reqs := cbMock.GetSuccessRequests()
					if len(reqs) == 0 {
						reqs = cbMock.GetFailRequests()
					}
					if len(reqs) == 0 {
						reqs = cbMock.GetSuspendRequests()
					}
					if len(reqs) > 0 {
						tt.validatePayload(t, reqs[0])
					}
				}
			}
		})
	}
}

func TestServer_ProcessWebhookMsg_DirectClientPoolRouting_MultiServer(t *testing.T) {
	mockServerA, addrA, cleanupA := setupMockCallbackServer(t)
	defer cleanupA()

	mockServerB, addrB, cleanupB := setupMockCallbackServer(t)
	defer cleanupB()

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:          pub,
		webhookConns: make(map[string]*grpc.ClientConn),
		grpcTimeout:  30,
		grpcMaxMsgMb: 4,
	}

	evt1 := &events.WebhookEvent{
		Request: &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "job_server_a"},
			StepId:      "step_a",
			TaskSubject: addrA,
		},
	}
	evt1Bytes, _ := json.Marshal(evt1)
	err1 := srv.processWebhookMsg(context.Background(), mockMsg{data: evt1Bytes})
	assert.NoError(t, err1)

	evt2 := &events.WebhookEvent{
		Request: &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "job_server_b"},
			StepId:      "step_b",
			TaskSubject: addrB,
		},
	}
	evt2Bytes, _ := json.Marshal(evt2)
	err2 := srv.processWebhookMsg(context.Background(), mockMsg{data: evt2Bytes})
	assert.NoError(t, err2)

	assert.Equal(t, 1, mockServerA.OnSuccessCount())
	assert.Equal(t, 1, mockServerB.OnSuccessCount())
	assert.Equal(t, "job_server_a", mockServerA.GetSuccessRequests()[0].Identity.JobId)
	assert.Equal(t, "job_server_b", mockServerB.GetSuccessRequests()[0].Identity.JobId)
}
