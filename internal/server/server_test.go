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
	data    []byte
	subject string
}

func (m mockMsg) Data() []byte {
	return m.data
}

func (m mockMsg) Subject() string {
	return m.subject
}

func TestOrchestratorServer_ExecuteTask(t *testing.T) {
	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:          pub,
		webhookConns: make(map[string]*clientConnEntry),
		grpcMaxMsgMb: 4,
		grpcTimeout:  30,
	}

	t.Run("ExecuteTask missing identity panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ExecuteTask missing jobId panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1"},
		}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ExecuteTask missing CallbackAddress panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			TaskSubject: "worker.test",
		}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ExecuteTask integration requires NATS", func(t *testing.T) {
		t.Skip("Skipping integration test requiring NATS")
	})
}

type mockCallbackServer struct {
	pb.UnimplementedOrchestratorCallbackServiceServer
	mu                      sync.Mutex
	receivedSuccess         *pb.ExecuteTaskRequest
	receivedFail            *pb.JobFailedRequest
	receivedSuspend         *pb.JobSuspendedRequest
	receivedSuccessRequests []*pb.ExecuteTaskRequest
	receivedFailRequests    []*pb.JobFailedRequest
	receivedSuspendRequests []*pb.JobSuspendedRequest
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

func (s *mockCallbackServer) OnJobFailed(ctx context.Context, req *pb.JobFailedRequest) (*pb.JobFailedResponse, error) {
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

func (s *mockCallbackServer) OnJobSuspended(ctx context.Context, req *pb.JobSuspendedRequest) (*pb.JobSuspendedResponse, error) {
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

func (s *mockCallbackServer) GetFailRequests() []*pb.JobFailedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.JobFailedRequest, len(s.receivedFailRequests))
	copy(cp, s.receivedFailRequests)
	return cp
}

func (s *mockCallbackServer) GetSuspendRequests() []*pb.JobSuspendedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.JobSuspendedRequest, len(s.receivedSuspendRequests))
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
	validatePayload func(t *testing.T, req any)
}

func TestServer_ProcessWebhookMsg_ClientCallbackRouting_TableDriven(t *testing.T) {
	cbMock, cbAddr, cleanup := setupMockCallbackServer(t)
	defer cleanup()

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:          pub,
		webhookConns: make(map[string]*clientConnEntry),
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
			validatePayload: func(t *testing.T, req any) {
				r := req.(*pb.ExecuteTaskRequest)
				assert.Equal(t, "job_success_100", r.Identity.JobId)
				assert.Equal(t, "step_parse_doc", r.StepId)
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
			validatePayload: func(t *testing.T, req any) {
				r := req.(*pb.JobFailedRequest)
				assert.Equal(t, "job_failed_200", r.Identity.JobId)
				assert.Equal(t, "step_llm_inference", r.StepId)
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
			validatePayload: func(t *testing.T, req any) {
				r := req.(*pb.JobSuspendedRequest)
				assert.Equal(t, "job_suspended_300", r.Identity.JobId)
				assert.Equal(t, "step_human_validation", r.StepId)
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
					StepId:          tt.stepID,
					TaskSubject:     "worker.test",
					CallbackAddress: targetAddr,
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
					if len(cbMock.GetSuccessRequests()) > 0 {
						tt.validatePayload(t, cbMock.GetSuccessRequests()[0])
					} else if len(cbMock.GetFailRequests()) > 0 {
						tt.validatePayload(t, cbMock.GetFailRequests()[0])
					} else if len(cbMock.GetSuspendRequests()) > 0 {
						tt.validatePayload(t, cbMock.GetSuspendRequests()[0])
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
		webhookConns: make(map[string]*clientConnEntry),
		grpcTimeout:  30,
		grpcMaxMsgMb: 4,
	}

	evt1 := &events.WebhookEvent{
		Request: &pb.ExecuteTaskRequest{
			Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "job_server_a"},
			StepId:          "step_a",
			TaskSubject:     "worker.test",
			CallbackAddress: addrA,
		},
	}
	evt1Bytes, _ := json.Marshal(evt1)
	err1 := srv.processWebhookMsg(context.Background(), mockMsg{data: evt1Bytes})
	assert.NoError(t, err1)

	evt2 := &events.WebhookEvent{
		Request: &pb.ExecuteTaskRequest{
			Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "job_server_b"},
			StepId:          "step_b",
			TaskSubject:     "worker.test",
			CallbackAddress: addrB,
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

type mockEngine struct {
	resumeJobFunc func(ctx context.Context, req *pb.ResumeJobRequest) error
}

func (m *mockEngine) ExecuteTask(ctx context.Context, req *pb.ExecuteTaskRequest) error {
	return nil
}

func (m *mockEngine) ResumeJob(ctx context.Context, req *pb.ResumeJobRequest) error {
	if m.resumeJobFunc != nil {
		return m.resumeJobFunc(ctx, req)
	}
	return nil
}

func TestOrchestratorServer_ResumeJob(t *testing.T) {
	cbMock, cbAddr, cleanup := setupMockCallbackServer(t)
	defer cleanup()

	mockEng := &mockEngine{}
	srv := &OrchestratorServer{
		eng:          mockEng,
		webhookConns: make(map[string]*clientConnEntry),
		grpcTimeout:  30,
		grpcMaxMsgMb: 4,
	}

	ctx := context.Background()

	t.Run("Happy Path Dispatches gRPC Callback", func(t *testing.T) {
		cbMock.Reset()
		var capturedEngReq *pb.ResumeJobRequest
		mockEng.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) error {
			capturedEngReq = req
			return nil
		}

		req := &pb.ResumeJobRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_resume",
				AppId:    "app_resume",
				JobId:    "job_resume_123",
			},
			StepId:          "step_classify",
			CallbackAddress: cbAddr,
		}

		resp, err := srv.ResumeJob(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.True(t, resp.GetSuccess())
		assert.Equal(t, "Job resumed successfully", resp.GetMessage())
		assert.Equal(t, "job_resume_123", capturedEngReq.GetIdentity().GetJobId())
		assert.Equal(t, "step_classify", capturedEngReq.GetStepId())
		assert.Equal(t, 1, cbMock.OnSuccessCount())
		assert.Equal(t, "job_resume_123", cbMock.GetSuccessRequests()[0].Identity.JobId)
		assert.Equal(t, "step_classify", cbMock.GetSuccessRequests()[0].StepId)
	})

	t.Run("Callback dispatch failure returns Success false", func(t *testing.T) {
		req := &pb.ResumeJobRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant_resume",
				AppId:    "app_resume",
				JobId:    "job_resume_fail",
			},
			StepId:          "step_fail",
			CallbackAddress: "127.0.0.1:54321", // non-existent server
		}

		resp, err := srv.ResumeJob(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.False(t, resp.GetSuccess())
		assert.Contains(t, resp.GetMessage(), "failed to resume job callback")
	})

	t.Run("Fail-fast panics on missing identity or callback address", func(t *testing.T) {
		assert.Panics(t, func() {
			_, _ = srv.ResumeJob(ctx, nil)
		})

		assert.Panics(t, func() {
			_, _ = srv.ResumeJob(ctx, &pb.ResumeJobRequest{
				StepId:          "step1",
				CallbackAddress: cbAddr,
			})
		})

		assert.Panics(t, func() {
			_, _ = srv.ResumeJob(ctx, &pb.ResumeJobRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "",
					AppId:    "app",
					JobId:    "job",
				},
				StepId:          "step1",
				CallbackAddress: cbAddr,
			})
		})

		assert.Panics(t, func() {
			_, _ = srv.ResumeJob(ctx, &pb.ResumeJobRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "tenant",
					AppId:    "app",
					JobId:    "job",
				},
				StepId:          "",
				CallbackAddress: cbAddr,
			})
		})

		assert.Panics(t, func() {
			_, _ = srv.ResumeJob(ctx, &pb.ResumeJobRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "tenant",
					AppId:    "app",
					JobId:    "job",
				},
				StepId:          "step1",
				CallbackAddress: "",
			})
		})
	})
}

