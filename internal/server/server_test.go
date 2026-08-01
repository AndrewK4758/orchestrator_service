package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func init() {
	os.Setenv("SEMANTIC_CACHE_SERVICE_ADDRESS", "localhost:11115")
	os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", "4")
	os.Setenv("CACHE_TIMEOUT_SECONDS", "5")
	os.Setenv("GRPC_TIMEOUT_SECONDS", "30")
	os.Setenv("NATS_TIMEOUT_SECONDS", "30")
	os.Setenv("WEBHOOKS_MAX_CONCURRENCY", "10")
}

type mockExecuteWorkflowNodeServer struct {
	grpc.ServerStream
	ctx      context.Context
	reqs     []*pb.ExecuteWorkflowNodeRequest
	reqIndex int
	resp     *pb.ExecuteWorkflowNodeResponse
}

func (m *mockExecuteWorkflowNodeServer) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *mockExecuteWorkflowNodeServer) Recv() (*pb.ExecuteWorkflowNodeRequest, error) {
	if m.reqIndex < len(m.reqs) {
		req := m.reqs[m.reqIndex]
		m.reqIndex++
		return req, nil
	}
	return nil, io.EOF
}

func (m *mockExecuteWorkflowNodeServer) Send(res *pb.ExecuteWorkflowNodeResponse) error {
	m.resp = res
	return nil
}

type mockPublisher struct {
	mu               sync.Mutex
	publishedWebhook *events.WebhookEvent
	publishedMsgs    [][]byte
}

func (m *mockPublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	return nil
}
func (m *mockPublisher) PublishCacheStore(ctx context.Context, data []byte) error { return nil }

func (m *mockPublisher) PublishJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}

func (m *mockPublisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}
func (m *mockPublisher) PurgeQueues(ctx context.Context) error { return nil }
func (m *mockPublisher) PublishWebhook(ctx context.Context, payload *events.WebhookEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedWebhook = payload
	return nil
}
func (m *mockPublisher) ConsumeWebhooks(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}
func (m *mockPublisher) GetActiveJobsKV(ctx context.Context) (jetstream.KeyValue, error) {
	return nil, nil // Return nil or a mock KV store if needed
}

func TestOrchestratorServer_ExecuteWorkflowNode(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), pub, nil)

	t.Run("ExecuteWorkflowNode queues to internal memory cache and NATS", func(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
		req := &pb.ExecuteWorkflowNodeRequest{
			Identity: &pb.InfrastructureIdentity{JobId: "job_1", TenantId: "tenant_1", AppId: "app_1"},
			StepId:   "step_1",
			Metadata: map[string]string{"app_id": "test_app"},
		}
		res, err := srv.ExecuteWorkflowNode(context.Background(), req)

		assert.NoError(t, err)
		assert.True(t, res.Success)
		assert.Equal(t, "step_1", res.StepId)

		assert.Equal(t, 1, len(pub.publishedMsgs))
	})
}

type mockMsg struct {
	jetstream.Msg
	data []byte
}

func (m mockMsg) Data() []byte {
	return m.data
}

func TestIdempotentExecution_Concurrent(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_concurrent", TenantId: "tenant_1", AppId: "app_1"},
		Metadata: map[string]string{"app_id": "test_app"},
		StepId:   "step_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}

	// We'll simulate Jetstream delivering the same message twice concurrently.
	msgBytes, _ := proto.Marshal(req)

	var wg sync.WaitGroup
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
			if err != nil && err.Error() != "node config is missing" {
				// ignore
			}
		}()
	}

	wg.Wait()

	// We assert it doesn't crash or trigger race conditions running 3 concurrently.
}

type mockCallbackServer struct {
	pb.UnimplementedOrchestratorCallbackServiceServer
	mu                      sync.Mutex
	receivedSuccess         *pb.ExecuteWorkflowNodeRequest
	receivedFail            *pb.ExecuteWorkflowNodeRequest
	receivedSuspend         *pb.ExecuteWorkflowNodeRequest
	receivedSuccessRequests []*pb.ExecuteWorkflowNodeRequest
	receivedFailRequests    []*pb.ExecuteWorkflowNodeRequest
	receivedSuspendRequests []*pb.ExecuteWorkflowNodeRequest
	onSuccessCount          int
	onFailCount             int
	onSuspendCount          int
	streamError             error
	failError               error
	suspendError            error
}

func (s *mockCallbackServer) OnNodeExecutionComplete(stream grpc.BidiStreamingServer[pb.ExecuteWorkflowNodeRequest, pb.NodeExecutionCompleteResponse]) error {
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
	return stream.Send(&pb.NodeExecutionCompleteResponse{Success: true})
}

func (s *mockCallbackServer) OnJobFailed(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*pb.JobFailedResponse, error) {
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

func (s *mockCallbackServer) OnJobSuspended(ctx context.Context, req *pb.ExecuteWorkflowNodeRequest) (*pb.JobSuspendedResponse, error) {
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

func (s *mockCallbackServer) GetSuccessRequests() []*pb.ExecuteWorkflowNodeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.ExecuteWorkflowNodeRequest, len(s.receivedSuccessRequests))
	copy(cp, s.receivedSuccessRequests)
	return cp
}

func (s *mockCallbackServer) GetFailRequests() []*pb.ExecuteWorkflowNodeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.ExecuteWorkflowNodeRequest, len(s.receivedFailRequests))
	copy(cp, s.receivedFailRequests)
	return cp
}

func (s *mockCallbackServer) GetSuspendRequests() []*pb.ExecuteWorkflowNodeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*pb.ExecuteWorkflowNodeRequest, len(s.receivedSuspendRequests))
	copy(cp, s.receivedSuspendRequests)
	return cp
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

func TestOrchestratorServer_GenericStateFuzzing(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	// Setup mock callback server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	cbAddress := lis.Addr().String()

	grpcServer := grpc.NewServer()
	cbMock := &mockCallbackServer{}
	pb.RegisterOrchestratorCallbackServiceServer(grpcServer, cbMock)
	go func() {
		grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	pub := &mockPublisher{}
	srv := NewServer(context.Background(), pub, nil)

	// Setup mock cache client to force a cache hit (bypassing AI)
	srv.cacheClient = &mockCacheClient{}

	funcStruct, _ := structpb.NewStruct(map[string]interface{}{
		"NestedObj": map[string]interface{}{
			"UnknownField": "Value1",
			"ListField":    []interface{}{1.0, 2.0, 3.0},
		},
		"TopLevelString": "ArbitraryData",
	})

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_fuzz_1", TenantId: "tenant_1", AppId: "app_1"},
		StepId:          "step_1",
		CallbackAddress: cbAddress,
		GlobalState:     &pb.GlobalState{Functional: funcStruct},
		Metadata: map[string]string{
			"app_id":          "FuzzApp",
			"correlation_id":  "123-abc",
			"x-custom-header": "test",
		},
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
		Cache: &pb.CacheContract{
			Enabled:        true,
			CollectionName: "test_cache",
		},
	}

	srv.cacheClient = &mockCacheClient{
		checkCacheFn: func(ctx context.Context, in *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			// Validate request was forwarded correctly
			assert.NotNil(t, in.Identity)
			assert.Equal(t, "job_fuzz_1", in.Identity.JobId)
			assert.Equal(t, "test_cache", in.CollectionName)
			return &pb.CheckCacheResponse{
				Hit:              true,
				Confidence:       1.0,
				ExtractedPayload: `{"mock": "cache_result"}`,
			}, nil
		},
	}

	// 1. First test ExecuteWorkflowNode safely merges metadata and doesn't drop schema
	res, err := srv.ExecuteWorkflowNode(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.True(t, res.Success)
	assert.Equal(t, "FuzzApp", req.Metadata["app_id"]) // Validates the app_id wasn't mutated in a hardcoded way

	// 2. Pass to handleJobMessage. It should fail since there's no actual AI configured,
	// triggering OnJobFailed but WITH the unaltered JobID and proper Traceability.
	msgBytes, _ := proto.Marshal(req)
	err = srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.NoError(t, err) // the handler itself returns nil, but fires callback

	// Now we simulate JetStream picking up the published webhook payload
	assert.NotNil(t, pub.publishedWebhook, "Should have published a webhook event via cache bypass")
	webhookBytes, _ := json.Marshal(pub.publishedWebhook)
	err = srv.processWebhookMsg(context.Background(), mockMsg{data: webhookBytes})
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond) // wait for callback

	cbMock.mu.Lock()
	defer cbMock.mu.Unlock()

	assert.NotNil(t, cbMock.receivedSuccess, "Should have received OnNodeExecutionComplete")
	assert.Equal(t, "job_fuzz_1", cbMock.receivedSuccess.Identity.JobId)

	// Validate the generic state wasn't corrupted in the payload delivered back to the client
	returnedStateBytes, _ := protojson.Marshal(cbMock.receivedSuccess.GlobalState)
	returnedState := string(returnedStateBytes)
	assert.NotEmpty(t, returnedState)
	assert.Contains(t, returnedState, "Value1")
	assert.Contains(t, returnedState, "ArbitraryData")
}

type mockCacheClient struct {
	checkCacheFn func(ctx context.Context, in *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error)
}

func (m *mockCacheClient) CheckCache(ctx context.Context, in *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
	if m.checkCacheFn != nil {
		return m.checkCacheFn(ctx, in, opts...)
	}
	return &pb.CheckCacheResponse{
		Hit:              true,
		Confidence:       1.0,
		ExtractedPayload: `{"mock": "cache_result"}`,
	}, nil
}

func (m *mockCacheClient) StoreExtraction(ctx context.Context, in *pb.StoreExtractionRequest, opts ...grpc.CallOption) (*pb.StoreExtractionResponse, error) {
	return &pb.StoreExtractionResponse{Success: true}, nil
}

func (m *mockCacheClient) SeedCache(ctx context.Context, in *pb.SeedCacheRequest, opts ...grpc.CallOption) (*pb.SeedCacheResponse, error) {
	return &pb.SeedCacheResponse{Success: true}, nil
}

func (m *mockCacheClient) RegisterBlankDocument(ctx context.Context, in *pb.RegisterBlankDocumentRequest, opts ...grpc.CallOption) (*pb.RegisterBlankDocumentResponse, error) {
	return &pb.RegisterBlankDocumentResponse{
		Success: true,
	}, nil
}

func (m *mockCacheClient) QueryBlankDocument(ctx context.Context, in *pb.QueryBlankDocumentRequest, opts ...grpc.CallOption) (*pb.QueryBlankDocumentResponse, error) {
	return &pb.QueryBlankDocumentResponse{}, nil
}

func (m *mockCacheClient) CheckMetadataExists(ctx context.Context, in *pb.CheckMetadataRequest, opts ...grpc.CallOption) (*pb.CheckMetadataResponse, error) {
	return &pb.CheckMetadataResponse{Exists: true}, nil
}

func TestOrchestratorServer_CallbackTimeoutResiliency(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_timeout_1", TenantId: "tenant_1", AppId: "app_1"},
		StepId:          "step_1",
		CallbackAddress: "127.0.0.1:9999", // Unreachable port
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}

	msgBytes, _ := proto.Marshal(req)

	// handleJobMessage should succeed and publish the webhook event
	err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.NoError(t, err)

	assert.NotNil(t, pub.publishedWebhook)
	webhookBytes, _ := json.Marshal(pub.publishedWebhook)

	// processWebhookMsg should fail to dial and return an error
	err = srv.processWebhookMsg(context.Background(), mockMsg{data: webhookBytes})
	assert.Error(t, err)
}

func TestOrchestratorServer_CacheServiceTimeoutFallback(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_cache_timeout", TenantId: "tenant_1", AppId: "app_1"},
		StepId:   "step_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
		Cache: &pb.CacheContract{
			Enabled:        true,
			CollectionName: "test_cache",
		},
	}

	timeoutTriggered := false

	// Simulate cache timeout by returning context.DeadlineExceeded
	srv.cacheClient = &mockCacheClient{
		checkCacheFn: func(ctx context.Context, in *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			timeoutTriggered = true
			return nil, context.DeadlineExceeded
		},
	}

	// But because we want the engine to execute without panicking or actually running AI,
	// wait, if we mock the engine, or just let it fail because we didn't mock AI?
	// The AI will fail since there is no API key, but we want to assert that the fallback is attempted!

	msgBytes, _ := proto.Marshal(req)
	err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})

	// The step executor engine will fail because there is no API key / mock AI setup in this basic test,
	// but it should NOT return an error specifically from the cache service.
	assert.NoError(t, err) // handleJobMessage itself returns nil but fires failure callback
	assert.True(t, timeoutTriggered, "Cache service should have been queried and timed out")

	// We can check if `activeJobs` is cleaned up or if callback was fired with failure
}

type mockKeyValue struct {
	jetstream.KeyValue
	getFn    func(ctx context.Context, key string) (jetstream.KeyValueEntry, error)
	deleteFn func(ctx context.Context, key string) error
}

func (m *mockKeyValue) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if m.getFn != nil {
		return m.getFn(ctx, key)
	}
	return nil, jetstream.ErrKeyNotFound
}

func (m *mockKeyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, key)
	}
	return nil
}

type mockKeyValueEntry struct {
	jetstream.KeyValueEntry
	val []byte
}

func (m *mockKeyValueEntry) Value() []byte {
	return m.val
}

func TestOrchestratorServer_ResumeJob(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	tests := []struct {
		name          string
		jobID         string
		corrected     *structpb.Struct
		setupKV       func(*testing.T, *mockKeyValue)
		validate      func(*testing.T, *pb.ResumeJobResponse, *mockPublisher, bool)
	}{
		{
			name:          "TestResumeJob_MergeArbitraryJson",
			jobID:         "job_merge",
			corrected: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"unknown_top_level": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"nested": structpb.NewStringValue("value"),
						},
					}),
					"list_field": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewNumberValue(1),
							structpb.NewNumberValue(2),
							structpb.NewNumberValue(3),
						},
					}),
				},
			},
			setupKV: func(t *testing.T, kv *mockKeyValue) {
				initialReq := &pb.ExecuteWorkflowNodeRequest{
					Identity:        &pb.InfrastructureIdentity{JobId: "job_merge", TenantId: "tenant_1", AppId: "app_1"},
					StepId:          "step_merge",
					CallbackAddress: "localhost",
					GlobalState: &pb.GlobalState{},
				}
				initialReqBytes, _ := proto.Marshal(initialReq)
				kv.getFn = func(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
					assert.Equal(t, "job_merge", key)
					return &mockKeyValueEntry{val: initialReqBytes}, nil
				}
				kv.deleteFn = func(ctx context.Context, key string) error { return nil }
			},
			validate: func(t *testing.T, res *pb.ResumeJobResponse, pub *mockPublisher, deleteCalled bool) {
				assert.True(t, res.Success)
				assert.NotNil(t, pub.publishedWebhook)
				assert.Equal(t, "step_merge", pub.publishedWebhook.Request.GetStepId())
			},
		},
		{
			name:          "TestResumeJob_Success",
			jobID:         "job_success",
			corrected: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"msg": structpb.NewStringValue("success"),
				},
			},
			setupKV: func(t *testing.T, kv *mockKeyValue) {
				initialReq := &pb.ExecuteWorkflowNodeRequest{
					Identity:        &pb.InfrastructureIdentity{JobId: "job_success", TenantId: "tenant_1", AppId: "app_1"},
					StepId:          "step_success",
					CallbackAddress: "localhost",
				}
				initialReqBytes, _ := proto.Marshal(initialReq)
				kv.getFn = func(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
					return &mockKeyValueEntry{val: initialReqBytes}, nil
				}
				kv.deleteFn = func(ctx context.Context, key string) error { return nil }
			},
			validate: func(t *testing.T, res *pb.ResumeJobResponse, pub *mockPublisher, deleteCalled bool) {
				assert.True(t, res.Success)
				assert.True(t, deleteCalled)
				assert.NotNil(t, pub.publishedWebhook)
				assert.Empty(t, pub.publishedMsgs) // Not republished
			},
		},
		{
			name:          "TestResumeJob_NotFound",
			jobID:         "job_missing",
			corrected: &structpb.Struct{
				Fields: map[string]*structpb.Value{},
			},
			setupKV: func(t *testing.T, kv *mockKeyValue) {
				kv.getFn = func(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
					return nil, jetstream.ErrKeyNotFound
				}
			},
			validate: func(t *testing.T, res *pb.ResumeJobResponse, pub *mockPublisher, deleteCalled bool) {
				assert.False(t, res.Success)
				assert.Equal(t, "Job not found in KV store", res.Message)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
			pub := &mockPublisher{}
			srv := NewServer(context.Background(), pub, nil)

			deleteCalled := false
			mockKV := &mockKeyValue{}
			tt.setupKV(t, mockKV)

			originalDelete := mockKV.deleteFn
			if originalDelete != nil {
				mockKV.deleteFn = func(ctx context.Context, key string) error {
					deleteCalled = true
					return originalDelete(ctx, key)
				}
			}

			srv.kvStore = mockKV

			resumeReq := &pb.ResumeJobRequest{
				Identity:      &pb.InfrastructureIdentity{JobId: tt.jobID, TenantId: "tenant_1", AppId: "app_1"},
				Corrected: tt.corrected,
			}

			res, err := srv.ResumeJob(context.Background(), resumeReq)
			assert.NoError(t, err)
			tt.validate(t, res, pub, deleteCalled)
		})
	}
}

func TestOrchestratorServer_PurgeSystemState(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	tests := []struct {
		name        string
		appID       string
		purgeQueues bool
		setupActive func(*OrchestratorServer) *bool
		validate    func(*testing.T, *OrchestratorServer, *pb.PurgeSystemStateResponse, *bool)
	}{
		{
			name:        "TestPurgeSystemState_WithActiveExecs",
			appID:       "tenant_app_1",
			purgeQueues: true,
			setupActive: func(srv *OrchestratorServer) *bool {
				cancelCalled := new(bool)
				srv.activeExecMu.Lock()
				srv.activeExecs["job_active_1"] = &JobExecInfo{
					AppID: "tenant_app_1",
					Cancel: func() {
						*cancelCalled = true
					},
				}
				srv.activeExecMu.Unlock()
				return cancelCalled
			},
			validate: func(t *testing.T, srv *OrchestratorServer, res *pb.PurgeSystemStateResponse, cancelCalled *bool) {
				assert.True(t, res.Success)
				assert.Equal(t, int32(1), res.JobsCancelled)
				assert.Equal(t, int32(3), res.QueuesPurged)
				assert.True(t, *cancelCalled)

				srv.activeExecMu.Lock()
				_, exists := srv.activeExecs["job_active_1"]
				srv.activeExecMu.Unlock()
				assert.False(t, exists)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
			pub := &mockPublisher{}
			srv := NewServer(context.Background(), pub, nil)

			cancelCalled := tt.setupActive(srv)

			purgeReq := &pb.PurgeSystemStateRequest{
				Identity:    &pb.InfrastructureIdentity{AppId: tt.appID},
				PurgeQueues: tt.purgeQueues,
			}

			res, err := srv.PurgeSystemState(context.Background(), purgeReq)
			assert.NoError(t, err)
			tt.validate(t, srv, res, cancelCalled)
		})
	}
}

type webhookRoutingTestCase struct {
	name            string
	jobID           string
	stepID          string
	isSuspended     bool
	errorContract   *pb.ErrorContract
	overrideAddr    string // if empty, defaults to the running mock server address
	expectError     bool
	expectedSuccess bool
	expectOnSuccess int
	expectOnFail    int
	expectOnSuspend int
	validatePayload func(t *testing.T, req *pb.ExecuteWorkflowNodeRequest)
}

func TestServer_ProcessWebhookMsg_ClientCallbackRouting_TableDriven(t *testing.T) {
	cbMock, cbAddr, cleanup := setupMockCallbackServer(t)
	defer cleanup()

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:                  pub,
		webhookConns:         make(map[string]*grpc.ClientConn),
		grpcTimeoutSecs:      30,
		grpcMaxMessageSizeMb: 4,
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
			validatePayload: func(t *testing.T, req *pb.ExecuteWorkflowNodeRequest) {
				assert.Equal(t, "job_success_100", req.Identity.JobId)
				assert.Equal(t, "step_parse_doc", req.StepId)
				assert.True(t, req.Success)
				assert.Nil(t, req.ErrorContract)
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
			validatePayload: func(t *testing.T, req *pb.ExecuteWorkflowNodeRequest) {
				assert.Equal(t, "job_failed_200", req.Identity.JobId)
				assert.Equal(t, "step_llm_inference", req.StepId)
				assert.False(t, req.Success)
				assert.NotNil(t, req.ErrorContract)
				assert.Equal(t, "ERR_LLM_TIMEOUT", req.ErrorContract.Code)
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
			validatePayload: func(t *testing.T, req *pb.ExecuteWorkflowNodeRequest) {
				assert.Equal(t, "job_suspended_300", req.Identity.JobId)
				assert.Equal(t, "step_human_validation", req.StepId)
				assert.False(t, req.Success)
			},
		},
		{
			name:            "EmptyCallbackAddress_SkippedWithNoError",
			jobID:           "job_empty_cb",
			stepID:          "step_1",
			isSuspended:     false,
			errorContract:   nil,
			overrideAddr:    "",
			expectError:     false,
			expectedSuccess: false,
			expectOnSuccess: 0,
			expectOnFail:    0,
			expectOnSuspend: 0,
		},
		{
			name:            "UnreachableCallbackAddress_ReturnsErrorForNack",
			jobID:           "job_unreachable",
			stepID:          "step_1",
			isSuspended:     false,
			errorContract:   nil,
			overrideAddr:    "127.0.0.1:9999",
			expectError:     true,
			expectOnSuccess: 0,
			expectOnFail:    0,
			expectOnSuspend: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cbMock.Reset()

			targetAddr := cbAddr
			if tt.overrideAddr != "" || tt.name == "EmptyCallbackAddress_SkippedWithNoError" {
				targetAddr = tt.overrideAddr
			}

			evt := &events.WebhookEvent{
				Request: &pb.ExecuteWorkflowNodeRequest{
					Identity: &pb.InfrastructureIdentity{
						TenantId: "tenant_test",
						AppId:    "app_test",
						JobId:    tt.jobID,
					},
					StepId:          tt.stepID,
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
			}

			assert.Equal(t, tt.expectOnSuccess, cbMock.onSuccessCount, "OnNodeExecutionComplete call count mismatch")
			assert.Equal(t, tt.expectOnFail, cbMock.onFailCount, "OnJobFailed call count mismatch")
			assert.Equal(t, tt.expectOnSuspend, cbMock.onSuspendCount, "OnJobSuspended call count mismatch")

			if tt.expectOnSuccess > 0 {
				reqs := cbMock.GetSuccessRequests()
				assert.Len(t, reqs, tt.expectOnSuccess)
				if tt.validatePayload != nil {
					tt.validatePayload(t, reqs[0])
				}
			}
			if tt.expectOnFail > 0 {
				reqs := cbMock.GetFailRequests()
				assert.Len(t, reqs, tt.expectOnFail)
				if tt.validatePayload != nil {
					tt.validatePayload(t, reqs[0])
				}
			}
			if tt.expectOnSuspend > 0 {
				reqs := cbMock.GetSuspendRequests()
				assert.Len(t, reqs, tt.expectOnSuspend)
				if tt.validatePayload != nil {
					tt.validatePayload(t, reqs[0])
				}
			}
		})
	}
}

func TestServer_ProcessWebhookMsg_DirectClientPoolRouting_MultiServer(t *testing.T) {
	// Verifies that gRPC client pool dials req.CallbackAddress directly without graph resolution
	mockServerA, addrA, cleanupA := setupMockCallbackServer(t)
	defer cleanupA()

	mockServerB, addrB, cleanupB := setupMockCallbackServer(t)
	defer cleanupB()

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:                  pub,
		webhookConns:         make(map[string]*grpc.ClientConn),
		grpcTimeoutSecs:      30,
		grpcMaxMessageSizeMb: 4,
	}

	// Message 1 -> Server A
	evt1 := &events.WebhookEvent{
		Request: &pb.ExecuteWorkflowNodeRequest{
			Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "job_server_a"},
			StepId:          "step_a",
			CallbackAddress: addrA,
		},
	}
	evt1Bytes, _ := json.Marshal(evt1)
	err1 := srv.processWebhookMsg(context.Background(), mockMsg{data: evt1Bytes})
	assert.NoError(t, err1)

	// Message 2 -> Server B
	evt2 := &events.WebhookEvent{
		Request: &pb.ExecuteWorkflowNodeRequest{
			Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "job_server_b"},
			StepId:          "step_b",
			CallbackAddress: addrB,
		},
	}
	evt2Bytes, _ := json.Marshal(evt2)
	err2 := srv.processWebhookMsg(context.Background(), mockMsg{data: evt2Bytes})
	assert.NoError(t, err2)

	// Assertions for strict routing isolation
	assert.Equal(t, 1, mockServerA.onSuccessCount, "Server A should receive exactly 1 call")
	assert.Equal(t, 0, mockServerA.onFailCount)
	assert.Equal(t, "job_server_a", mockServerA.GetSuccessRequests()[0].Identity.JobId)

	assert.Equal(t, 1, mockServerB.onSuccessCount, "Server B should receive exactly 1 call")
	assert.Equal(t, 0, mockServerB.onFailCount)
	assert.Equal(t, "job_server_b", mockServerB.GetSuccessRequests()[0].Identity.JobId)

	// Verify gRPC connection pool caching
	srv.webhookConnMu.RLock()
	defer srv.webhookConnMu.RUnlock()
	assert.Contains(t, srv.webhookConns, addrA, "Client pool must cache connection to Server A")
	assert.Contains(t, srv.webhookConns, addrB, "Client pool must cache connection to Server B")
}
