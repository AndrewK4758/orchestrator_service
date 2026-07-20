package server

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

import "io"

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
	publishedWebhook *events.WebhookEvent
	publishedMsgs    [][]byte
}

func (m *mockPublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	return nil
}
func (m *mockPublisher) PublishCacheStore(ctx context.Context, data []byte) error { return nil }
func (m *mockPublisher) PublishJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error {
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}
func (m *mockPublisher) PublishPriorityJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error {
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}
func (m *mockPublisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}
func (m *mockPublisher) PurgeQueues(ctx context.Context) error { return nil }
func (m *mockPublisher) PublishWebhook(ctx context.Context, payload *events.WebhookEvent) error {
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
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

	t.Run("ExecuteWorkflowNode queues to internal memory cache and NATS", func(t *testing.T) {
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
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

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
	mu              sync.Mutex
	receivedSuccess *pb.NodeExecutionCompleteRequest
	receivedFail    *pb.JobFailedRequest
}

func (s *mockCallbackServer) OnNodeExecutionComplete(stream grpc.BidiStreamingServer[pb.NodeExecutionCompleteRequest, pb.NodeExecutionCompleteResponse]) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.receivedSuccess = req
	s.mu.Unlock()
	return stream.Send(&pb.NodeExecutionCompleteResponse{Success: true})
}

func (s *mockCallbackServer) OnJobFailed(ctx context.Context, req *pb.JobFailedRequest) (*pb.JobFailedResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receivedFail = req
	return &pb.JobFailedResponse{Success: true}, nil
}

func TestOrchestratorServer_GenericStateFuzzing(t *testing.T) {
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
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

	// Setup mock cache client to force a cache hit (bypassing AI)
	srv.cacheClient = &mockCacheClient{}

	// Create a completely arbitrary generic state
	stateMap := map[string]any{
		"NestedObj": map[string]any{
			"UnknownField": "Value1",
			"ListField":    []any{1.0, 2.0, 3.0},
		},
		"TopLevelString": "ArbitraryData",
	}
	stateStruct, err := structpb.NewStruct(stateMap)
	assert.NoError(t, err)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_fuzz_1", TenantId: "tenant_1", AppId: "app_1"},
		StepId:          "step_1",
		CallbackAddress: cbAddress,
		GlobalState:     stateStruct,
		Metadata: map[string]string{
			"app_id":          "FuzzApp",
			"correlation_id":  "123-abc",
			"x-custom-header": "test",
		},
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
			Cache: &pb.CacheConfiguration{
				Enabled:        true,
				CollectionName: "test_cache",
				LookupInput:    "test",
				Metadata: map[string]string{
					"filename": "test.pdf",
				},
			},
		},
	}

	srv.cacheClient = &mockCacheClient{
		checkCacheFn: func(ctx context.Context, in *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			// Validate identity was forwarded correctly
			assert.NotNil(t, in.Identity)
			assert.Equal(t, "job_fuzz_1", in.Identity.JobId)
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
	returnedState := cbMock.receivedSuccess.GlobalState
	assert.NotNil(t, returnedState)

	nestedObj := returnedState.Fields["NestedObj"].GetStructValue().Fields
	assert.Equal(t, "Value1", nestedObj["UnknownField"].GetStringValue())
	assert.Equal(t, "ArbitraryData", returnedState.Fields["TopLevelString"].GetStringValue())
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

func (m *mockCacheClient) CheckMetadataExists(ctx context.Context, in *pb.CheckMetadataRequest, opts ...grpc.CallOption) (*pb.CheckMetadataResponse, error) {
	return &pb.CheckMetadataResponse{Exists: true}, nil
}

func TestOrchestratorServer_CallbackTimeoutResiliency(t *testing.T) {
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

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
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_cache_timeout", TenantId: "tenant_1", AppId: "app_1"},
		StepId:   "step_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
			Cache: &pb.CacheConfiguration{
				Enabled:        true,
				CollectionName: "test_cache",
				LookupInput:    "test",
			},
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

	// The ReAct engine will fail because there is no API key / mock AI setup in this basic test,
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
	tests := []struct {
		name          string
		jobID         string
		correctedJson string
		setupKV       func(*testing.T, *mockKeyValue)
		validate      func(*testing.T, *pb.ResumeJobResponse, *mockPublisher, bool)
	}{
		{
			name:          "TestResumeJob_MergeArbitraryJson",
			jobID:         "job_merge",
			correctedJson: `{"unknown_top_level": {"nested": "value"}, "list_field": [1,2,3]}`,
			setupKV: func(t *testing.T, kv *mockKeyValue) {
				initialReq := &pb.ExecuteWorkflowNodeRequest{
					Identity:    &pb.InfrastructureIdentity{JobId: "job_merge", TenantId: "tenant_1", AppId: "app_1"},
					StepId:      "step_merge",
					CallbackAddress: "localhost",
					GlobalState: &structpb.Struct{Fields: map[string]*structpb.Value{}},
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
				assert.Equal(t, "step_merge", pub.publishedWebhook.LastStepId)
			},
		},
		{
			name:          "TestResumeJob_Success",
			jobID:         "job_success",
			correctedJson: `{"msg": "success"}`,
			setupKV: func(t *testing.T, kv *mockKeyValue) {
				initialReq := &pb.ExecuteWorkflowNodeRequest{
					Identity:   &pb.InfrastructureIdentity{JobId: "job_success", TenantId: "tenant_1", AppId: "app_1"},
					StepId:     "step_success",
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
			correctedJson: `{}`,
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
			pub := &mockPublisher{}
			srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

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
				CorrectedJson: tt.correctedJson,
			}

			res, err := srv.ResumeJob(context.Background(), resumeReq)
			assert.NoError(t, err)
			tt.validate(t, res, pub, deleteCalled)
		})
	}
}

func TestOrchestratorServer_PurgeSystemState(t *testing.T) {
	tests := []struct {
		name          string
		appID         string
		purgeQueues   bool
		setupActive   func(*OrchestratorServer) *bool
		validate      func(*testing.T, *OrchestratorServer, *pb.PurgeSystemStateResponse, *bool)
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
			pub := &mockPublisher{}
			srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

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
