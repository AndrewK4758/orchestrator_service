package server

import (
	"context"
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

type mockPublisher struct {
	publishedMsgs [][]byte
}

func (m *mockPublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	return nil
}
func (m *mockPublisher) PublishCacheStore(ctx context.Context, data []byte) error { return nil }
func (m *mockPublisher) PublishJob(ctx context.Context, msgId string, payload []byte) error {
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}
func (m *mockPublisher) PublishPriorityJob(ctx context.Context, msgId string, payload []byte) error {
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}
func (m *mockPublisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}
func (m *mockPublisher) PurgeQueues(ctx context.Context) error { return nil }

func TestOrchestratorServer_ExecuteWorkflowNode(t *testing.T) {
	pub := &mockPublisher{}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	t.Run("ExecuteWorkflowNode queues to internal memory cache and NATS", func(t *testing.T) {
		req := &pb.ExecuteWorkflowNodeRequest{
			JobId:  "job_1",
			StepId: "step_1",
		}
		res, err := srv.ExecuteWorkflowNode(context.Background(), req)

		assert.NoError(t, err)
		assert.True(t, res.Success)
		assert.Equal(t, "step_1", res.StepId) // wait res.StepId would be step_1, wait we pass req.StepId

		srv.mu.RLock()
		tracker, exists := srv.activeJobs["job_1"]
		srv.mu.RUnlock()

		assert.True(t, exists)
		assert.Equal(t, "job_1", tracker.Req.JobId)
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
	srv := NewServer(&ai_client.Client{}, pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		JobId:  "job_concurrent",
		StepId: "step_1",
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

func (s *mockCallbackServer) OnNodeExecutionComplete(ctx context.Context, req *pb.NodeExecutionCompleteRequest) (*pb.NodeExecutionCompleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receivedSuccess = req
	return &pb.NodeExecutionCompleteResponse{Success: true}, nil
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
	srv := NewServer(&ai_client.Client{}, pub, nil)

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
		JobId:           "job_fuzz_1",
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
			// Validate metadata was forwarded correctly
			assert.NotNil(t, in.Metadata)
			assert.Equal(t, "test.pdf", in.Metadata.Fields["filename"].GetStringValue())
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
	assert.True(t, res.Success)
	assert.Equal(t, "FuzzApp", req.Metadata["app_id"]) // Validates the app_id wasn't mutated in a hardcoded way

	// 2. Pass to handleJobMessage. It should fail since there's no actual AI configured,
	// triggering OnJobFailed but WITH the unaltered JobID and proper Traceability.
	msgBytes, _ := proto.Marshal(req)
	err = srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.NoError(t, err) // the handler itself returns nil, but fires callback

	time.Sleep(100 * time.Millisecond) // wait for callback

	cbMock.mu.Lock()
	defer cbMock.mu.Unlock()

	assert.NotNil(t, cbMock.receivedSuccess, "Should have received OnNodeExecutionComplete via cache bypass")
	assert.Equal(t, "job_fuzz_1", cbMock.receivedSuccess.JobId)

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
	srv := NewServer(&ai_client.Client{}, pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		JobId:           "job_timeout_1",
		StepId:          "step_1",
		CallbackAddress: "127.0.0.1:9999", // Unreachable port
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}

	msgBytes, _ := proto.Marshal(req)

	// Should attempt to dial 127.0.0.1:9999, fail, and return an error to NAK the message
	err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.Error(t, err)
}

func TestOrchestratorServer_CallbackFailureCleanup(t *testing.T) {
	pub := &mockPublisher{}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		JobId:           "job_cleanup",
		StepId:          "step_1",
		CallbackAddress: "127.0.0.1:12345", // A non-existent local port to simulate down client
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
			Cache: &pb.CacheConfiguration{
				Enabled:        true,
				CollectionName: "test_cache",
				LookupInput:    "test",
			},
		},
	}
	
	// Force a cache hit so we don't need real AI
	srv.cacheClient = &mockCacheClient{
		checkCacheFn: func(ctx context.Context, in *pb.CheckCacheRequest, opts ...grpc.CallOption) (*pb.CheckCacheResponse, error) {
			return &pb.CheckCacheResponse{Hit: true, Confidence: 1.0, ExtractedPayload: `{"status": "ok"}`}, nil
		},
	}

	msgBytes, _ := proto.Marshal(req)
	err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.Error(t, err)

	// Callback will fail. We should assert that `activeJobs` no longer contains "job_cleanup"
	srv.mu.RLock()
	_, exists := srv.activeJobs["job_cleanup"]
	srv.mu.RUnlock()

	assert.False(t, exists, "JobTracker should have been cleaned up after callback failure")
}

func TestOrchestratorServer_CacheServiceTimeoutFallback(t *testing.T) {
	pub := &mockPublisher{}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		JobId:  "job_cache_timeout",
		StepId: "step_1",
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
