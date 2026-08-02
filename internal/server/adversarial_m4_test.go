package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func init() {
	os.Setenv("PUBLISH_TIMEOUT_SECONDS", "5")
	os.Setenv("SEMANTIC_CACHE_SERVICE_ADDRESS", "localhost:11115")
	os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", "4")
	os.Setenv("CACHE_TIMEOUT_SECONDS", "5")
	os.Setenv("GRPC_TIMEOUT_SECONDS", "30")
	os.Setenv("NATS_TIMEOUT_SECONDS", "30")
	os.Setenv("WEBHOOKS_MAX_CONCURRENCY", "10")
}

// TestAdversarial_Orchestrator_RoutingAgnosticism_ExtremeInputs verifies Orchestrator routes requests with extreme StorageUri and generic metadata without inspecting or corrupting them.
func TestAdversarial_Orchestrator_RoutingAgnosticism_ExtremeInputs(t *testing.T) {
	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:                  pub,
		webhookConns:         make(map[string]*grpc.ClientConn),
		grpcTimeoutSecs:      30,
		grpcMaxMessageSizeMb: 4,
	}

	extremeURIs := []string{
		"internal-store://tenant-1/app-2/job-3/deeply/nested/dir/document.pdf?query=val&special=$#@!%20space",
		"internal-store://tenant-X/app-Y/job-Z/file_with_unicode_ñ_ä_ö_中文.docx",
		"internal-store://tenant-A/app-B/job-C/zero_byte_doc.txt",
		"internal-store://tenant-100/app-200/job-300/" + string(make([]byte, 1000)), // Very long URI
	}

	for i, uri := range extremeURIs {
		t.Run(fmt.Sprintf("Extreme_StorageUri_%d", i), func(t *testing.T) {
			funcStruct, _ := structpb.NewStruct(map[string]interface{}{
				"ClaimCheckUri": uri,
				"ArbitraryData": "some_value",
			})

			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: fmt.Sprintf("tenant_%d", i),
					AppId:    fmt.Sprintf("app_%d", i),
					JobId:    fmt.Sprintf("job_extreme_%d", i),
				},
				StepId:          fmt.Sprintf("step_extreme_%d", i),
				CallbackAddress: "127.0.0.1:0",
				GlobalState:     &pb.GlobalState{Functional: funcStruct},
				Storage: &pb.StorageContract{
					DocumentUri: uri,
				},
				NodeConfig: &pb.NodeConfiguration{
					NodeName:            "Agnostic Test Node",
					TargetWorkerSubject: "worker.ai.inference",
				},
			}

			res, err := srv.ExecuteWorkflowNode(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.True(t, res.Success)

			// Assert pub.PublishJob received verbatim request
			pub.mu.Lock()
			lastMsg := pub.publishedMsgs[len(pub.publishedMsgs)-1]
			pub.mu.Unlock()

			unmarshaled := &pb.ExecuteWorkflowNodeRequest{}
			err = proto.Unmarshal(lastMsg, unmarshaled)
			assert.NoError(t, err)
			assert.Equal(t, uri, unmarshaled.Storage.DocumentUri)
			assert.Equal(t, uri, unmarshaled.GlobalState.Functional.Fields["ClaimCheckUri"].GetStringValue())
		})
	}
}

// TestAdversarial_Orchestrator_ProcessWebhookMsg_ConcurrentStress stress tests concurrent webhook callbacks and gRPC connection pooling.
func TestAdversarial_Orchestrator_ProcessWebhookMsg_ConcurrentStress(t *testing.T) {
	// Setup 3 independent mock callback servers
	serverCount := 3
	mocks := make([]*mockCallbackServer, serverCount)
	addrs := make([]string, serverCount)

	for i := 0; i < serverCount; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NoError(t, err)

		grpcServer := grpc.NewServer()
		cbMock := &mockCallbackServer{}
		pb.RegisterOrchestratorCallbackServiceServer(grpcServer, cbMock)
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		defer grpcServer.Stop()

		mocks[i] = cbMock
		addrs[i] = lis.Addr().String()
	}

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:                  pub,
		webhookConns:         make(map[string]*grpc.ClientConn),
		grpcTimeoutSecs:      30,
		grpcMaxMessageSizeMb: 4,
	}

	concurrency := 60
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			targetServerIdx := id % serverCount
			targetAddr := addrs[targetServerIdx]

			evt := &events.WebhookEvent{
				Request: &pb.ExecuteWorkflowNodeRequest{
					Identity: &pb.InfrastructureIdentity{
						TenantId: "tenant_stress",
						AppId:    "app_stress",
						JobId:    fmt.Sprintf("job_stress_%d", id),
					},
					StepId:          fmt.Sprintf("step_stress_%d", id),
					CallbackAddress: targetAddr,
				},
			}

			evtBytes, err := json.Marshal(evt)
			assert.NoError(t, err)

			msg := mockMsg{data: evtBytes}
			err = srv.processWebhookMsg(context.Background(), msg)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify all 60 messages were delivered across the 3 servers cleanly
	totalDelivered := 0
	for i := 0; i < serverCount; i++ {
		mocks[i].mu.Lock()
		count := mocks[i].onSuccessCount
		mocks[i].mu.Unlock()
		totalDelivered += count
	}

	assert.Equal(t, concurrency, totalDelivered, "All concurrent webhooks should be successfully delivered")

	// Verify connection pool contains all 3 servers
	srv.webhookConnMu.RLock()
	defer srv.webhookConnMu.RUnlock()
	assert.Len(t, srv.webhookConns, serverCount)
}
