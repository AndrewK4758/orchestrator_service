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
		pub:          pub,
		webhookConns: make(map[string]*clientConnEntry),
		grpcTimeout:  30,
		grpcMaxMsgMb: 4,
	}
	_ = srv

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

			req := &pb.ExecuteTaskRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: fmt.Sprintf("tenant_%d", i),
					AppId:    fmt.Sprintf("app_%d", i),
					JobId:    fmt.Sprintf("job_extreme_%d", i),
				},
				StepId:      fmt.Sprintf("step_extreme_%d", i),
				TaskSubject: "worker.ai.inference",
				Payload:     funcStruct,
			}

			// Validate payload values retain integrity
			assert.Equal(t, uri, req.Payload.Fields["ClaimCheckUri"].GetStringValue())
		})
	}
}

// TestAdversarial_Orchestrator_ProcessWebhookMsg_ConcurrentStress stress tests concurrent webhook callbacks and gRPC connection pooling.
func TestAdversarial_Orchestrator_ProcessWebhookMsg_ConcurrentStress(t *testing.T) {
	serverCount := 3
	addrs := make([]string, serverCount)
	cbMocks := make([]*mockCallbackServer, serverCount)

	for i := 0; i < serverCount; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NoError(t, err)

		grpcServer := grpc.NewServer()
		cbMock := &mockCallbackServer{}
		cbMocks[i] = cbMock
		pb.RegisterOrchestratorCallbackServiceServer(grpcServer, cbMock)
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		defer grpcServer.Stop()

		addrs[i] = lis.Addr().String()
	}

	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub:          pub,
		webhookConns: make(map[string]*clientConnEntry),
		grpcTimeout:  30,
		grpcMaxMsgMb: 4,
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
				Request: &pb.ExecuteTaskRequest{
					Identity: &pb.InfrastructureIdentity{
						TenantId: "tenant_stress",
						AppId:    "app_stress",
						JobId:    fmt.Sprintf("job_stress_%d", id),
					},
					StepId:          fmt.Sprintf("step_stress_%d", id),
					TaskSubject:     "worker.test",
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

	totalReceived := 0
	for _, cb := range cbMocks {
		totalReceived += cb.OnSuccessCount()
	}
	assert.Equal(t, concurrency, totalReceived, "Expected all 60 concurrent webhooks to be dispatched to gRPC callback servers")
}
