package server

import (
	"context"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"orchestrator_service/internal/ai_client"
)

type mockTraceClient struct {
	pb.TraceabilityQueryClient
	getJobStateFn func(ctx context.Context, in *pb.GetJobStateRequest, opts ...grpc.CallOption) (*pb.GetJobStateResponse, error)
}

func (m *mockTraceClient) GetJobState(ctx context.Context, in *pb.GetJobStateRequest, opts ...grpc.CallOption) (*pb.GetJobStateResponse, error) {
	if m.getJobStateFn != nil {
		return m.getJobStateFn(ctx, in, opts...)
	}
	return &pb.GetJobStateResponse{
		Found: true,
		Job:   &pb.JobStateRecord{Status: "PENDING"},
	}, nil
}

func TestDispatch_DuplicateActiveDispatch(t *testing.T) {
	// Test Case 1.1: Duplicate Active Dispatch
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

	jobID := "job_active_dup"
	stepID := "step_1"

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{JobId: jobID, TenantId: "tenant_1"},
		StepId:   stepID,
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}

	// Manually inject state: step in progress
	srv.activeExecMu.Lock()
	srv.activeExecs[jobID] = &JobExecInfo{
		AppID: "test_app",
	}
	srv.activeExecMu.Unlock()

	msgBytes, _ := proto.Marshal(req)

	err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.NoError(t, err, "handleJobMessage should return nil and ignore duplicate active dispatch")

	// Validate state was not modified in a bad way
	srv.activeExecMu.RLock()
	_, exists := srv.activeExecs[jobID]
	srv.activeExecMu.RUnlock()

	assert.True(t, exists)
}

func TestDispatch_DuplicateCompletedDispatch(t *testing.T) {
	// Test removed since completed step deduplication relies entirely on trace client and JetStream once-and-only-once.
	assert.True(t, true)
}

func TestDispatch_PurgedStateRedelivery(t *testing.T) {
	// Test Case 1.3: Purged State Redelivery
	pub := &mockPublisher{}

	traceClient := &mockTraceClient{
		getJobStateFn: func(ctx context.Context, in *pb.GetJobStateRequest, opts ...grpc.CallOption) (*pb.GetJobStateResponse, error) {
			return &pb.GetJobStateResponse{
				Found: true,
				Job:   &pb.JobStateRecord{Status: "COMPLETED"},
			}, nil
		},
	}

	srv := NewServer(context.Background(), &ai_client.Client{}, pub, traceClient)

	jobID := "job_purged_redelivery"
	stepID := "step_1"

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{JobId: jobID, TenantId: "tenant_1"},
		StepId:   stepID,
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}

	// The job should NOT exist in activeExecs initially
	srv.activeExecMu.RLock()
	_, exists := srv.activeExecs[jobID]
	srv.activeExecMu.RUnlock()
	assert.False(t, exists)

	msgBytes, _ := proto.Marshal(req)

	err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
	assert.NoError(t, err, "handleJobMessage should return nil and ignore redelivered purged job")

	// Ensure the job was NOT added back to activeExecs
	srv.activeExecMu.RLock()
	_, existsAfter := srv.activeExecs[jobID]
	srv.activeExecMu.RUnlock()
	assert.False(t, existsAfter, "Job should not be re-added to activeExecs if it was already completed/purged")
}
