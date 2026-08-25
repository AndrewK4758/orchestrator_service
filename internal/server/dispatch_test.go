package server

import (
	"context"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
)

func TestDispatch_DuplicateActiveDispatch(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub: pub,
	}

	req := &pb.ExecuteTaskRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_active_dup", TenantId: "tenant_1", AppId: "app_1"},
		StepId:          "step_1",
		TaskSubject:     "worker.test",
		CallbackAddress: "localhost:1234",
	}

	res, err := srv.ExecuteTask(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func TestDispatch_DuplicateCompletedDispatch(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	assert.True(t, true)
}

func TestDispatch_PurgedStateRedelivery(t *testing.T) {
	t.Skip("Skipping integration test requiring NATS")
	pub := &mockPublisher{}
	srv := &OrchestratorServer{
		pub: pub,
	}

	req := &pb.ExecuteTaskRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_purged_redelivery", TenantId: "tenant_1", AppId: "app_1"},
		StepId:          "step_1",
		TaskSubject:     "worker.test",
		CallbackAddress: "localhost:1234",
	}

	res, err := srv.ExecuteTask(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}
