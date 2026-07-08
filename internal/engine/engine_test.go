package engine

import (
	pb "github.com/AndrewK4758/shared_protos"
	"testing"
)

func TestProcessingState_Initialization(t *testing.T) {
	ps := &pb.WorkflowContext{
		JobId: "test_job_1",
		Metadata: map[string]string{
			"tenant_id": "test_tenant",
		},
	}

	if ps.JobId != "test_job_1" {
		t.Errorf("Expected jobID test_job_1, got %s", ps.JobId)
	}

	if ps.Metadata["tenant_id"] != "test_tenant" {
		t.Errorf("Expected metadata tenant_id, got %s", ps.Metadata["tenant_id"])
	}
}
