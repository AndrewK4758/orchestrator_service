package engine

import (
	pb "github.com/AndrewK4758/shared_protos"
	"testing"
)

func TestProcessingState_Initialization(t *testing.T) {
	req := &pb.ExecuteWorkflowNodeRequest{
		StepId: "step-1",
		Identity: &pb.InfrastructureIdentity{
			TenantId: "test_tenant",
			AppId:    "test_app",
			JobId:    "test_job_1",
		},
	}

	if req.Identity.JobId != "test_job_1" {
		t.Errorf("Expected jobID test_job_1, got %s", req.Identity.JobId)
	}

	if req.Identity.TenantId != "test_tenant" {
		t.Errorf("Expected tenant_id test_tenant, got %s", req.Identity.TenantId)
	}
}
