package engine

import (
	"testing"
)

func TestProcessingState_Initialization(t *testing.T) {
	ps := &ProcessingState{
		JobID: "test_job_1",
		Metadata: map[string]string{
			"tenant_id": "test_tenant",
		},
	}

	if ps.JobID != "test_job_1" {
		t.Errorf("Expected jobID test_job_1, got %s", ps.JobID)
	}

	if ps.Metadata["tenant_id"] != "test_tenant" {
		t.Errorf("Expected metadata tenant_id, got %s", ps.Metadata["tenant_id"])
	}
}
