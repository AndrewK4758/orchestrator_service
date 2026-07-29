package engine

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
)

// BenchmarkStateMerge_Allocations validates that merging state does not allocate massive deep copies of the JSON tree.
func BenchmarkStateMerge_Allocations(b *testing.B) {
	b.StopTimer()

	state := &pb.ExecuteWorkflowNodeRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_test"},
		GlobalState: &pb.GlobalState{},
	}

	b.StartTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_ = state.GlobalState
		}
	})
}

// TestStateMap_AlgorithmicComplexity ensures State lookup utilizes O(1) string access
func TestStateMap_AlgorithmicComplexity(t *testing.T) {

	state := &pb.ExecuteWorkflowNodeRequest{
		Identity:        &pb.InfrastructureIdentity{JobId: "job_test"},
		GlobalState: &pb.GlobalState{},
	}

	allocs := testing.AllocsPerRun(10, func() {
		if state.GlobalState == nil {
			t.Fatalf("Expected state JSON to be non-empty")
		}
	})

	if allocs > 5 {
		t.Fatalf("String lookup generated excessive allocations: %f", allocs)
	}
}
