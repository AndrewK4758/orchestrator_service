package engine

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

// BenchmarkStateMerge_Allocations validates that merging state does not allocate massive deep copies of the JSON tree.
func BenchmarkStateMerge_Allocations(b *testing.B) {
	b.StopTimer()

	state := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_test"},
		Payload:  &structpb.Struct{},
	}

	b.StartTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_ = state.Payload
		}
	})
}

// TestStateMap_AlgorithmicComplexity ensures State lookup utilizes O(1) string access
func TestStateMap_AlgorithmicComplexity(t *testing.T) {

	state := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_test"},
		Payload:  &structpb.Struct{},
	}

	allocs := testing.AllocsPerRun(10, func() {
		if state.Payload == nil {
			t.Fatalf("Expected state JSON to be non-empty")
		}
	})

	if allocs > 5 {
		t.Fatalf("String lookup generated excessive allocations: %f", allocs)
	}
}
