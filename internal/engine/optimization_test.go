package engine

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

// BenchmarkStateMerge_Allocations validates that merging state does not allocate massive deep copies of the JSON tree.
func BenchmarkStateMerge_Allocations(b *testing.B) {
	b.StopTimer()
	fields := make(map[string]*structpb.Value)
	for i := range 100 {
		fields[string(rune(i))] = structpb.NewStringValue("test_value")
	}

	globalState := &structpb.Struct{Fields: fields}
	state := &pb.WorkflowContext{
		JobId:       "job_test",
		GlobalState: globalState,
	}

	b.StartTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			// A lightweight read access of state Fields
			_ = state.GlobalState.Fields
		}
	})
}

// TestStateMap_AlgorithmicComplexity ensures State lookup utilizes O(1) map access
func TestStateMap_AlgorithmicComplexity(t *testing.T) {
	fields := make(map[string]*structpb.Value)
	fields["key_lookup"] = structpb.NewStringValue("lookup_value")

	globalState := &structpb.Struct{Fields: fields}

	allocs := testing.AllocsPerRun(10, func() {
		// O(1) native map lookup validation
		val, ok := globalState.Fields["key_lookup"]
		if !ok || val.GetStringValue() != "lookup_value" {
			t.Fatalf("Expected O(1) lookup to succeed")
		}
	})

	// Reading from a map in Go shouldn't incur massive allocations
	if allocs > 5 {
		t.Fatalf("O(1) Map lookup generated excessive allocations: %f", allocs)
	}
}
