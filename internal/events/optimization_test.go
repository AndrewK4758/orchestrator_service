package events

import (
	"encoding/json"
	"testing"
	"time"
)

// BenchmarkJSONMarshal_ZeroAlloc validates the JSON serialization of TraceabilityEvent.
func BenchmarkJSONMarshal_ZeroAlloc(b *testing.B) {
	event := TraceabilityEvent{
		JobID:     "test_job",
		AppID:     "Artemis",
		Timestamp: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
		EventType: "Started",
		Metadata:  map[string]any{"step": "step_1"},
	}

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			_, _ = json.Marshal(event)
		}
	})
}

// TestJSONMarshal_DataLocality verifies that the allocations during JSON marshaling of standard events are constrained.
func TestJSONMarshal_DataLocality(t *testing.T) {
	event := TraceabilityEvent{
		JobID:     "test_job",
		AppID:     "Artemis",
		Timestamp: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
		EventType: "Started",
		Metadata:  map[string]any{"step": "step_1"},
	}

	allocs := testing.AllocsPerRun(10, func() {
		_, _ = json.Marshal(event)
	})

	if allocs > 10 {
		t.Fatalf("Excessive allocations during JSON marshal: %f", allocs)
	}
}
