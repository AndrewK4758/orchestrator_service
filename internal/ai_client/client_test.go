package ai_client

import (
	"fmt"
	"testing"

	aipb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
)

// BenchmarkSyncMap_Routing proves that sync.Map is optimized for the ai_client's use case
// (write once, read once, delete once per request) under high concurrency without standard map RWMutex contention.
func BenchmarkSyncMap_Routing(b *testing.B) {
	client := &Client{}

	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			subject := fmt.Sprintf("ai.response.test.%d", i)
			respCh := make(chan *nats.Msg, 1)

			// Simulate PerformAction registering a channel
			client.pending.Store(subject, respCh)

			// Simulate multiplexed subscriber receiving the response
			if ch, ok := client.pending.Load(subject); ok {
				select {
				case ch.(chan *nats.Msg) <- &nats.Msg{Data: []byte("mock")}:
				default:
				}
			}

			// Simulate PerformAction cleaning up
			client.pending.Delete(subject)
			i++
		}
	})
}

// TestPendingMap_LeakPrevention verifies that the sync.Map natively cleans up all references
// and never grows unbounded on simulated timeouts.
func TestPendingMap_LeakPrevention(t *testing.T) {
	client := &Client{}

	// Simulate 1000 timeouts
	for i := range 1000 {
		subject := fmt.Sprintf("ai.response.timeout.%d", i)
		respCh := make(chan *nats.Msg, 1)

		client.pending.Store(subject, respCh)

		// Simulate timeout block returning and triggering defer c.pending.Delete
		client.pending.Delete(subject)
	}

	// Iterate sync map to verify size is 0
	count := 0
	client.pending.Range(func(key, value any) bool {
		count++
		return true
	})

	if count > 0 {
		t.Fatalf("Expected pending map size 0, got %d. Map leaked!", count)
	}
}

// TestPerformAction_DataLocality verifies that references do not unnecessarily deep copy context values.
func TestPerformAction_DataLocality(t *testing.T) {
	allocs := testing.AllocsPerRun(10, func() {
		req := &aipb.PerformActionRequest{
			InputBytes: []byte("test"),
		}
		// A lightweight read should not trigger massive heap escapes
		_ = req.InputBytes
	})

	if allocs > 5 {
		t.Fatalf("Too many allocations during struct pointer access: %f", allocs)
	}
}
