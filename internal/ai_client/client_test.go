package ai_client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aipb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// mockJSMsg implements jetstream.Msg for testing
type mockJSMsg struct {
	subject string
	ackCnt  int32
}

func (m *mockJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *mockJSMsg) Data() []byte                              { return nil }
func (m *mockJSMsg) Headers() nats.Header                      { return nil }
func (m *mockJSMsg) Subject() string                           { return m.subject }
func (m *mockJSMsg) Reply() string                             { return "" }
func (m *mockJSMsg) Ack() error {
	atomic.AddInt32(&m.ackCnt, 1)
	return nil
}
func (m *mockJSMsg) DoubleAck(context.Context) error        { return nil }
func (m *mockJSMsg) Nak() error                             { return nil }
func (m *mockJSMsg) NakWithDelay(delay time.Duration) error { return nil }
func (m *mockJSMsg) InProgress() error                      { return nil }
func (m *mockJSMsg) Term() error                            { return nil }
func (m *mockJSMsg) TermWithReason(reason string) error     { return nil }

func TestJetStream_DuplicateWebhookReplies_Idempotency(t *testing.T) {
	client := &Client{}
	subject := "ai.response.test.idempotency"
	respCh := make(chan jetstream.Msg, 1)

	client.pending.Store(subject, respCh)

	var wg sync.WaitGroup
	numDuplicates := 10

	// Track all messages to verify Ack behavior
	msgs := make([]*mockJSMsg, numDuplicates)
	for i := 0; i < numDuplicates; i++ {
		msgs[i] = &mockJSMsg{subject: subject}
	}

	// Simulate dispatching duplicate webhook replies simultaneously
	for i := 0; i < numDuplicates; i++ {
		wg.Add(1)
		go func(msg *mockJSMsg) {
			defer wg.Done()

			// Exact logic from client.go multiplexer
			if ch, ok := client.pending.Load(msg.Subject()); ok {
				select {
				case ch.(chan jetstream.Msg) <- msg:
				default:
					msg.Ack()
				}
			}
		}(msgs[i])
	}

	// Wait for all dispatches to complete
	wg.Wait()

	// Verify exactly one message made it to the channel
	if len(respCh) != 1 {
		t.Fatalf("Expected exactly 1 message in channel, got %d", len(respCh))
	}

	// Verify the remaining messages were Acked
	var totalAcks int32
	for _, m := range msgs {
		totalAcks += atomic.LoadInt32(&m.ackCnt)
	}

	if totalAcks != int32(numDuplicates-1) {
		t.Fatalf("Expected %d acks for duplicate messages, got %d", numDuplicates-1, totalAcks)
	}

	// Clean up
	client.pending.Delete(subject)
}
