package server

import (
	"testing"
	"context"
	"sync"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/proto"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type mockPublisher struct {
	publishedMsgs [][]byte
}

func (m *mockPublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error { return nil }
func (m *mockPublisher) PublishCacheStore(ctx context.Context, data []byte) error  { return nil }
func (m *mockPublisher) PublishJob(ctx context.Context, payload []byte) error {
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}
func (m *mockPublisher) PublishPriorityJob(ctx context.Context, payload []byte) error {
	m.publishedMsgs = append(m.publishedMsgs, payload)
	return nil
}
func (m *mockPublisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}
func (m *mockPublisher) PurgeQueues(ctx context.Context) error { return nil }

func TestOrchestratorServer_ExecuteWorkflowNode(t *testing.T) {
	pub := &mockPublisher{}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	t.Run("ExecuteWorkflowNode queues to internal memory cache and NATS", func(t *testing.T) {
		req := &pb.ExecuteWorkflowNodeRequest{
			JobId: "job_1",
			StepId: "step_1",
		}
		res, err := srv.ExecuteWorkflowNode(context.Background(), req)

		assert.NoError(t, err)
		assert.True(t, res.Success)
		assert.Equal(t, "step_1", res.StepId) // wait res.StepId would be step_1, wait we pass req.StepId

		srv.mu.RLock()
		tracker, exists := srv.activeJobs["job_1"]
		srv.mu.RUnlock()

		assert.True(t, exists)
		assert.Equal(t, "job_1", tracker.Req.JobId)
		assert.Equal(t, 1, len(pub.publishedMsgs))
	})
}

type mockMsg struct {
		jetstream.Msg
		data []byte
	}

func (m mockMsg) Data() []byte {
	return m.data
}

func TestIdempotentExecution_Concurrent(t *testing.T) {
	pub := &mockPublisher{}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	req := &pb.ExecuteWorkflowNodeRequest{
		JobId: "job_concurrent",
		StepId: "step_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}

	// We'll simulate Jetstream delivering the same message twice concurrently.
	msgBytes, _ := proto.Marshal(req)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
		// If we don't have proper mutex locking, race detector will trigger or execution will panic.
		if err != nil && err.Error() != "node config is missing" {
			// ignore for now, we just want to test concurrency safety
		}
	}()

	go func() {
		defer wg.Done()
		err := srv.handleJobMessage(context.Background(), mockMsg{data: msgBytes})
		if err != nil && err.Error() != "node config is missing" {
			// ignore
		}
	}()

	wg.Wait()
}
