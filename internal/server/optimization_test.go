package server

import (
	"fmt"
	"testing"

	"orchestrator_service/internal/ai_client"

	pb "github.com/AndrewK4758/shared_protos"
)

func BenchmarkActiveJobsLockContention(b *testing.B) {
	pub := &mockPublisher{}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	// Pre-fill activeJobs to simulate load
	for i := range 10000 {
		jobID := fmt.Sprintf("job_%d", i)
		srv.activeJobs[jobID] = &JobTracker{Req: &pb.ExecuteWorkflowNodeRequest{JobId: fmt.Sprintf("job_%d", i)}}
	}

	b.ResetTimer()

	// Simulate heavy concurrent Read/Writes to activeJobs map
	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			jobID := fmt.Sprintf("job-%d", i)

			// Simulate Write (Add Job)
			srv.mu.Lock()
			srv.activeJobs[jobID] = &JobTracker{Req: &pb.ExecuteWorkflowNodeRequest{JobId: jobID}}
			srv.mu.Unlock()

			// Simulate Read (Check Job)
			srv.mu.RLock()
			_, exists := srv.activeJobs[jobID]
			srv.mu.RUnlock()

			if !exists {
				b.Errorf("Expected job to exist in map")
			}
			i++
		}
	})
}
