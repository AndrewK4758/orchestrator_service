package server

import (
	"context"
	"fmt"
	"testing"

	"orchestrator_service/internal/ai_client"

	pb "github.com/AndrewK4758/shared_protos"
)

func BenchmarkActiveJobsLockContention(b *testing.B) {
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), &ai_client.Client{}, pub, nil)

	// Pre-fill activeJobs to simulate load
	for i := range 10000 {
		jobID := fmt.Sprintf("job_%d", i)
		srv.activeJobs[jobID] = &JobTracker{Req: &pb.ExecuteWorkflowNodeRequest{Identity: &pb.InfrastructureIdentity{JobId: fmt.Sprintf("job_%d", i), TenantId: "tenant_1"}}}
	}

	b.ResetTimer()

	// Simulate heavy concurrent Read/Writes to activeJobs map
	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			jobID := fmt.Sprintf("job-%d", i)

			// Simulate Write (Add Job)
			srv.mu.Lock()
			srv.activeJobs[jobID] = &JobTracker{Req: &pb.ExecuteWorkflowNodeRequest{Identity: &pb.InfrastructureIdentity{JobId: jobID, TenantId: "tenant_1"}}}
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
