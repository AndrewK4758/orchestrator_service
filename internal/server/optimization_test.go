package server

import (
	"context"
	"fmt"
	"testing"

)

func BenchmarkActiveJobsLockContention(b *testing.B) {
	pub := &mockPublisher{}
	srv := NewServer(context.Background(), pub, nil)

	// Pre-fill activeExecs to simulate load
	for i := range 10000 {
		jobID := fmt.Sprintf("job_%d", i)
		srv.activeExecs[jobID] = &JobExecInfo{AppID: "test"}
	}

	b.ResetTimer()

	// Simulate heavy concurrent Read/Writes to activeExecs map
	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			jobID := fmt.Sprintf("job-%d", i)

			// Simulate Write (Add Job)
			srv.activeExecMu.Lock()
			srv.activeExecs[jobID] = &JobExecInfo{AppID: "test"}
			srv.activeExecMu.Unlock()

			// Simulate Read (Check Job)
			srv.activeExecMu.RLock()
			_, exists := srv.activeExecs[jobID]
			srv.activeExecMu.RUnlock()

			if !exists {
				b.Errorf("Expected job to exist in map")
			}
			i++
		}
	})
}
