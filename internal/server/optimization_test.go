package server

import (
	"fmt"
	"testing"
)

func BenchmarkWebhookConnsLockContention(b *testing.B) {
	srv := &OrchestratorServer{
		webhookConns: make(map[string]*clientConnEntry),
	}

	for i := range 1000 {
		addr := fmt.Sprintf("127.0.0.1:%d", 10000+i)
		srv.webhookConns[addr] = nil
	}

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			addr := fmt.Sprintf("127.0.0.1:%d", 10000+(i%1000))

			srv.webhookConnMu.RLock()
			_, exists := srv.webhookConns[addr]
			srv.webhookConnMu.RUnlock()

			if !exists {
				b.Errorf("Expected conn to exist in map")
			}
			i++
		}
	})
}
