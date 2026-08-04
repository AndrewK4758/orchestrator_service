package server

import (
	"fmt"
	"testing"

	"google.golang.org/grpc"
)

func BenchmarkWebhookConnsLockContention(b *testing.B) {
	srv := &OrchestratorServer{
		webhookConns: make(map[string]*grpc.ClientConn),
	}

	// Pre-fill webhookConns to simulate load
	for i := 0; i < 1000; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 10000+i)
		srv.webhookConns[addr] = nil
	}

	b.ResetTimer()

	// Simulate heavy concurrent Read/Writes to webhookConns map
	b.RunParallel(func(p *testing.PB) {
		i := 0
		for p.Next() {
			addr := fmt.Sprintf("127.0.0.1:%d", 10000+(i%1000))

			// Simulate Read (Check Conn)
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
