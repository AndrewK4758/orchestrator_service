package main

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"orchestrator_service/internal/events"
)

func main() {
	pub, err := events.NewPublisher(context.Background(), "nats://localhost:4222")
	if err != nil {
		panic(err)
	}
	err = pub.ConsumeJobs(context.Background(), func(ctx context.Context, msg jetstream.Msg) error { return nil })
	fmt.Printf("ConsumeJobs returned: %v\n", err)
}
