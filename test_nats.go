package main

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		fmt.Printf("Connect err: %v\n", err)
		return
	}
	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Printf("JS err: %v\n", err)
		return
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "JOBS")
	if err != nil {
		fmt.Printf("Stream err: %v\n", err)
		return
	}
	info, _ := stream.Info(ctx)
	fmt.Printf("JOBS stream has %d messages\n", info.State.Msgs)

	cons, err := stream.Consumer(ctx, "orchestrator_jobs_consumer")
	if err != nil {
		fmt.Printf("Consumer err: %v\n", err)
		return
	}
	cinfo, _ := cons.Info(ctx)
	fmt.Printf("Consumer unacked: %d\n", cinfo.NumAckPending)
}
