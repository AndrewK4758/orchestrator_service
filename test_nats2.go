package main

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, _ := nats.Connect("nats://localhost:4222")
	js, _ := jetstream.New(nc)
	
	// Print stream info before
	stream, _ := js.Stream(context.Background(), "JOBS")
	info, _ := stream.Info(context.Background())
	fmt.Printf("Before Publish: LastSeq=%d\n", info.State.LastSeq)
	
	ack, err := js.Publish(context.Background(), "jobs.incoming", []byte("test"))
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	} else {
		fmt.Printf("SUCCESS: seq %d\n", ack.Sequence)
	}
	
	info2, _ := stream.Info(context.Background())
	fmt.Printf("After Publish: LastSeq=%d\n", info2.State.LastSeq)
}
