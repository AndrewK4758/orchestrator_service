package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher handles pushing state and trace events to JetStream.
type Publisher struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// NewPublisher creates a new NATS JetStream publisher.
func NewPublisher(natsURL string) (*Publisher, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream Context (v2 API)
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Ensure the Trace Stream exists
	ctx := context.Background()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TRACE_EVENTS",
		Subjects: []string{"trace.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update trace stream: %w", err)
	}

	// Ensure the Jobs Stream exists
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "JOBS",
		Subjects: []string{"jobs.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update jobs stream: %w", err)
	}

	return &Publisher{
		nc: nc,
		js: js,
	}, nil
}

// PublishTrace sends an event securely to JetStream so the .NET Traceability Service can record it.
func (p *Publisher) PublishTrace(ctx context.Context, event TraceabilityEvent) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Publish to a specific subject, e.g., "trace.job.started"
	subject := fmt.Sprintf("trace.%s", event.EventType)

	// Use JetStream publish for guaranteed delivery
	_, err = p.js.Publish(ctx, subject, eventBytes)
	if err != nil {
		return fmt.Errorf("failed to publish trace event to jetstream: %w", err)
	}

	return nil
}

// Close gracefully closes the NATS connection.
func (p *Publisher) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

// PublishJob publishes a new job request to JetStream.
func (p *Publisher) PublishJob(ctx context.Context, jobID string, payload []byte) error {
	subject := "jobs.incoming"
	_, err := p.js.Publish(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("failed to publish job to jetstream: %w", err)
	}
	return nil
}

// ConsumeJobs starts a consumer that processes incoming jobs using the provided handler.
func (p *Publisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	cons, err := p.js.CreateOrUpdateConsumer(ctx, "JOBS", jetstream.ConsumerConfig{
		Durable:       "orchestrator_jobs_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Minute, // Give AI service up to 30 minutes to process
		MaxAckPending: 5,                // Concurrency control: max 5 jobs processing simultaneously per instance
		FilterSubject: "jobs.incoming",
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		err := handler(ctx, msg)
		if err != nil {
			log.Printf("ERROR: Failed to process job msg: %v", err)
			msg.Nak()
		} else {
			msg.Ack()
		}
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	go func() {
		<-ctx.Done()
		cc.Stop()
	}()

	return nil
}
