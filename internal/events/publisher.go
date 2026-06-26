package events

import (
	"context"
	"encoding/json"
	"fmt"

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
	
	// jetstream.Publish requires a context for the new API
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
