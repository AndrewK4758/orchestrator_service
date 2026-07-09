package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
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
	log.Printf("INFO: [Publisher] Preparing to publish trace event: %+v", event)
	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("ERROR: [Publisher] Failed to marshal event: %v", err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Publish to a specific subject, e.g., "trace.job.started"
	subject := fmt.Sprintf("trace.%s", event.EventType)
	log.Printf("INFO: [Publisher] Publishing to NATS subject: '%s'", subject)

	// Use JetStream publish for guaranteed delivery
	_, err = p.js.Publish(ctx, subject, eventBytes)
	if err != nil {
		log.Printf("ERROR: [Publisher] Failed to publish trace event to NATS subject '%s': %v", subject, err)
		return fmt.Errorf("failed to publish trace event to jetstream: %w", err)
	}

	log.Printf("INFO: [Publisher] Successfully published to NATS subject: '%s'", subject)
	return nil
}

// Close gracefully closes the NATS connection.
func (p *Publisher) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

// PublishJob publishes a new job request to JetStream.
func (p *Publisher) PublishJob(ctx context.Context, payload []byte) error {
	subject := "jobs.incoming"
	_, err := p.js.Publish(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("failed to publish job to jetstream: %w", err)
	}
	return nil
}

func (p *Publisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	maxAckPending := 50
	if envMax := os.Getenv("MAX_ACK_PENDING"); envMax != "" {
		if m, err := strconv.Atoi(envMax); err == nil && m > 0 {
			maxAckPending = m
		}
	}

	ackWaitSecs := 1800 // 30 minutes default
	if envVal := os.Getenv("NATS_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			ackWaitSecs = parsed
		}
	}

	cons, err := p.js.CreateOrUpdateConsumer(ctx, "JOBS", jetstream.ConsumerConfig{
		Durable:       "orchestrator_jobs_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Duration(ackWaitSecs) * time.Second,
		MaxAckPending: maxAckPending, // Concurrency control: max jobs processing simultaneously per instance
		FilterSubject: "jobs.incoming",
		MaxDeliver:    3, // Prevent poison messages from infinite redelivery
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	log.Printf("DEBUG: Consumer started on stream JOBS, subject jobs.incoming, waiting for messages...")
	sem := make(chan struct{}, maxAckPending)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		meta, _ := msg.Metadata()
		if meta != nil {
			log.Printf("DEBUG: Consumer received a message! Seq: %v", meta.Sequence.Stream)
		} else {
			log.Printf("DEBUG: Consumer received a message!")
		}

		sem <- struct{}{}
		go func(m jetstream.Msg) {
			defer func() { <-sem }()
			err := handler(ctx, m)
			if err != nil {
				log.Printf("ERROR: Failed to process job msg: %v", err)
				_ = m.Nak()
			} else {
				_ = m.Ack()
			}
		}(msg)
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
