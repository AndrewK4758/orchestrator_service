package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"orchestrator_service/internal/logger"
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
func NewPublisher(ctx context.Context, natsURL string) (*Publisher, error) {
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
	traceStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TRACE_EVENTS",
		Subjects: []string{"trace.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update trace stream: %w", err)
	}

	// Ensure the Jobs Stream exists
	jobsStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "JOBS",
		Subjects: []string{"jobs.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update jobs stream: %w", err)
	}

	if os.Getenv("PURGE_QUEUE_ON_STARTUP") == "true" {
		logger.Info("System", "DEVELOPMENT MODE: Purging JOBS and TRACE_EVENTS streams on startup...")
		_ = jobsStream.Purge(ctx)
		_ = traceStream.Purge(ctx)
	}

	return &Publisher{
		nc: nc,
		js: js,
	}, nil
}

// PublishTrace sends an event securely to JetStream so the .NET Traceability Service can record it.
func (p *Publisher) PublishTrace(ctx context.Context, event TraceabilityEvent) error {
	logger.Info("Publisher", "Preparing to publish trace event: %+v", event)
	eventBytes, err := json.Marshal(event)
	if err != nil {
		logger.Error("Publisher", "Failed to marshal event: %v", err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Publish to a specific subject, e.g., "trace.job.started"
	subject := fmt.Sprintf("trace.%s", event.EventType)
	logger.Info("Publisher", "Publishing to NATS subject: '%s'", subject)

	// Use JetStream publish for guaranteed delivery
	_, err = p.js.Publish(ctx, subject, eventBytes)
	if err != nil {
		logger.Error("Publisher", "Failed to publish trace event to NATS subject '%s': %v", subject, err)
		return fmt.Errorf("failed to publish trace event to jetstream: %w", err)
	}

	logger.Info("Publisher", "Successfully published to NATS subject: '%s'", subject)
	return nil
}

// Close gracefully closes the NATS connection.
func (p *Publisher) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

// PublishJob publishes a new job request to JetStream.
func (p *Publisher) PublishJob(ctx context.Context, msgId string, payload []byte) error {
	subject := "jobs.incoming"
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, msgId)

	_, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish job to jetstream: %w", err)
	}
	p.LogQueueSizeAsync(ctx, "Enqueue", "orchestrator_jobs_consumer")
	return nil
}

// PublishPriorityJob publishes an active job continuation to the priority JetStream subject.
func (p *Publisher) PublishPriorityJob(ctx context.Context, msgId string, payload []byte) error {
	subject := "jobs.priority"
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, msgId)

	_, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to publish priority job to jetstream: %w", err)
	}
	p.LogQueueSizeAsync(ctx, "Enqueue", "orchestrator_priority_consumer")
	return nil
}

func (p *Publisher) PublishCacheStore(ctx context.Context, data []byte) error {
	subject := "cache.requests.store"
	_, err := p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish cache store to jetstream: %w", err)
	}
	return nil
}

// LogQueueSizeAsync asynchronously queries JetStream for queue depths to provide non-blocking observability.
func (p *Publisher) LogQueueSizeAsync(parentCtx context.Context, action string, consumerName string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), 2*time.Second)
		defer cancel()
		cons, err := p.js.Consumer(ctx, "JOBS", consumerName)
		if err != nil {
			return
		}
		info, err := cons.Info(ctx)
		if err == nil {
			log.Printf("METRIC: [%s] Queue: %s | Pending: %d | AckPending: %d | Waiting: %d",
				action, consumerName, info.NumPending, info.NumAckPending, info.NumWaiting)
		}
	}()
}

func (p *Publisher) PurgeQueues(ctx context.Context) error {
	logger.Info("Publisher", "Purging all relevant JetStream queues...")

	streams := []string{"JOBS", "TRACE_EVENTS", "CACHE_REQUESTS"}
	for _, streamName := range streams {
		stream, err := p.js.Stream(ctx, streamName)
		if err == nil {
			_ = stream.Purge(ctx)
			logger.Info("Publisher", "Purged stream %s", streamName)
		}
	}
	return nil
}

func (p *Publisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	normalAckPending := 100
	priorityAckPending := 100

	if envVal := os.Getenv("NORMAL_MAX_ACK_PENDING"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			normalAckPending = parsed
		}
	}

	if envVal := os.Getenv("PRIORITY_MAX_ACK_PENDING"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			priorityAckPending = parsed
		}
	}

	normalPullMax := 100
	if envVal := os.Getenv("NORMAL_PULL_MAX_MESSAGES"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			normalPullMax = parsed
		}
	}

	priorityPullMax := 100
	if envVal := os.Getenv("PRIORITY_PULL_MAX_MESSAGES"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			priorityPullMax = parsed
		}
	}

	ackWaitSecs := 120 // 2 minutes default
	if envVal := os.Getenv("NATS_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			ackWaitSecs = parsed
		}
	}

	// Priority Consumer
	priorityCons, err := p.js.CreateOrUpdateConsumer(ctx, "JOBS", jetstream.ConsumerConfig{
		Durable:       "orchestrator_priority_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Duration(ackWaitSecs) * time.Second,
		MaxAckPending: priorityAckPending,
		FilterSubject: "jobs.priority",
		MaxDeliver:    3,
	})
	if err != nil {
		return fmt.Errorf("failed to create priority consumer: %w", err)
	}

	// Normal Consumer
	normalCons, err := p.js.CreateOrUpdateConsumer(ctx, "JOBS", jetstream.ConsumerConfig{
		Durable:       "orchestrator_jobs_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Duration(ackWaitSecs) * time.Second,
		MaxAckPending: normalAckPending,
		FilterSubject: "jobs.incoming",
		MaxDeliver:    3,
	})
	if err != nil {
		return fmt.Errorf("failed to create normal consumer: %w", err)
	}

	logger.Debug("System", "Consumers started on stream JOBS (Normal pending=%d, Priority pending=%d). Waiting for messages...", normalAckPending, priorityAckPending)
	createProcessMsg := func(consumerName string) func(msg jetstream.Msg) {
		return func(msg jetstream.Msg) {
			p.LogQueueSizeAsync(ctx, "Dequeue", consumerName)
			go func(m jetstream.Msg) {
				// Heartbeat goroutine to keep the message alive during long AI processing
				done := make(chan struct{})
				go func() {
					ticker := time.NewTicker(60 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							_ = m.InProgress()
						case <-done:
							return
						}
					}
				}()

				err := handler(ctx, m)
				close(done) // Signal heartbeat to stop

				if err != nil {
					logger.Error("System", "Failed to process job msg: %v", err)
					_ = m.Nak()
				} else {
					_ = m.Ack()
				}
			}(msg)
		}
	}

	ccPriority, err := priorityCons.Consume(createProcessMsg("orchestrator_priority_consumer"), jetstream.PullMaxMessages(priorityPullMax))
	if err != nil {
		return fmt.Errorf("failed to start consuming priority: %w", err)
	}

	ccNormal, err := normalCons.Consume(createProcessMsg("orchestrator_jobs_consumer"), jetstream.PullMaxMessages(normalPullMax))
	if err != nil {
		ccPriority.Stop()
		return fmt.Errorf("failed to start consuming normal: %w", err)
	}

	go func() {
		<-ctx.Done()
		ccPriority.Stop()
		ccNormal.Stop()
	}()

	return nil
}
