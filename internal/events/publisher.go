package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamJobs                  = "JOBS"
	StreamTraceEvents           = "TRACE_EVENTS"
	StreamWebhooks              = "WEBHOOKS"
	StreamCacheRequests         = "CACHE_REQUESTS"
	StreamWorkers               = "WORKERS"
	StreamAiResponses           = "AI_RESPONSES"
	StreamOrchestratorResponses = "ORCHESTRATOR_RESPONSES"

	KVActiveJobs         = "active_jobs"
	KVCacheRequestsStore = "cache.requests.store"

	SubjectIncomingJobFormat = "jobs.%s.%s.%s.incoming"
	SubjectTraceFormat       = "trace.%s"
)

// Publisher handles pushing state and trace events to JetStream.
type Publisher struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// NewPublisher creates a new NATS JetStream publisher.
func NewPublisher(ctx context.Context, natsURL string) (*Publisher, error) {
	nc, err := natsutil.ConnectWithRetries(natsURL, "Orchestrator")
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
		Name:     StreamTraceEvents,
		Subjects: []string{"trace.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update trace stream: %w", err)
	}

	// Ensure the Jobs Stream exists
	jobsStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamJobs,
		Subjects: []string{"jobs.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update jobs stream: %w", err)
	}

	// Ensure the Webhooks Stream exists
	webhooksStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamWebhooks,
		Subjects: []string{"webhooks.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update webhooks stream: %w", err)
	}

	// Ensure the Workers Stream exists
	workersStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamWorkers,
		Subjects:  []string{"worker.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update workers stream: %w", err)
	}

	// Ensure the AI_RESPONSES Stream exists
	aiResponsesStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamAiResponses,
		Subjects: []string{"ai.responses.>", "ai.response.>"},
		MaxAge:   1 * time.Hour,
		MaxBytes: 1024 * 1024 * 1024,
		MaxMsgs:  100000,
		Discard:  jetstream.DiscardOld,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update ai_responses stream: %w", err)
	}

	// Ensure the ORCHESTRATOR_RESPONSES Stream exists
	orchestratorResponsesStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamOrchestratorResponses,
		Subjects: []string{"orchestrator.responses.>", "orchestrator.response.>"},
		MaxAge:   1 * time.Hour,
		MaxBytes: 1024 * 1024 * 1024,
		MaxMsgs:  100000,
		Discard:  jetstream.DiscardOld,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create or update orchestrator_responses stream: %w", err)
	}

	if envutil.MustGetString("PURGE_QUEUE_ON_STARTUP") == "true" {
		logger.Info("System", "DEVELOPMENT MODE: Purging JOBS, TRACE_EVENTS, WEBHOOKS, WORKERS, AI_RESPONSES, and ORCHESTRATOR_RESPONSES streams on startup...")
		if err := jobsStream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge jobs stream: %v", err) }
		if err := traceStream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge trace stream: %v", err) }
		if err := webhooksStream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge webhooks stream: %v", err) }
		if err := workersStream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge workers stream: %v", err) }
		if err := aiResponsesStream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge aiResponses stream: %v", err) }
		if err := orchestratorResponsesStream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge orchestratorResponses stream: %v", err) }
		if kvStream, err := js.Stream(ctx, "KV_"+KVActiveJobs); err == nil {
			_ = kvStream.Purge(ctx)
		}
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
	subject := fmt.Sprintf(SubjectTraceFormat, event.EventType)
	logger.Info("Publisher", "Publishing to NATS subject: '%s'", subject)

	// Use JetStream publish for guaranteed delivery
	_, err = p.js.Publish(ctx, subject, eventBytes)
	tenantID, appID, jobID := natsutil.ExtractIdentity(event.Identity)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "Orchestrator",
		Event:     "ENQUEUED",
		Stream:    StreamTraceEvents,
		Subject:   subject,
		TenantID:  tenantID,
		AppID:     appID,
		JobID:     jobID,
		SizeBytes: len(eventBytes),
		Error:     err,
	})
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
func sanitizeSubjectToken(token string) string {
	token = strings.ReplaceAll(token, " ", "_")
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, ">", "_")
	token = strings.ReplaceAll(token, "*", "_")
	return token
}

// PublishJob publishes a new job request to JetStream.
func (p *Publisher) PublishJob(ctx context.Context, identity *pb.InfrastructureIdentity, msgId string, payload []byte) error {
	tenantId := sanitizeSubjectToken(identity.GetTenantId())
	appId := sanitizeSubjectToken(identity.GetAppId())
	jobId := sanitizeSubjectToken(identity.GetJobId())
	subject := fmt.Sprintf(SubjectIncomingJobFormat, tenantId, appId, jobId)
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, msgId)

	_, err := p.js.PublishMsg(ctx, msg)
	t, a, j := natsutil.ExtractIdentity(identity)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "Orchestrator",
		Event:     "ENQUEUED",
		Stream:    StreamJobs,
		Subject:   subject,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		MsgID:     msgId,
		SizeBytes: len(payload),
		Error:     err,
	})
	if err != nil {
		return fmt.Errorf("failed to publish job to jetstream: %w", err)
	}
	p.LogQueueSizeAsync(ctx, "Enqueue", "orchestrator_jobs_consumer")
	return nil
}

func (p *Publisher) PublishCacheStore(ctx context.Context, data []byte) error {
	subject := KVCacheRequestsStore
	_, err := p.js.Publish(ctx, subject, data)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "Orchestrator",
		Event:     "ENQUEUED",
		Stream:    StreamCacheRequests,
		Subject:   subject,
		SizeBytes: len(data),
		Error:     err,
	})
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
		cons, err := p.js.Consumer(ctx, StreamJobs, consumerName)
		if err != nil {
			logger.Warn("System", "Failed to retrieve consumer info for metrics logging: %v", err)
			return
		}
		info, err := cons.Info(ctx)
		if err == nil {
			logger.Info("Queue metrics", "%s: %s, %s: %s, %s: %d, %s: %d, %s: %d", "action", action, "consumer", consumerName, "pending", info.NumPending, "ackPending", info.NumAckPending, "waiting", info.NumWaiting)
		}
	}()
}

func (p *Publisher) PurgeQueues(ctx context.Context) error {
	logger.Info("Publisher", "Purging all relevant JetStream queues...")

	streams := []string{StreamJobs, StreamTraceEvents, StreamCacheRequests, StreamWorkers}
	for _, streamName := range streams {
		stream, err := p.js.Stream(ctx, streamName)
		if err == nil {
			if err := stream.Purge(ctx); err != nil { logger.Error("System", "Failed to purge stream: %v", err) }
			logger.Info("Publisher", "Purged stream %s", streamName)
		}
	}
	return nil
}

// GetActiveJobsKV returns the Key-Value store used to track jobs awaiting human validation.
func (p *Publisher) GetActiveJobsKV(ctx context.Context) (jetstream.KeyValue, error) {
	kv, err := p.js.KeyValue(ctx, KVActiveJobs)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			kv, err = p.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
				Bucket:  KVActiveJobs,
				Storage: jetstream.MemoryStorage,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create active_jobs KV bucket: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to get active_jobs KV bucket: %w", err)
		}
	}
	return kv, nil
}

func (p *Publisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	normalAckPending := 100
	priorityAckPending := 100

	if envVal := envutil.MustGetString("NORMAL_MAX_ACK_PENDING"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			normalAckPending = parsed
		} else if err != nil {
			return fmt.Errorf("failed to parse NORMAL_MAX_ACK_PENDING: %w", err)
		}
	}

	if envVal := envutil.MustGetString("PRIORITY_MAX_ACK_PENDING"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			priorityAckPending = parsed
		} else if err != nil {
			return fmt.Errorf("failed to parse PRIORITY_MAX_ACK_PENDING: %w", err)
		}
	}

	normalPullMax := 100
	if envVal := envutil.MustGetString("NORMAL_PULL_MAX_MESSAGES"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			normalPullMax = parsed
		} else if err != nil {
			return fmt.Errorf("failed to parse NORMAL_PULL_MAX_MESSAGES: %w", err)
		}
	}

	priorityPullMax := 100
	if envVal := envutil.MustGetString("PRIORITY_PULL_MAX_MESSAGES"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil && parsed > 0 {
			priorityPullMax = parsed
		} else if err != nil {
			return fmt.Errorf("failed to parse PRIORITY_PULL_MAX_MESSAGES: %w", err)
		}
	}

	ackWaitSecs, errNatsTimeout := strconv.Atoi(envutil.MustGetString("NATS_TIMEOUT_SECONDS")) // 2 minutes default
	if errNatsTimeout != nil {
		return fmt.Errorf("failed to parse NATS_TIMEOUT_SECONDS: %w", errNatsTimeout)
	}
	maxDeliver, errNatsMaxDel := strconv.Atoi(envutil.MustGetString("NATS_MAX_DELIVER"))
	if errNatsMaxDel != nil {
		return fmt.Errorf("failed to parse NATS_MAX_DELIVER: %w", errNatsMaxDel)
	}
	const (
		DurablePriorityConsumerName = "orchestrator_priority_consumer"
		DurableNormalConsumerName   = "orchestrator_jobs_consumer"
		SubjectPriority             = "jobs.*.*.*.priority"
		SubjectNormal               = "jobs.*.*.*.incoming"
	)

	// Priority Consumer
	priorityCons, err := p.js.CreateOrUpdateConsumer(ctx, StreamJobs, jetstream.ConsumerConfig{
		Durable:       DurablePriorityConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Duration(ackWaitSecs) * time.Second,
		MaxAckPending: priorityAckPending,
		FilterSubject: SubjectPriority,
		MaxDeliver:    maxDeliver,
	})
	if err != nil {
		return fmt.Errorf("failed to create priority consumer: %w", err)
	}

	// Normal Consumer
	normalCons, err := p.js.CreateOrUpdateConsumer(ctx, StreamJobs, jetstream.ConsumerConfig{
		Durable:       DurableNormalConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Duration(ackWaitSecs) * time.Second,
		MaxAckPending: normalAckPending,
		FilterSubject: SubjectNormal,
		MaxDeliver:    maxDeliver,
	})
	if err != nil {
		return fmt.Errorf("failed to create normal consumer: %w", err)
	}

	logger.Debug("System", "Consumers started on stream JOBS (Normal pending=%d, Priority pending=%d). Waiting for messages...", normalAckPending, priorityAckPending)
	createProcessMsg := func(consumerName string, sem chan struct{}) func(msg jetstream.Msg) {
		return func(msg jetstream.Msg) {
			sem <- struct{}{}
			p.LogQueueSizeAsync(ctx, "Dequeue", consumerName)
			go func(m jetstream.Msg) {
				defer func() { <-sem }()
				startTime := time.Now()
				meta, _ := m.Metadata()
				var attempt uint64 = 1
				if meta != nil {
					attempt = meta.NumDelivered
				}
				t, a, j := natsutil.ExtractIdentityFromJobSubject(m.Subject())
				msgID := ""
				if m.Headers() != nil {
					msgID = m.Headers().Get(nats.MsgIdHdr)
				}

				natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
					Component: "Orchestrator",
					Event:     "DEQUEUED",
					Stream:    StreamJobs,
					Subject:   m.Subject(),
					Consumer:  consumerName,
					TenantID:  t,
					AppID:     a,
					JobID:     j,
					MsgID:     msgID,
					SizeBytes: len(m.Data()),
					Attempt:   attempt,
				})

				// Heartbeat goroutine to keep the message alive during long AI processing
				done := make(chan struct{})
				go func() {
					ticker := time.NewTicker(60 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							if err := m.InProgress(); err == nil {
								natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
									Component: "Orchestrator",
									Event:     "IN_PROGRESS",
									Stream:    StreamJobs,
									Subject:   m.Subject(),
									Consumer:  consumerName,
									TenantID:  t,
									AppID:     a,
									JobID:     j,
									Attempt:   attempt,
								})
							}
						case <-done:
							return
						}
					}
				}()

				ctxMsg, cancelMsg := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Minute)
				err := handler(ctxMsg, m)
				cancelMsg()
				close(done) // Signal heartbeat to stop

				durationMs := time.Since(startTime).Milliseconds()
				if err != nil {
					logger.Error("System", "Failed to process job msg: %v", err)
					natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
						Component:  "Orchestrator",
						Event:      "NACKED",
						Stream:     StreamJobs,
						Subject:    m.Subject(),
						Consumer:   consumerName,
						TenantID:   t,
						AppID:      a,
						JobID:      j,
						MsgID:      msgID,
						Attempt:    attempt,
						DurationMs: durationMs,
						Error:      err,
					})
					if nakErr := m.Nak(); nakErr != nil { logger.Error("System", "Failed to NAK message: %v", nakErr) }
					return
				}
				
				natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
					Component:  "Orchestrator",
					Event:      "ACKNOWLEDGED",
					Stream:     StreamJobs,
					Subject:    m.Subject(),
					Consumer:   consumerName,
					TenantID:   t,
					AppID:      a,
					JobID:      j,
					MsgID:      msgID,
					Attempt:    attempt,
					DurationMs: durationMs,
				})
				if ackErr := m.Ack(); ackErr != nil { logger.Error("System", "Failed to ACK message: %v", ackErr) }
			}(msg)
		}
	}

	prioritySem := make(chan struct{}, priorityAckPending)
	ccPriority, err := priorityCons.Consume(createProcessMsg("orchestrator_priority_consumer", prioritySem), jetstream.PullMaxMessages(priorityPullMax))
	if err != nil {
		return fmt.Errorf("failed to start consuming priority: %w", err)
	}

	normalSem := make(chan struct{}, normalAckPending)
	ccNormal, err := normalCons.Consume(createProcessMsg("orchestrator_jobs_consumer", normalSem), jetstream.PullMaxMessages(normalPullMax))
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

// PublishWebhook queues a webhook to NATS for resilient delivery.
func (p *Publisher) PublishWebhook(ctx context.Context, payload *WebhookEvent) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	jobID := ""
	stepID := ""
	var identity *pb.InfrastructureIdentity
	if payload != nil && payload.Request != nil {
		stepID = payload.Request.GetStepId()
		identity = payload.Request.Identity
	}

	tenantID, appID, jobID := natsutil.ExtractIdentity(identity)
	if tenantID == "" || appID == "" || jobID == "" {
		return fmt.Errorf("invalid identity: tenantID, appID, and jobID are all required and cannot be empty")
	}

	msgId := fmt.Sprintf("%s-%s", jobID, stepID)
	msg := &nats.Msg{
		Subject: "webhooks.callback",
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, msgId)

	_, err = p.js.PublishMsg(ctx, msg)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "Orchestrator",
		Event:     "ENQUEUED",
		Stream:    StreamWebhooks,
		Subject:   "webhooks.callback",
		TenantID:  tenantID,
		AppID:     appID,
		JobID:     jobID,
		MsgID:     msgId,
		SizeBytes: len(data),
		Error:     err,
	})
	if err != nil {
		return fmt.Errorf("failed to publish webhook to jetstream: %w", err)
	}

	return nil
}

// ConsumeWebhooks handles consuming webhook payloads with exponential backoff.
func (p *Publisher) ConsumeWebhooks(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	cons, err := p.js.CreateOrUpdateConsumer(ctx, StreamWebhooks, jetstream.ConsumerConfig{
		Durable:       "orchestrator_webhooks_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: envutil.MustGetInt("WEBHOOKS_MAX_ACK_PENDING"),
		MaxDeliver:    envutil.MustGetInt("WEBHOOKS_MAX_DELIVER"),
		FilterSubject: "webhooks.callback",
		BackOff: []time.Duration{
			10 * time.Second,
			30 * time.Second,
			1 * time.Minute,
			2 * time.Minute,
			5 * time.Minute,
			10 * time.Minute,
			30 * time.Minute,
			1 * time.Hour,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create webhooks consumer: %w", err)
	}

	logger.Debug("System", "Consumer started on stream WEBHOOKS. Waiting for messages...")

	maxConcurrency := 50
	if envVal := envutil.MustGetString("WEBHOOKS_MAX_CONCURRENCY"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxConcurrency = parsed
		}
	}
	sem := make(chan struct{}, maxConcurrency)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		sem <- struct{}{}
		go func(m jetstream.Msg) {
			defer func() { <-sem }()
			startTime := time.Now()
			meta, _ := m.Metadata()
			var attempt uint64 = 1
			if meta != nil {
				attempt = meta.NumDelivered
			}
			t, a, j := "", "", ""
			var evt WebhookEvent
			if err := json.Unmarshal(m.Data(), &evt); err == nil {
				if evt.Request != nil && evt.Request.Identity != nil {
					t, a, j = natsutil.ExtractIdentity(evt.Request.Identity)
				}
			}

			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component: "Orchestrator",
				Event:     "DEQUEUED",
				Stream:    StreamWebhooks,
				Subject:   m.Subject(),
				Consumer:  "orchestrator_webhooks_consumer",
				TenantID:  t,
				AppID:     a,
				JobID:     j,
				SizeBytes: len(m.Data()),
				Attempt:   attempt,
			})

			ctxMsg, cancelMsg := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
			err := handler(ctxMsg, m)
			cancelMsg()
			durationMs := time.Since(startTime).Milliseconds()
			if err != nil {
				logger.Error("System", "Failed to process webhook msg, nacking for redelivery: %v", err)
				natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
					Component:  "Orchestrator",
					Event:      "NACKED",
					Stream:     StreamWebhooks,
					Subject:    m.Subject(),
					Consumer:   "orchestrator_webhooks_consumer",
					TenantID:   t,
					AppID:      a,
					JobID:      j,
					Attempt:    attempt,
					DurationMs: durationMs,
					Error:      err,
				})
				if nakErr := m.Nak(); nakErr != nil { logger.Error("System", "Failed to NAK message: %v", nakErr) }
				return
			}
			
			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component:  "Orchestrator",
				Event:      "ACKNOWLEDGED",
				Stream:     StreamWebhooks,
				Subject:    m.Subject(),
				Consumer:   "orchestrator_webhooks_consumer",
				TenantID:   t,
				AppID:      a,
				JobID:      j,
				Attempt:    attempt,
				DurationMs: durationMs,
			})
			if ackErr := m.Ack(); ackErr != nil { logger.Error("System", "Failed to ACK message: %v", ackErr) }
		}(msg)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming webhooks: %w", err)
	}

	go func() {
		<-ctx.Done()
		cc.Stop()
	}()

	return nil
}

// ConsumeOrchestratorResponses handles consuming responses from workers
func (p *Publisher) ConsumeOrchestratorResponses(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	cons, err := p.js.CreateOrUpdateConsumer(ctx, StreamOrchestratorResponses, jetstream.ConsumerConfig{
		Durable:       "orchestrator_responses_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
		FilterSubject: "orchestrator.response.>",
	})
	if err != nil {
		return fmt.Errorf("failed to create orchestrator responses consumer: %w", err)
	}

	logger.Debug("System", "Consumer started on stream %s. Waiting for messages...", StreamOrchestratorResponses)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		go func(m jetstream.Msg) {
			startTime := time.Now()
			md, _ := m.Metadata()
			attempt := uint64(1)
			if md != nil {
				attempt = md.NumDelivered
			}

			// Try parsing subject (orchestrator.response.<app>.<tenant>.<job>.<step>)
			parts := strings.Split(m.Subject(), ".")
			a, t, j := "N/A", "N/A", "N/A"
			if len(parts) >= 5 {
				a = parts[2]
				t = parts[3]
				j = parts[4]
			}

			ctxMsg, cancelMsg := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
			err := handler(ctxMsg, m)
			cancelMsg()
			durationMs := time.Since(startTime).Milliseconds()
			if err != nil {
				logger.Error("System", "Failed to process orchestrator response, nacking: %v", err)
				if nakErr := m.Nak(); nakErr != nil { logger.Error("System", "Failed to NAK message: %v", nakErr) }
				return
			}
			
			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component:  "Orchestrator",
				Event:      "ACKNOWLEDGED",
				Stream:     StreamOrchestratorResponses,
				Subject:    m.Subject(),
				Consumer:   "orchestrator_responses_consumer",
				TenantID:   t,
				AppID:      a,
				JobID:      j,
				Attempt:    attempt,
				DurationMs: durationMs,
			})
			if ackErr := m.Ack(); ackErr != nil { logger.Error("System", "Failed to ACK message: %v", ackErr) }
		}(msg)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming orchestrator responses: %w", err)
	}

	go func() {
		<-ctx.Done()
		cc.Stop()
	}()

	return nil
}
