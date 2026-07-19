package ai_client

import (
	"context"
	"fmt"
	"orchestrator_service/internal/logger"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	aipb "github.com/AndrewK4758/shared_protos"
)

// Client wraps the NATS connection to the AI Service.
type Client struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	clientID string
	sub      jetstream.ConsumeContext
	pending  sync.Map
}

// NewClient connects to the NATS server.
func NewClient(natsURL string) (*Client, error) {
	logger.Info("System", "Connecting to NATS at %s for AI Service...", natsURL)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ensure AI_RESPONSES stream exists
	streamName := "AI_RESPONSES"
	_, err = js.Stream(ctx, streamName)
	if err != nil {
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{"ai.response.>"},
		})
		if err != nil {
			logger.Warn("System", "failed to create AI_RESPONSES stream (might already exist): %v", err)
		} else {
			logger.Info("System", "Successfully created JetStream stream: %s", streamName)
		}
	}

	clientID := uuid.New().String()
	client := &Client{
		nc:       nc,
		js:       js,
		clientID: clientID,
	}

	// Create a single multiplexed JetStream subscription for this orchestrator instance
	subSubject := fmt.Sprintf("ai.response.%s.>", clientID)
	
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subSubject,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream consumer: %w", err)
	}
	
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		replySubj := msg.Subject()
		if ch, ok := client.pending.Load(replySubj); ok {
			ch.(chan jetstream.Msg) <- msg
		} else {
			// Orphaned message or timeout
			msg.Ack()
		}
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to start consuming JS messages: %w", err)
	}
	client.sub = cc

	return client, nil
}

// Close gracefully closes the NATS connection.
func (c *Client) Close() error {
	if c.sub != nil {
		c.sub.Stop()
	}
	c.nc.Close()
	return nil
}

// PerformAction publishes a request to the AI service via JetStream and waits for a response on a Core NATS reply subject.
func (c *Client) PerformAction(ctx context.Context, workflowCtx *aipb.ExecuteWorkflowNodeRequest, currentAction *aipb.ActionDefinition, inputBytes []byte, inputText string, modelChoice aipb.ModelChoice) (*aipb.PerformActionResponse, error) {
	req := &aipb.PerformActionRequest{
		InputBytes:    inputBytes,
		InputText:     inputText,
		Context:       workflowCtx,
		CurrentAction: currentAction,
		ModelChoice:   modelChoice,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal PerformActionRequest: %w", err)
	}

	appID := "default"
	tenantID := "default"
	if workflowCtx != nil && workflowCtx.Metadata != nil {
		if val, ok := workflowCtx.Metadata["app_id"]; ok && val != "" {
			appID = val
		}
		if val, ok := workflowCtx.Metadata["tenant_id"]; ok && val != "" {
			tenantID = val
		}
	}

	jobID := "default"
	if workflowCtx != nil && workflowCtx.JobId != "" {
		jobID = workflowCtx.JobId
	}

	modelStr := "UNSPECIFIED"
	switch req.ModelChoice {
	case aipb.ModelChoice_MODEL_CHOICE_EXTRACTION:
		modelStr = "EXTRACTION"
	case aipb.ModelChoice_MODEL_CHOICE_CLASSIFICATION:
		modelStr = "CLASSIFICATION"
	}
	subject := fmt.Sprintf("ai.action.%s.%s.%s.%s", modelStr, appID, tenantID, jobID)
	// Incorporate the orchestrator clientID so the multiplexed subscriber catches it
	replySubject := fmt.Sprintf("ai.response.%s.%s.%s.%s.%d", c.clientID, appID, tenantID, jobID, time.Now().UnixNano())

	// Register the channel for the multiplexed reply
	respCh := make(chan jetstream.Msg, 1)
	c.pending.Store(replySubject, respCh)
	defer c.pending.Delete(replySubject)

	// Publish to JetStream
	msg := nats.NewMsg(subject)
	msg.Reply = replySubject
	msg.Header = make(nats.Header)
	msg.Header.Set("Reply-Subject", replySubject)
	msg.Data = reqBytes

	_, err = c.js.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to JetStream %s: %w", subject, err)
	}

	timeoutSecs := 300 // 5 minutes default
	if envVal := os.Getenv("NATS_TIMEOUT_SECONDS"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			timeoutSecs = parsed
		}
	}

	// Wait for response
	var respMsg jetstream.Msg
	select {
	case msg := <-respCh:
		respMsg = msg
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout or context cancelled waiting for ai_service reply: %w", ctx.Err())
	case <-time.After(time.Duration(timeoutSecs) * time.Second):
		return nil, fmt.Errorf("timeout waiting for ai_service reply (%ds fallback)", timeoutSecs)
	}

	// Acknowledge the JetStream message
	if err := respMsg.Ack(); err != nil {
		logger.Warn("System", "failed to ack AI response message: %v", err)
	}

	resp := &aipb.PerformActionResponse{}
	if err := proto.Unmarshal(respMsg.Data(), resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PerformActionResponse: %w", err)
	}

	if !resp.GetSuccess() {
		return nil, fmt.Errorf("ai_service returned failure: %s", resp.GetErrorMessage())
	}

	return resp, nil
}
