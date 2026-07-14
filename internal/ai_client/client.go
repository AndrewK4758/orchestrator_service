package ai_client

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	aipb "github.com/AndrewK4758/shared_protos"
)

// Client wraps the NATS connection to the AI Service.
type Client struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	clientID string
	sub      *nats.Subscription
	pending  sync.Map
}

// NewClient connects to the NATS server.
func NewClient(natsURL string) (*Client, error) {
	log.Printf("Connecting to NATS at %s for AI Service...", natsURL)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	// Ensure AI_RESPONSES stream exists
	streamName := "AI_RESPONSES"
	_, err = js.StreamInfo(streamName)
	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{"ai.response.>"},
		})
		if err != nil {
			log.Printf("Warning: failed to create AI_RESPONSES stream (might already exist): %v", err)
		} else {
			log.Printf("Successfully created JetStream stream: %s", streamName)
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
	sub, err := js.Subscribe(subSubject, func(msg *nats.Msg) {
		replySubj := msg.Subject
		if ch, ok := client.pending.Load(replySubj); ok {
			ch.(chan *nats.Msg) <- msg
		} else {
			// Orphaned message or timeout
			msg.Ack()
		}
	}, nats.ManualAck())
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create multiplexed JS subscription: %w", err)
	}
	client.sub = sub

	return client, nil
}

// Close gracefully closes the NATS connection.
func (c *Client) Close() error {
	if c.sub != nil {
		c.sub.Unsubscribe()
	}
	c.nc.Close()
	return nil
}

// PerformAction publishes a request to the AI service via JetStream and waits for a response on a Core NATS reply subject.
func (c *Client) PerformAction(ctx context.Context, workflowCtx *aipb.WorkflowContext, currentAction *aipb.ActionDefinition, inputBytes []byte, inputText string, modelChoice aipb.ModelChoice) (*aipb.PerformActionResponse, error) {
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
	respCh := make(chan *nats.Msg, 1)
	c.pending.Store(replySubject, respCh)
	defer c.pending.Delete(replySubject)

	// Publish to JetStream
	msg := nats.NewMsg(subject)
	msg.Reply = replySubject
	msg.Header = make(nats.Header)
	msg.Header.Set("Reply-Subject", replySubject)
	msg.Data = reqBytes

	_, err = c.js.PublishMsg(msg)
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
	var respMsg *nats.Msg
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
		log.Printf("Warning: failed to ack AI response message: %v", err)
	}

	resp := &aipb.PerformActionResponse{}
	if err := proto.Unmarshal(respMsg.Data, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PerformActionResponse: %w", err)
	}

	if !resp.GetSuccess() {
		return nil, fmt.Errorf("ai_service returned failure: %s", resp.GetErrorMessage())
	}

	return resp, nil
}
