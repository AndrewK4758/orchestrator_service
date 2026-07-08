package ai_client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	aipb "github.com/AndrewK4758/shared_protos"
)

// Client wraps the NATS connection to the AI Service.
type Client struct {
	nc *nats.Conn
	js nats.JetStreamContext
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

	return &Client{
		nc: nc,
		js: js,
	}, nil
}

// Close gracefully closes the NATS connection.
func (c *Client) Close() error {
	c.nc.Close()
	return nil
}

// PerformAction publishes a request to the AI service via JetStream and waits for a response on a Core NATS reply subject.
func (c *Client) PerformAction(ctx context.Context, workflowCtx *aipb.WorkflowContext, currentAction *aipb.ActionDefinition, inputBytes []byte, inputText string) (*aipb.PerformActionResponse, error) {
	req := &aipb.PerformActionRequest{
		InputBytes:    inputBytes,
		InputText:     inputText,
		Context:       workflowCtx,
		CurrentAction: currentAction,
		ModelChoice:   aipb.ModelChoice_MODEL_CHOICE_LOCAL,
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

	subject := fmt.Sprintf("ai.action.%s.%s.%s", appID, tenantID, jobID)
	replySubject := fmt.Sprintf("ai.response.%s.%s.%s.%d", appID, tenantID, jobID, time.Now().UnixNano())

	// Create a synchronous subscription on the JetStream reply subject
	sub, err := c.js.SubscribeSync(replySubject)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to reply subject on JetStream: %w", err)
	}
	defer sub.Unsubscribe()

	// Publish to JetStream
	msg := nats.NewMsg(subject)
	msg.Reply = replySubject
	msg.Data = reqBytes

	_, err = c.js.PublishMsg(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to JetStream %s: %w", subject, err)
	}

	// Wait for response
	// The AI Service could take a long time to process an LLM prompt. Wait up to 5 minutes.
	respMsg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		// Fallback to a long timeout if context doesn't have one
		respMsg, err = sub.NextMsg(5 * time.Minute)
		if err != nil {
			return nil, fmt.Errorf("timeout waiting for ai_service reply: %w", err)
		}
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
