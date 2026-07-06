package ai_client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	aipb "doc_processor/shared_protos"
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
func (c *Client) PerformAction(ctx context.Context, appID string, tenantID string, jobID string, inputBytes []byte, inputText string, prompt string, jsonSchema string) (*aipb.PerformActionResponse, error) {
	req := &aipb.PerformActionRequest{
		InputBytes:  inputBytes,
		InputText:   inputText,
		Prompt:      prompt,
		JsonSchema:  jsonSchema,
		ModelChoice: aipb.ModelChoice_MODEL_CHOICE_LOCAL,
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal PerformActionRequest: %w", err)
	}

	if appID == "" {
		appID = "default"
	}
	if tenantID == "" {
		tenantID = "default"
	}
	if jobID == "" {
		jobID = "default"
	}

	subject := fmt.Sprintf("ai.action.%s.%s.%s", appID, tenantID, jobID)
	replySubject := nats.NewInbox()

	// Create a synchronous subscription on the reply subject
	sub, err := c.nc.SubscribeSync(replySubject)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to reply subject: %w", err)
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

	resp := &aipb.PerformActionResponse{}
	if err := proto.Unmarshal(respMsg.Data, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PerformActionResponse: %w", err)
	}

	if !resp.GetSuccess() {
		return nil, fmt.Errorf("ai_service returned failure: %s", resp.GetErrorMessage())
	}

	return resp, nil
}
