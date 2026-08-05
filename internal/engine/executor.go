package engine

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"
	"github.com/AndrewK4758/shared_utils/workflowparser"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Engine struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	clientID string
}

func NewEngine(ctx context.Context) (*Engine, error) {
	natsURL := envutil.MustGetString("NATS_URL")

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to init JetStream: %w", err)
	}

	return &Engine{
		nc:       nc,
		js:       js,
		clientID: "orchestrator-task-router",
	}, nil
}

func (e *Engine) Close() {
	if e.nc != nil {
		e.nc.Close()
	}
}

// ExecuteTask acts as a pure Task Router. It validates identity and routes to the generic worker queue.
func (e *Engine) ExecuteTask(ctx context.Context, req *pb.ExecuteTaskRequest) error {
	if req == nil || req.Identity == nil || req.Identity.TenantId == "" || req.Identity.AppId == "" || req.Identity.JobId == "" {
		return fmt.Errorf("invalid request: missing infrastructure identity")
	}
	if req.TaskSubject == "" {
		return fmt.Errorf("invalid request: missing task subject")
	}

	t, a, j := req.Identity.TenantId, req.Identity.AppId, req.Identity.JobId
	subject := fmt.Sprintf("%s.%s.%s.%s", req.TaskSubject, a, t, j)

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	callbackAddr := workflowparser.NewTaskRequestParser(req).MustGetCallbackAddress(req.Identity.JobId)
	encodedCallback := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(callbackAddr))

	// Artemis (the client) will be listening on this subject for completion
	replySubj := fmt.Sprintf("orchestrator.response.%s.%s.%s.%s.%s", a, t, j, req.StepId, encodedCallback)

	msgID := fmt.Sprintf("%s-%s", j, req.StepId)
	nMsg := nats.NewMsg(subject)
	nMsg.Reply = replySubj
	nMsg.Header = make(nats.Header)
	nMsg.Header.Set("Reply-Subject", replySubj)
	nMsg.Data = reqBytes

	reqJSON := protojson.MarshalOptions{Multiline: false}.Format(req)
	logger.Info("Engine", "Outbound task dispatch payload for subject %s: %s", subject, reqJSON)

	if e.js != nil {
		_, err = e.js.PublishMsg(ctx, nMsg, jetstream.WithMsgID(msgID))
	} else {
		err = e.nc.PublishMsg(nMsg)
	}

	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: envutil.MustGetString("LOG_COMPONENT_ORCHESTRATOR"),
		Event:     envutil.MustGetString("LOG_EVENT_ENQUEUED"),
		Stream:    envutil.MustGetString("NATS_WORKERS_STREAM"),
		Subject:   subject,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		MsgID:     msgID,
		SizeBytes: len(reqBytes),
		Error:     err,
	})

	if err != nil {
		logger.Error("Engine", "Failed to publish task to %s: %v", subject, err)
		return fmt.Errorf("failed to publish to worker: %w", err)
	}

	return nil
}
