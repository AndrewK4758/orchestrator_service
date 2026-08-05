package engine

import "github.com/AndrewK4758/shared_utils/payloadkeys"

import (
	"strings"
	"context"
	"errors"
	"os"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestMain(m *testing.M) {
	os.Setenv("LOG_COMPONENT_ORCHESTRATOR", "orchestrator")
	os.Setenv("LOG_EVENT_ENQUEUED", "enqueued")
	os.Setenv("NATS_WORKERS_STREAM", "WORKERS")
	os.Exit(m.Run())
}

type mockJetStream struct {
	mock.Mock
	jetstream.JetStream
}

func (m *mockJetStream) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	args := m.Called(ctx, msg, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*jetstream.PubAck), args.Error(1)
}

func assertPanicsWithError(t *testing.T, expectedErr string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("The code did not panic")
		} else {
			err, ok := r.(error)
			if !ok {
				t.Errorf("Expected panic with error, got %v", r)
			} else if !strings.Contains(err.Error(), expectedErr) {
				t.Errorf("Expected error to contain %q, but got %q", expectedErr, err.Error())
			}
		}
	}()
	f()
}

func TestEngine_ExecuteTask_ValidationErrors(t *testing.T) {
	eng := &Engine{
		clientID: "orchestrator-task-router",
	}
	ctx := context.Background()

	t.Run("nil request", func(t *testing.T) {
		assertPanicsWithError(t, "missing infrastructure identity", func() {
			eng.ExecuteTask(ctx, nil)
		})
	})

	t.Run("nil identity", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			TaskSubject: "worker.test",
		}
		assertPanicsWithError(t, "missing infrastructure identity", func() {
			eng.ExecuteTask(ctx, req)
		})
	})

	t.Run("empty tenant_id", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{AppId: "app1", JobId: "job1"},
			TaskSubject: "worker.test",
		}
		assertPanicsWithError(t, "missing infrastructure identity", func() {
			eng.ExecuteTask(ctx, req)
		})
	})

	t.Run("empty app_id", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", JobId: "job1"},
			TaskSubject: "worker.test",
		}
		assertPanicsWithError(t, "missing infrastructure identity", func() {
			eng.ExecuteTask(ctx, req)
		})
	})

	t.Run("empty job_id", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1"},
			TaskSubject: "worker.test",
		}
		assertPanicsWithError(t, "missing infrastructure identity", func() {
			eng.ExecuteTask(ctx, req)
		})
	})

	t.Run("missing task subject", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		}
		assertPanicsWithError(t, "missing task subject", func() {
			eng.ExecuteTask(ctx, req)
		})
	})
}

func TestEngine_ExecuteTask_PublishSuccess(t *testing.T) {
	mockJS := new(mockJetStream)
	eng := &Engine{
		js:       mockJS,
		clientID: "orchestrator-task-router",
	}

	payload, err := structpb.NewStruct(map[string]interface{}{
		payloadkeys.MetadataKey: map[string]interface{}{
			"callback_address": "localhost:1234",
		},
	})
	assert.NoError(t, err)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant1",
			AppId:    "app1",
			JobId:    "job1",
		},
		TaskSubject: "worker.ai.inference",
		StepId:      "step123",
		Payload:     payload,
	}

	mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
		expectedSubject := "worker.ai.inference.app1.tenant1.job1"
		expectedReply := "orchestrator.response.app1.tenant1.job1.step123.bG9jYWxob3N0OjEyMzQ"
		return msg.Subject == expectedSubject && msg.Header.Get("Reply-Subject") == expectedReply
	}), mock.Anything).Return(&jetstream.PubAck{Sequence: 1}, nil)

	err = eng.ExecuteTask(context.Background(), req)
	assert.NoError(t, err)
	mockJS.AssertExpectations(t)
}

func TestEngine_ExecuteTask_PublishError(t *testing.T) {
	mockJS := new(mockJetStream)
	eng := &Engine{
		js:       mockJS,
		clientID: "orchestrator-task-router",
	}

	payload, err := structpb.NewStruct(map[string]interface{}{
		payloadkeys.MetadataKey: map[string]interface{}{
			"callback_address": "localhost:1234",
		},
	})
	assert.NoError(t, err)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant1",
			AppId:    "app1",
			JobId:    "job1",
		},
		TaskSubject: "worker.error",
		StepId:      "step123",
		Payload:     payload,
	}

	mockJS.On("PublishMsg", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("nats publish failed"))

	err = eng.ExecuteTask(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish to worker")
}

func TestEngine_Close_NilConn(t *testing.T) {
	eng := &Engine{}
	assert.NotPanics(t, func() {
		eng.Close()
	})
}
