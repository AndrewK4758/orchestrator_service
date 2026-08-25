package engine

import (
    "context"
    "errors"
    "os"
    "strings"
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

	t.Run("missing callback address", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			TaskSubject: "worker.test",
		}
		assertPanicsWithError(t, "missing callback address", func() {
			eng.ExecuteTask(ctx, req)
		})
	})

	t.Run("missing task subject", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
			CallbackAddress: "localhost:1234",
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

	payload, err := structpb.NewStruct(map[string]any{
		"key": "val",
	})
	assert.NoError(t, err)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant1",
			AppId:    "app1",
			JobId:    "job1",
		},
		TaskSubject:     "worker.ai.inference",
		StepId:          "step123",
		CallbackAddress: "localhost:1234",
		Payload:         payload,
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

	payload, err := structpb.NewStruct(map[string]any{
		"key": "val",
	})
	assert.NoError(t, err)

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant1",
			AppId:    "app1",
			JobId:    "job1",
		},
		TaskSubject:     "worker.error",
		StepId:          "step123",
		CallbackAddress: "localhost:1234",
		Payload:         payload,
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

func TestEngine_ResumeJob(t *testing.T) {
	mockJS := new(mockJetStream)
	eng := &Engine{
		js:       mockJS,
		clientID: "orchestrator-task-router",
	}

	ctx := context.Background()

	t.Run("Happy path publishes to JetStream", func(t *testing.T) {
		req := &pb.ResumeJobRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant1",
				AppId:    "app1",
				JobId:    "job1",
			},
			StepId:          "step_resumed",
			CallbackAddress: "http://localhost:50051",
		}

		mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
			expectedSubject := "orchestrator.job.resumed.tenant1.app1.job1"
			return msg.Subject == expectedSubject
		}), mock.Anything).Return(&jetstream.PubAck{Sequence: 1}, nil)

		err := eng.ResumeJob(ctx, req)
		assert.NoError(t, err)
		mockJS.AssertExpectations(t)
	})

	t.Run("Fail fast panics on invalid identity", func(t *testing.T) {
		assertPanicsWithError(t, "missing infrastructure identity", func() {
			_ = eng.ResumeJob(ctx, nil)
		})

		assertPanicsWithError(t, "missing infrastructure identity", func() {
			_ = eng.ResumeJob(ctx, &pb.ResumeJobRequest{
				StepId:          "step1",
				CallbackAddress: "http://localhost:50051",
			})
		})

		assertPanicsWithError(t, "missing step id", func() {
			_ = eng.ResumeJob(ctx, &pb.ResumeJobRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t1",
					AppId:    "a1",
					JobId:    "j1",
				},
				StepId:          "",
				CallbackAddress: "http://localhost:50051",
			})
		})

		assertPanicsWithError(t, "missing callback address", func() {
			_ = eng.ResumeJob(ctx, &pb.ResumeJobRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "t1",
					AppId:    "a1",
					JobId:    "j1",
				},
				StepId:          "step1",
				CallbackAddress: "",
			})
		})
	})
}

func TestEngine_ExecuteTask_MsgID_UniquenessAcrossTaskTypes(t *testing.T) {
	mockJS := new(mockJetStream)
	eng := &Engine{
		js:       mockJS,
		clientID: "orchestrator-task-router",
	}

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant-101",
		AppId:    "app-alpha",
		JobId:    "job-unique-test",
	}

	req1 := &pb.ExecuteTaskRequest{
		Identity:        identity,
		TaskSubject:     "worker.cache.lookup",
		StepId:          "step-500",
		CallbackAddress: "localhost:8080",
	}

	req2 := &pb.ExecuteTaskRequest{
		Identity:        identity,
		TaskSubject:     "worker.doc.processing.cpp_ml",
		StepId:          "step-500",
		CallbackAddress: "localhost:8080",
	}

	var publishedMsgIDs []string
	mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
		msgID := msg.Header.Get(nats.MsgIdHdr)
		publishedMsgIDs = append(publishedMsgIDs, msgID)
		return true
	}), mock.Anything).Return(&jetstream.PubAck{Sequence: 1}, nil)

	err := eng.ExecuteTask(context.Background(), req1)
	assert.NoError(t, err)

	err = eng.ExecuteTask(context.Background(), req2)
	assert.NoError(t, err)

	assert.Len(t, publishedMsgIDs, 2)
	assert.Equal(t, "job-unique-test-step-500-worker.cache.lookup-0", publishedMsgIDs[0])
	assert.Equal(t, "job-unique-test-step-500-worker.doc.processing.cpp_ml-0", publishedMsgIDs[1])
	assert.NotEqual(t, publishedMsgIDs[0], publishedMsgIDs[1], "Task message IDs must be unique under identical JobId and StepId")
}

func TestEngine_ExecuteTask_MsgID_AttemptHandling(t *testing.T) {
	mockJS := new(mockJetStream)
	eng := &Engine{
		js:       mockJS,
		clientID: "orchestrator-task-router",
	}

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant-101",
		AppId:    "app-alpha",
		JobId:    "job-attempt-test",
	}

	t.Run("x-attempt header sets attempt index", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:        identity,
			TaskSubject:     "worker.ai.inference",
			StepId:          "step-600",
			CallbackAddress: "localhost:8080",
			Headers:         map[string]string{"x-attempt": "2"},
		}

		var capturedMsgID string
		mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
			capturedMsgID = msg.Header.Get(nats.MsgIdHdr)
			return true
		}), mock.Anything).Return(&jetstream.PubAck{Sequence: 1}, nil).Once()

		err := eng.ExecuteTask(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, "job-attempt-test-step-600-worker.ai.inference-2", capturedMsgID)
	})

	t.Run("attempt header sets attempt index", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:        identity,
			TaskSubject:     "worker.ai.inference",
			StepId:          "step-600",
			CallbackAddress: "localhost:8080",
			Headers:         map[string]string{"attempt": "3"},
		}

		var capturedMsgID string
		mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
			capturedMsgID = msg.Header.Get(nats.MsgIdHdr)
			return true
		}), mock.Anything).Return(&jetstream.PubAck{Sequence: 2}, nil).Once()

		err := eng.ExecuteTask(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, "job-attempt-test-step-600-worker.ai.inference-3", capturedMsgID)
	})

	t.Run("default attempt index is 0 when unsupplied", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:        identity,
			TaskSubject:     "worker.ai.inference",
			StepId:          "step-600",
			CallbackAddress: "localhost:8080",
		}

		var capturedMsgID string
		mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
			capturedMsgID = msg.Header.Get(nats.MsgIdHdr)
			return true
		}), mock.Anything).Return(&jetstream.PubAck{Sequence: 3}, nil).Once()

		err := eng.ExecuteTask(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, "job-attempt-test-step-600-worker.ai.inference-0", capturedMsgID)
	})
}

func TestEngine_ExecuteTask_MsgID_InvalidAttempt_Panics(t *testing.T) {
	eng := &Engine{
		clientID: "orchestrator-task-router",
	}

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant-101",
		AppId:    "app-alpha",
		JobId:    "job-attempt-test",
	}

	t.Run("invalid x-attempt header panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:        identity,
			TaskSubject:     "worker.ai.inference",
			StepId:          "step-600",
			CallbackAddress: "localhost:8080",
			Headers:         map[string]string{"x-attempt": "invalid_num"},
		}

		assertPanicsWithError(t, "invalid x-attempt header", func() {
			_ = eng.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("invalid attempt header panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity:        identity,
			TaskSubject:     "worker.ai.inference",
			StepId:          "step-600",
			CallbackAddress: "localhost:8080",
			Headers:         map[string]string{"attempt": "invalid_num"},
		}

		assertPanicsWithError(t, "invalid attempt header", func() {
			_ = eng.ExecuteTask(context.Background(), req)
		})
	})
}

func TestEngine_ExecuteTask_MultiWorkerChain_NoDeduplicationDrop(t *testing.T) {
	mockJS := new(mockJetStream)
	eng := &Engine{
		js:       mockJS,
		clientID: "orchestrator-task-router",
	}

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant-multi",
		AppId:    "app-multi",
		JobId:    "job-multi-worker",
	}

	// Simulate JetStream deduplication window: if the exact same msgID is published twice, return error
	seenMsgIDs := make(map[string]bool)
	mockJS.On("PublishMsg", mock.Anything, mock.MatchedBy(func(msg *nats.Msg) bool {
		msgID := msg.Header.Get(nats.MsgIdHdr)
		if seenMsgIDs[msgID] {
			return false
		}
		seenMsgIDs[msgID] = true
		return true
	}), mock.Anything).Return(&jetstream.PubAck{Sequence: 1}, nil)

	// Step dispatches worker 1: cache lookup
	req1 := &pb.ExecuteTaskRequest{
		Identity:        identity,
		TaskSubject:     "worker.cache.lookup",
		StepId:          "step-chain-1",
		CallbackAddress: "localhost:9000",
	}
	err := eng.ExecuteTask(context.Background(), req1)
	assert.NoError(t, err)

	// Step dispatches worker 2: doc processing (same JobId and StepId)
	req2 := &pb.ExecuteTaskRequest{
		Identity:        identity,
		TaskSubject:     "worker.doc.processing.cpp_ml",
		StepId:          "step-chain-1",
		CallbackAddress: "localhost:9000",
	}
	err = eng.ExecuteTask(context.Background(), req2)
	assert.NoError(t, err)

	assert.True(t, seenMsgIDs["job-multi-worker-step-chain-1-worker.cache.lookup-0"])
	assert.True(t, seenMsgIDs["job-multi-worker-step-chain-1-worker.doc.processing.cpp_ml-0"])
	assert.Equal(t, 2, len(seenMsgIDs))
}

