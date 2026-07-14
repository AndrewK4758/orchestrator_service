package server

import (
	"context"
	"strings"
	"testing"

	"orchestrator_service/internal/ai_client"
	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/structpb"
)

type MockJobPublisher struct {
	PublishJobFunc         func(ctx context.Context, payload []byte) error
	PublishPriorityJobFunc func(ctx context.Context, payload []byte) error
	PurgeQueuesFunc        func(ctx context.Context) error
}

func (m *MockJobPublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	return nil
}

func (m *MockJobPublisher) PublishJob(ctx context.Context, payload []byte) error {
	if m.PublishJobFunc != nil {
		return m.PublishJobFunc(ctx, payload)
	}
	return nil
}

func (m *MockJobPublisher) PublishPriorityJob(ctx context.Context, payload []byte) error {
	if m.PublishPriorityJobFunc != nil {
		return m.PublishPriorityJobFunc(ctx, payload)
	}
	return nil
}

func (m *MockJobPublisher) ConsumeJobs(ctx context.Context, handler func(ctx context.Context, msg jetstream.Msg) error) error {
	return nil
}

func (m *MockJobPublisher) PublishCacheStore(ctx context.Context, data []byte) error {
	return nil
}

func (m *MockJobPublisher) PurgeQueues(ctx context.Context) error {
	if m.PurgeQueuesFunc != nil {
		return m.PurgeQueuesFunc(ctx)
	}
	return nil
}

func TestServerRouting(t *testing.T) {
	tests := []struct {
		name                string
		setup               func(srv *OrchestratorServer)
		execute             func(srv *OrchestratorServer) error
		expectPriorityQueue bool
		expectStandardQueue bool
		expectedErr         string
	}{
		{
			name: "SubmitJob queues to internal memory cache",
			execute: func(srv *OrchestratorServer) error {
				_, err := srv.SubmitJob(context.Background(), &pb.SubmitJobRequest{
					Context: &pb.WorkflowContext{JobId: "job123"},
				})
				return err
			},
			expectPriorityQueue: false,
			expectStandardQueue: false,
		},
		{
			name: "StartJob queues to Standard incoming queue",
			setup: func(srv *OrchestratorServer) {
				srv.SubmitJob(context.Background(), &pb.SubmitJobRequest{
					Context: &pb.WorkflowContext{JobId: "job-standard"},
				})
			},
			execute: func(srv *OrchestratorServer) error {
				_, err := srv.StartJob(context.Background(), &pb.StartJobRequest{JobId: "job-standard"})
				return err
			},
			expectPriorityQueue: false,
			expectStandardQueue: true,
		},
		{
			name: "SubmitStep queues to Priority incoming queue",
			setup: func(srv *OrchestratorServer) {
				srv.SubmitJob(context.Background(), &pb.SubmitJobRequest{
					Context: &pb.WorkflowContext{JobId: "job-priority-step"},
				})
			},
			execute: func(srv *OrchestratorServer) error {
				_, err := srv.SubmitStep(context.Background(), &pb.SubmitStepRequest{JobId: "job-priority-step", StepId: "step2"})
				return err
			},
			expectPriorityQueue: true,
			expectStandardQueue: false,
		},
		{
			name: "ResumeJob queues to Priority incoming queue",
			setup: func(srv *OrchestratorServer) {
				srv.SubmitJob(context.Background(), &pb.SubmitJobRequest{
					Context: &pb.WorkflowContext{JobId: "job-resume"},
					Steps:   []*pb.StepDefinition{{Id: "step-resume", StepName: "test"}},
				})
				srv.SubmitStep(context.Background(), &pb.SubmitStepRequest{JobId: "job-resume", StepId: "step-resume"})
			},
			execute: func(srv *OrchestratorServer) error {
				_, err := srv.ResumeJob(context.Background(), &pb.ResumeJobRequest{JobId: "job-resume"})
				return err
			},
			expectPriorityQueue: true,
			expectStandardQueue: false,
		},
		{
			name: "StartJob fails on missing Job",
			execute: func(srv *OrchestratorServer) error {
				_, err := srv.StartJob(context.Background(), &pb.StartJobRequest{JobId: "nonexistent"})
				return err
			},
			expectPriorityQueue: false,
			expectStandardQueue: false,
			expectedErr:         "not found in cache",
		},
		{
			name: "SubmitStep Idempotency (Multiple calls queue multiple times for JetStream deduplication)",
			setup: func(srv *OrchestratorServer) {
				srv.SubmitJob(context.Background(), &pb.SubmitJobRequest{
					Context: &pb.WorkflowContext{JobId: "job-idempotent"},
				})
			},
			execute: func(srv *OrchestratorServer) error {
				_, err1 := srv.SubmitStep(context.Background(), &pb.SubmitStepRequest{JobId: "job-idempotent", StepId: "step1"})
				_, err2 := srv.SubmitStep(context.Background(), &pb.SubmitStepRequest{JobId: "job-idempotent", StepId: "step1"})
				if err1 != nil {
					return err1
				}
				return err2
			},
			expectPriorityQueue: true, // Should succeed and push to queue, JetStream handles dedupe or replay.
			expectStandardQueue: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publishedStandard := false
			publishedPriority := false

			mockPub := &MockJobPublisher{
				PublishJobFunc: func(ctx context.Context, payload []byte) error {
					publishedStandard = true
					return nil
				},
				PublishPriorityJobFunc: func(ctx context.Context, payload []byte) error {
					publishedPriority = true
					return nil
				},
			}

			srv := NewServer(&ai_client.Client{}, mockPub, nil)

			if tc.setup != nil {
				tc.setup(srv)
			}

			publishedStandard = false
			publishedPriority = false

			err := tc.execute(srv)

			if tc.expectedErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
					t.Fatalf("expected err containing %q, got %v", tc.expectedErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if publishedStandard != tc.expectStandardQueue {
				t.Errorf("expected standard queue publish %v, got %v", tc.expectStandardQueue, publishedStandard)
			}

			if publishedPriority != tc.expectPriorityQueue {
				t.Errorf("expected priority queue publish %v, got %v", tc.expectPriorityQueue, publishedPriority)
			}
		})
	}
}

func TestArbitraryStateParsing(t *testing.T) {
	mockPub := &MockJobPublisher{}
	srv := NewServer(&ai_client.Client{}, mockPub, nil)

	statePayload, _ := structpb.NewStruct(map[string]interface{}{})
	statePayload.Fields = map[string]*structpb.Value{
		"fuzzed_key2": structpb.NewNumberValue(12345),
	}

	state := &pb.WorkflowContext{
		JobId:       "fuzz_job_123",
		GlobalState: statePayload,
	}

	_, err := srv.SubmitJob(context.Background(), &pb.SubmitJobRequest{
		Context: state,
	})

	if err != nil {
		t.Fatalf("Failed to submit job with arbitrary state: %v", err)
	}

	srv.mu.RLock()
	tracker := srv.activeJobs["fuzz_job_123"]
	srv.mu.RUnlock()

	if tracker.State.GlobalState.Fields["fuzzed_key2"].GetNumberValue() != 12345 {
		t.Fatalf("Arbitrary state was dropped or corrupted!")
	}
}

func TestPurgeSystemState(t *testing.T) {
	pub := &MockJobPublisher{
		PurgeQueuesFunc: func(ctx context.Context) error {
			return nil
		},
	}
	srv := NewServer(&ai_client.Client{}, pub, nil)

	// Inject active jobs
	cancelCalled1 := false
	cancelCalled2 := false
	cancelCalled3 := false

	srv.activeJobs["job_artemis_1"] = &JobTracker{
		State: &pb.WorkflowContext{
			JobId:    "job_artemis_1",
			Metadata: map[string]string{"app_id": "Artemis"},
		},
		Cancel: func() { cancelCalled1 = true },
	}
	srv.activeJobs["job_artemis_2"] = &JobTracker{
		State: &pb.WorkflowContext{
			JobId:    "job_artemis_2",
			Metadata: map[string]string{"app_id": "Artemis"},
		},
		Cancel: func() { cancelCalled2 = true },
	}
	srv.activeJobs["job_other_1"] = &JobTracker{
		State: &pb.WorkflowContext{
			JobId:    "job_other_1",
			Metadata: map[string]string{"app_id": "OtherApp"},
		},
		Cancel: func() { cancelCalled3 = true },
	}

	// 1. Happy Path: Purge Artemis jobs
	req := &pb.PurgeSystemStateRequest{
		AppId:       "Artemis",
		PurgeQueues: true,
	}
	resp, err := srv.PurgeSystemState(context.Background(), req)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if resp.JobsCancelled != 2 {
		t.Errorf("Expected 2 jobs cancelled, got %d", resp.JobsCancelled)
	}
	if resp.QueuesPurged != 3 {
		t.Errorf("Expected 3 queues purged, got %d", resp.QueuesPurged)
	}
	if !cancelCalled1 || !cancelCalled2 {
		t.Errorf("Expected Cancel() to be invoked on Artemis jobs")
	}
	if cancelCalled3 {
		t.Errorf("Expected Cancel() NOT to be invoked on OtherApp jobs")
	}

	// Ensure jobs were removed
	if _, ok := srv.activeJobs["job_artemis_1"]; ok {
		t.Errorf("Artemis job 1 was not removed from map")
	}
	if _, ok := srv.activeJobs["job_other_1"]; !ok {
		t.Errorf("OtherApp job 1 was mistakenly removed from map")
	}

	// 2. Edge Case: Unknown AppId
	req2 := &pb.PurgeSystemStateRequest{
		AppId:       "UnknownApp",
		PurgeQueues: false,
	}
	resp2, err := srv.PurgeSystemState(context.Background(), req2)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if resp2.JobsCancelled != 0 {
		t.Errorf("Expected 0 jobs cancelled for unknown app, got %d", resp2.JobsCancelled)
	}

	// 3. Validation Error
	req3 := &pb.PurgeSystemStateRequest{AppId: ""}
	_, err = srv.PurgeSystemState(context.Background(), req3)
	if err == nil {
		t.Errorf("Expected validation error for missing app_id")
	}
}

func TestUpdateExecutionAndMarshal(t *testing.T) {
	srv := &OrchestratorServer{}
	tracker := &JobTracker{
		State: &pb.WorkflowContext{
			JobId: "test_job_123",
		},
		CallbackAddress: "http://localhost:5000",
	}

	stepReq := &pb.SubmitStepRequest{
		JobId:  "test_job_123",
		StepId: "step_456",
	}

	payloadBytes, err := srv.updateExecutionAndMarshal(tracker, stepReq)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if payloadBytes == nil || len(payloadBytes) == 0 {
		t.Fatalf("Expected non-empty payload")
	}

	if tracker.State.ActiveExecution == nil {
		t.Fatalf("ActiveExecution should have been created")
	}
	if tracker.State.ActiveExecution.JobId != "test_job_123" {
		t.Errorf("Expected JobId test_job_123, got %s", tracker.State.ActiveExecution.JobId)
	}
	if tracker.State.ActiveExecution.CurrentStepId != "step_456" {
		t.Errorf("Expected CurrentStepId step_456, got %s", tracker.State.ActiveExecution.CurrentStepId)
	}
	if tracker.StepReq != stepReq {
		t.Errorf("Expected tracker to store the stepReq")
	}

	// Check if JSON unmarshals
	payloadStr := string(payloadBytes)
	if !strings.Contains(payloadStr, "state_bytes") || !strings.Contains(payloadStr, "step_req_bytes") || !strings.Contains(payloadStr, "callback_address") {
		t.Errorf("Payload missing expected JSON keys: %s", payloadStr)
	}
}
