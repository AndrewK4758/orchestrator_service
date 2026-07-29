package events

import (
	"encoding/json"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
)

func TestTraceabilityEvent_JSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	original := TraceabilityEvent{
		EventID:   "evt-123",
		EventType: EventActionCompleted,
		Timestamp: now,
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant-A",
			AppId:    "app-B",
			JobId:    "job-C",
		},
		JobNameID: 42,
		Stage:     "execution",
		JobAction: "ai_inference",
		ChunkID:   "chunk-1",
		ErrorContract: &pb.ErrorContract{
			Code:    "ERR_001",
			Message: "Something failed",
			Source:  "ai_worker",
		},
		Metadata:       map[string]any{"key1": "val1"},
		NodeOutputJson: `{"result":"ok"}`,
		ExtractedData:  "sample extracted data",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed for TraceabilityEvent: %v", err)
	}

	t.Logf("Verbatim TraceabilityEvent JSON:\n%s\n", string(data))

	var unmarshaled TraceabilityEvent
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal failed for TraceabilityEvent: %v", err)
	}

	if unmarshaled.EventID != original.EventID {
		t.Errorf("EventID mismatch: expected %q, got %q", original.EventID, unmarshaled.EventID)
	}
	if unmarshaled.EventType != original.EventType {
		t.Errorf("EventType mismatch: expected %q, got %q", original.EventType, unmarshaled.EventType)
	}
	if unmarshaled.Identity == nil {
		t.Fatalf("Identity is nil after unmarshaling!")
	}
	if unmarshaled.Identity.GetTenantId() != "tenant-A" {
		t.Errorf("Identity.TenantId mismatch: expected 'tenant-A', got %q", unmarshaled.Identity.GetTenantId())
	}
	if unmarshaled.Identity.GetAppId() != "app-B" {
		t.Errorf("Identity.AppId mismatch: expected 'app-B', got %q", unmarshaled.Identity.GetAppId())
	}
	if unmarshaled.Identity.GetJobId() != "job-C" {
		t.Errorf("Identity.JobId mismatch: expected 'job-C', got %q", unmarshaled.Identity.GetJobId())
	}
	if unmarshaled.JobNameID != original.JobNameID {
		t.Errorf("JobNameID mismatch: expected %d, got %d", original.JobNameID, unmarshaled.JobNameID)
	}
	if unmarshaled.ErrorContract == nil {
		t.Fatalf("ErrorContract is nil after unmarshaling!")
	}
	if unmarshaled.ErrorContract.GetCode() != "ERR_001" {
		t.Errorf("ErrorContract.Code mismatch: expected 'ERR_001', got %q", unmarshaled.ErrorContract.GetCode())
	}
	if unmarshaled.NodeOutputJson != `{"result":"ok"}` {
		t.Errorf("NodeOutputJson mismatch: expected %q, got %q", `{"result":"ok"}`, unmarshaled.NodeOutputJson)
	}
}

func TestTraceabilityEvent_NilFields(t *testing.T) {
	original := TraceabilityEvent{
		EventID:   "evt-minimal",
		EventType: EventChunkTrace,
		Timestamp: time.Now().UTC(),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed for minimal TraceabilityEvent: %v", err)
	}

	t.Logf("Verbatim Minimal TraceabilityEvent JSON:\n%s\n", string(data))

	var unmarshaled TraceabilityEvent
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if unmarshaled.Identity != nil {
		t.Errorf("Expected nil Identity, got %+v", unmarshaled.Identity)
	}
	if unmarshaled.ErrorContract != nil {
		t.Errorf("Expected nil ErrorContract, got %+v", unmarshaled.ErrorContract)
	}
	if unmarshaled.NodeOutputJson != "" {
		t.Errorf("Expected empty NodeOutputJson, got %+v", unmarshaled.NodeOutputJson)
	}
}

func TestWebhookEvent_JSONRoundTrip(t *testing.T) {
	original := WebhookEvent{
		Request: &pb.ExecuteWorkflowNodeRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant-X",
				AppId:    "app-Y",
				JobId:    "job-Z",
			},
			StepId: "step-42",
			NodeConfig: &pb.NodeConfiguration{
				TargetWorkerSubject: "worker.ai.inference",
				ExpectedSchema:      `{"type":"object"}`,
			},
			NodeOutputJson: `{"status":"completed"}`,
		},
		ErrorContract: &pb.ErrorContract{
			Code:    "ERR_WEBHOOK",
			Message: "Webhook notification error",
		},
		IsSuspended: true,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed for WebhookEvent: %v", err)
	}

	t.Logf("Verbatim WebhookEvent JSON:\n%s\n", string(data))

	var unmarshaled WebhookEvent
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal failed for WebhookEvent: %v", err)
	}

	if !unmarshaled.IsSuspended {
		t.Errorf("IsSuspended mismatch: expected true, got false")
	}
	if unmarshaled.Request == nil {
		t.Fatalf("Request is nil after unmarshaling!")
	}
	if unmarshaled.Request.GetStepId() != "step-42" {
		t.Errorf("Request.StepId mismatch: expected 'step-42', got %q", unmarshaled.Request.GetStepId())
	}
	if unmarshaled.Request.Identity == nil {
		t.Fatalf("Request.Identity is nil after unmarshaling!")
	}
	if unmarshaled.Request.Identity.GetTenantId() != "tenant-X" {
		t.Errorf("Request.Identity.TenantId mismatch: expected 'tenant-X', got %q", unmarshaled.Request.Identity.GetTenantId())
	}
	if unmarshaled.Request.Identity.GetAppId() != "app-Y" {
		t.Errorf("Request.Identity.AppId mismatch: expected 'app-Y', got %q", unmarshaled.Request.Identity.GetAppId())
	}
	if unmarshaled.Request.Identity.GetJobId() != "job-Z" {
		t.Errorf("Request.Identity.JobId mismatch: expected 'job-Z', got %q", unmarshaled.Request.Identity.GetJobId())
	}
	if unmarshaled.ErrorContract == nil {
		t.Fatalf("ErrorContract is nil after unmarshaling!")
	}
	if unmarshaled.ErrorContract.GetCode() != "ERR_WEBHOOK" {
		t.Errorf("ErrorContract.Code mismatch: expected 'ERR_WEBHOOK', got %q", unmarshaled.ErrorContract.GetCode())
	}
}

func TestWebhookEvent_NilFields(t *testing.T) {
	original := WebhookEvent{
		IsSuspended: false,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed for minimal WebhookEvent: %v", err)
	}

	t.Logf("Verbatim Minimal WebhookEvent JSON:\n%s\n", string(data))

	var unmarshaled WebhookEvent
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if unmarshaled.Request != nil {
		t.Errorf("Expected nil Request, got %+v", unmarshaled.Request)
	}
	if unmarshaled.ErrorContract != nil {
		t.Errorf("Expected nil ErrorContract, got %+v", unmarshaled.ErrorContract)
	}
}
