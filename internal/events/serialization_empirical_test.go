package events

import (
	"encoding/json"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
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
		GraphID: "00000000-0000-0000-0000-000000000001",
		InitialNodeID: "00000000-0000-0000-0000-000000000002",
		Stage:     "execution",
		JobAction: "ai_inference",
		ChunkID:   "chunk-1",
		ErrorContract: &pb.ErrorContract{
			Code:    "ERR_001",
			Message: "Something failed",
			Source:  "ai_worker",
		},
		Metadata:       json.RawMessage(`{"key1": "val1"}`),
		NodeOutput:     json.RawMessage(`{"result": "ok"}`),
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
	if unmarshaled.GraphID != original.GraphID {
		t.Errorf("GraphID mismatch: expected %s, got %s", original.GraphID, unmarshaled.GraphID)
	}
	if unmarshaled.InitialNodeID != original.InitialNodeID {
		t.Errorf("InitialNodeID mismatch: expected %s, got %s", original.InitialNodeID, unmarshaled.InitialNodeID)
	}
	if unmarshaled.ErrorContract == nil {
		t.Fatalf("ErrorContract is nil after unmarshaling!")
	}
	if unmarshaled.ErrorContract.GetCode() != "ERR_001" {
		t.Errorf("ErrorContract.Code mismatch: expected 'ERR_001', got %q", unmarshaled.ErrorContract.GetCode())
	}
	var nodeOutput map[string]any
	if err := json.Unmarshal(unmarshaled.NodeOutput, &nodeOutput); err != nil {
		t.Fatalf("Failed to unmarshal NodeOutput: %v", err)
	}
	if val, ok := nodeOutput["result"]; !ok || val != "ok" {
		t.Errorf("NodeOutput mismatch: expected result: ok, got %+v", nodeOutput)
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
	if len(unmarshaled.NodeOutput) != 0 {
		t.Errorf("Expected empty NodeOutput, got %+v", unmarshaled.NodeOutput)
	}
}

func TestWebhookEvent_JSONRoundTrip(t *testing.T) {
	original := WebhookEvent{
		Request: &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant-X",
				AppId:    "app-Y",
				JobId:    "job-Z",
			},
			StepId:      "step-42",
			TaskSubject: "worker.ai.inference",
			Payload: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"status": structpb.NewStringValue("completed"),
				},
			},
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
	if val := unmarshaled.Request.Payload.Fields["status"].GetStringValue(); val != "completed" {
		t.Errorf("Payload mismatch: expected %q, got %q", "completed", val)
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
