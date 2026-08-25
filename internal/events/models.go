package events

import (
	"encoding/json"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
)

// EventType defines the type of event being published to JetStream
type EventType string

const (
	EventChunkTrace           EventType = "chunk.trace"

    EventActionCompleted           EventType = "action.completed"
)

// TraceabilityEvent is the core wrapper for all compliance and tracing data.
// This is exactly what the .NET Traceability Service will listen for.
type TraceabilityEvent struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`

	// Compliance & Security Metadata
	Identity  *pb.InfrastructureIdentity `json:"identity,omitempty"`
	GraphID         string                     `json:"graph_id,omitempty"`
	InitialNodeID   string                     `json:"initial_node_id,omitempty"`

	Stage     string `json:"stage,omitempty"`
	JobAction string `json:"job_action,omitempty"`

	// Specific Payload Details (Optional based on EventType)
	ChunkID             string            `json:"chunk_id,omitempty"`
	ChunkIndex          int               `json:"chunk_index,omitempty"`
	Message             string            `json:"message,omitempty"`
	ErrorContract       *pb.ErrorContract `json:"error_contract,omitempty"`
	Metadata            json.RawMessage   `json:"metadata,omitempty"`
	NodeOutput          json.RawMessage   `json:"node_output,omitempty"`
	RequestSnapshotJson string            `json:"request_snapshot_json,omitempty"`
	ExtractedData       string            `json:"extracted_data,omitempty"` // Only populated on completion if needed
}

// WebhookEvent holds the payload necessary for the Orchestrator to securely relay callbacks
// via a resilient JetStream retry queue without storing raw byte slices or duplicate string IDs.
type WebhookEvent struct {
	Request       *pb.ExecuteTaskRequest `json:"request,omitempty"`
	ErrorContract *pb.ErrorContract              `json:"error_contract,omitempty"`
	IsSuspended   bool                           `json:"is_suspended"`
}
