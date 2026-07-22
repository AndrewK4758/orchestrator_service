package events

import "time"

// EventType defines the type of event being published to JetStream
type EventType string

const (
	EventChunkTrace           EventType = "chunk.trace"
	EventJobStepFailed        EventType = "step.failed"
	EventJobStepPendingHuman  EventType = "step.pending_human"
	EventStepValidationFailed EventType = "step.validation_failed"

	EventPlanApproved              EventType = "plan.approved"
	EventPlanRejectedRetry         EventType = "plan.rejected_retry"
	EventStepValidationFailedRetry EventType = "step.validation_failed_retry"
	EventActionStarted             EventType = "action.started"
	EventActionCompleted           EventType = "action.completed"
	EventActionFailed              EventType = "action.failed"
)

// TraceabilityEvent is the core wrapper for all compliance and tracing data.
// This is exactly what the .NET Traceability Service will listen for.
type TraceabilityEvent struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`

	// Compliance & Security Metadata (Crucial for Title/Closing Attorneys)
	TenantID string `json:"tenant_id,omitempty"` // Identifies the specific law firm / client
	AppID    string `json:"app_id,omitempty"`    // e.g., "DotNetWebClient"

	// Job Context
	JobID string `json:"job_id"`

	Stage     string `json:"stage,omitempty"`
	JobAction string `json:"job_action,omitempty"`

	// Specific Payload Details (Optional based on EventType)
	ChunkID       string         `json:"chunk_id,omitempty"`
	ChunkIndex    int            `json:"chunk_index,omitempty"`
	Message       string         `json:"message,omitempty"`
	Error         string         `json:"error,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	ExtractedData string         `json:"extracted_data,omitempty"` // Only populated on completion if needed
}

// WebhookEvent holds the payload necessary for the Orchestrator to securely relay callbacks
// via a resilient JetStream retry queue without storing Protobuf references directly.
type WebhookEvent struct {
	ReqBytes           []byte `json:"req_bytes"`
	StepOutputJSON     string `json:"step_output_json"`
	ErrorContractBytes []byte `json:"error_contract_bytes"`
	LastStepId         string `json:"last_step_id"`
	IsSuspended        bool   `json:"is_suspended"`
}
