package events

import "time"

// EventType defines the type of event being published to JetStream
type EventType string

const (
	EventJobStarted   EventType = "job.started"
	EventJobCompleted EventType = "job.completed"
	EventJobFailed    EventType = "job.failed"

	EventChunkStarted   EventType = "chunk.started"
	EventChunkCompleted EventType = "chunk.completed"
	EventChunkFailed    EventType = "chunk.failed"
	EventChunkTrace     EventType = "chunk.trace"

	EventJobStepStarted       EventType = "step.started"
	EventJobStepCompleted     EventType = "step.completed"
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
	UserID   string `json:"user_id,omitempty"`   // Identifies the specific user who uploaded
	AppID    string `json:"app_id,omitempty"`    // e.g., "DotNetWebClient"

	// Job Context
	JobID         string `json:"job_id"`
	DocumentType  string `json:"document_type,omitempty"` // e.g. "Attorney Closing Statement"
	CorrelationID string `json:"correlation_id,omitempty"`

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
