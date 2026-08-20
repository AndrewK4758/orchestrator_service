package validation

import (
	"time"
)

// FieldValidationResult records the outcome of a single field comparison across Tier 1, 2, or 3.
type FieldValidationResult struct {
	FieldKey   string  `json:"field_key"`
	FieldType  string  `json:"field_type"`
	DbValue    string  `json:"db_value"`
	DocValue   string  `json:"doc_value"`
	TierUsed   int     `json:"tier_used"`
	Algorithm  string  `json:"algorithm"`
	Score      float64 `json:"score"`
	IsMatch    bool    `json:"is_match"`
	Confidence float64 `json:"confidence"`
	Rationale  string  `json:"rationale,omitempty"`
	LatencyMs  int64   `json:"latency_ms"`
}

// DocumentValidationReport aggregates field validation results for a document workflow payload.
type DocumentValidationReport struct {
	OverallStatus    string                            `json:"overall_status"`
	TotalFields      int                               `json:"total_fields"`
	MatchedFields    int                               `json:"matched_fields"`
	MismatchedFields int                               `json:"mismatched_fields"`
	Tier1Count       int                               `json:"tier1_count"`
	Tier2Count       int                               `json:"tier2_count"`
	Tier3Count       int                               `json:"tier3_count"`
	FieldResults     map[string]*FieldValidationResult `json:"field_results"`
}

// DecisionDriftEvent represents a telemetry event emitted when Tier 3 fallback resolutions spike.
type DecisionDriftEvent struct {
	TenantID            string    `json:"tenant_id"`
	AppID               string    `json:"app_id"`
	JobID               string    `json:"job_id"`
	EventType           string    `json:"event_type"`
	WindowSize          int       `json:"window_size"`
	Tier3Count          int       `json:"tier3_count"`
	Tier3Percentage     float64   `json:"tier3_percentage"`
	ThresholdPercentage float64   `json:"threshold_percentage"`
	Timestamp           time.Time `json:"timestamp"`
}
