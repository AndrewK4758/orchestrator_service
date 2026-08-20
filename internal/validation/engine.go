package validation

import (
	"context"
	"fmt"
)

// FieldPair encapsulates the database value, document value, and domain type for a field.
type FieldPair struct {
	FieldType string `json:"field_type"`
	DbValue   string `json:"db_value"`
	DocValue  string `json:"doc_value"`
}

// CascadeEngine orchestrates the 3-Tier Cascading Semantic Validation Engine.
type CascadeEngine struct {
	arbiter      LLMArbiter
	driftTracker *DriftTracker
}

// NewCascadeEngine constructs a new CascadeEngine instance.
func NewCascadeEngine(arbiter LLMArbiter, pub JetStreamPublisher) *CascadeEngine {
	return &CascadeEngine{
		arbiter:      arbiter,
		driftTracker: NewDriftTracker(pub),
	}
}

// ValidateField validates a single field pair across Tier 1 -> Tier 2 -> Tier 3.
func (e *CascadeEngine) ValidateField(ctx context.Context, fieldKey, fieldType, dbVal, docVal string, contextInfo map[string]any) (*FieldValidationResult, error) {
	// Tier 1: Exact / SHA-256 Match
	if res, ok := EvaluateTier1(fieldKey, fieldType, dbVal, docVal); ok {
		return res, nil
	}

	// Tier 2: Deterministic Distance & Metric Evaluators
	if res, ok := EvaluateTier2(fieldKey, fieldType, dbVal, docVal); ok {
		return res, nil
	}

	// Tier 3: LLM Semantic Arbiter
	if e.arbiter != nil {
		return e.arbiter.ArbitrateField(ctx, fieldKey, fieldType, dbVal, docVal, contextInfo)
	}

	// Fallback if arbiter not provided
	return &FieldValidationResult{
		FieldKey:   fieldKey,
		FieldType:  fieldType,
		DbValue:    dbVal,
		DocValue:   docVal,
		TierUsed:   2,
		Algorithm:  "NO_MATCH_TIER2",
		Score:      0.0,
		IsMatch:    false,
		Confidence: 1.0,
		Rationale:  "Tier 1 & Tier 2 failed, LLM Arbiter unconfigured",
		LatencyMs:  0,
	}, nil
}

// ValidateDocument validates all field pairs for a document and compiles the DocumentValidationReport.
func (e *CascadeEngine) ValidateDocument(ctx context.Context, tenantID, appID, jobID string, fields map[string]FieldPair, contextInfo map[string]any) (*DocumentValidationReport, error) {
	// Absolute Fail Fast: Panic immediately on missing identity
	if tenantID == "" || appID == "" || jobID == "" {
		panic(fmt.Sprintf("action failed: invalid identity - tenantID=%q, appID=%q, jobID=%q", tenantID, appID, jobID))
	}

	report := &DocumentValidationReport{
		OverallStatus:    "PASSED",
		TotalFields:      len(fields),
		MatchedFields:    0,
		MismatchedFields: 0,
		Tier1Count:       0,
		Tier2Count:       0,
		Tier3Count:       0,
		FieldResults:     make(map[string]*FieldValidationResult, len(fields)),
	}

	var observedTiers []int

	for fieldKey, pair := range fields {
		res, err := e.ValidateField(ctx, fieldKey, pair.FieldType, pair.DbValue, pair.DocValue, contextInfo)
		if err != nil {
			// Record error result
			res = &FieldValidationResult{
				FieldKey:   fieldKey,
				FieldType:  pair.FieldType,
				DbValue:    pair.DbValue,
				DocValue:   pair.DocValue,
				TierUsed:   3,
				Algorithm:  "VALIDATION_ERROR",
				Score:      0.0,
				IsMatch:    false,
				Confidence: 0.0,
				Rationale:  err.Error(),
			}
		}

		report.FieldResults[fieldKey] = res
		observedTiers = append(observedTiers, res.TierUsed)

		if res.IsMatch {
			report.MatchedFields++
		} else {
			report.MismatchedFields++
		}

		switch res.TierUsed {
		case 1:
			report.Tier1Count++
		case 2:
			report.Tier2Count++
		case 3:
			report.Tier3Count++
		}
	}

	if report.MismatchedFields > 0 {
		report.OverallStatus = "REVIEW_REQUIRED"
	}

	// Track decision drift telemetry
	if e.driftTracker != nil && len(observedTiers) > 0 {
		_, _ = e.driftTracker.RecordAndCheck(ctx, tenantID, appID, jobID, observedTiers)
	}

	return report, nil
}
