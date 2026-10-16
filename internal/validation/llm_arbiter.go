package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/jsonclean"
)

// LLMArbiter defines the interface for Tier 3 semantic arbitration.
type LLMArbiter interface {
	ArbitrateField(ctx context.Context, fieldKey, fieldType, dbVal, docVal string, contextInfo map[string]any) (*FieldValidationResult, error)
}

// LLMArbiterResponse defines the JSON schema returned by the LLM.
type LLMArbiterResponse struct {
	IsMatch                bool    `json:"is_match"`
	Confidence             float64 `json:"confidence"`
	Rationale              string  `json:"rationale"`
	ResolvedCanonicalValue *string `json:"resolved_canonical_value,omitempty"`
}

// InferenceClient is a function signature for invoking the LLM model.
type InferenceClient func(ctx context.Context, prompt string, temperature float64) (string, error)

// DefaultLLMArbiter implements Tier 3 semantic arbitration using an inference client.
type DefaultLLMArbiter struct {
	inferenceClient     InferenceClient
	temperature         float64
	confidenceThreshold float64
}

// NewDefaultLLMArbiter constructs an LLM Arbiter with environment-backed configurations.
func NewDefaultLLMArbiter(client InferenceClient) *DefaultLLMArbiter {
	temp := 0.0
	if envTemp := envutil.GetStringOrDefault("VALIDATION_LLM_TEMPERATURE", ""); envTemp != "" {
		if t, err := strconv.ParseFloat(envTemp, 64); err == nil && t >= 0 {
			temp = t
		}
	}

	thresh := 0.85
	if envThresh := envutil.GetStringOrDefault("VALIDATION_LLM_CONFIDENCE_THRESHOLD", ""); envThresh != "" {
		if t, err := strconv.ParseFloat(envThresh, 64); err == nil && t > 0 {
			thresh = t
		}
	}

	return &DefaultLLMArbiter{
		inferenceClient:     client,
		temperature:         temp,
		confidenceThreshold: thresh,
	}
}

// BuildPrompt formats the field comparison prompt for the LLM arbiter.
func (a *DefaultLLMArbiter) BuildPrompt(fieldKey, fieldType, dbVal, docVal string, contextInfo map[string]any) string {
	ctxBytes, _ := json.Marshal(contextInfo)
	return fmt.Sprintf(`You are a deterministic semantic entity arbiter for financial and legal document verification.
Compare the Document Value against the Database Record Value and determine if they refer to the exact same real-world entity, address, or item despite differences in abbreviation, formatting, or spelling.

Field Key: %s
Field Type: %s
Database Value: %s
Document Value: %s
Context Metadata: %s

Respond with ONLY a JSON object matching this exact schema:
{
  "is_match": boolean,
  "confidence": number between 0.0 and 1.0,
  "rationale": "concise explanation of semantic identity decision",
  "resolved_canonical_value": "standardized canonical form or null"
}`, fieldKey, fieldType, dbVal, docVal, string(ctxBytes))
}

// ArbitrateField executes the Tier 3 semantic comparison.
func (a *DefaultLLMArbiter) ArbitrateField(ctx context.Context, fieldKey, fieldType, dbVal, docVal string, contextInfo map[string]any) (*FieldValidationResult, error) {
	start := time.Now()
	if a.inferenceClient == nil {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   3,
			Algorithm:  "LLM_ARBITER_UNCONFIGURED",
			Score:      0.0,
			IsMatch:    false,
			Confidence: 0.0,
			Rationale:  "LLM inference client not configured",
			LatencyMs:  time.Since(start).Milliseconds(),
		}, nil
	}

	prompt := a.BuildPrompt(fieldKey, fieldType, dbVal, docVal, contextInfo)
	rawResp, err := a.inferenceClient(ctx, prompt, a.temperature)
	if err != nil {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   3,
			Algorithm:  "LLM_ARBITER_FAILED",
			Score:      0.0,
			IsMatch:    false,
			Confidence: 0.0,
			Rationale:  fmt.Sprintf("LLM call failed: %v", err),
			LatencyMs:  time.Since(start).Milliseconds(),
		}, err
	}

	cleaned := jsonclean.CleanJSON(rawResp)
	var arbiterResp LLMArbiterResponse
	if err := json.Unmarshal([]byte(cleaned), &arbiterResp); err != nil {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   3,
			Algorithm:  "LLM_ARBITER_INVALID_JSON",
			Score:      0.0,
			IsMatch:    false,
			Confidence: 0.0,
			Rationale:  fmt.Sprintf("Failed to parse arbiter response JSON: %v", err),
			LatencyMs:  time.Since(start).Milliseconds(),
		}, nil
	}

	isAcceptedMatch := arbiterResp.IsMatch && arbiterResp.Confidence >= a.confidenceThreshold

	return &FieldValidationResult{
		FieldKey:   fieldKey,
		FieldType:  fieldType,
		DbValue:    dbVal,
		DocValue:   docVal,
		TierUsed:   3,
		Algorithm:  "LLM_ARBITER",
		Score:      arbiterResp.Confidence,
		IsMatch:    isAcceptedMatch,
		Confidence: arbiterResp.Confidence,
		Rationale:  arbiterResp.Rationale,
		LatencyMs:  time.Since(start).Milliseconds(),
	}, nil
}
