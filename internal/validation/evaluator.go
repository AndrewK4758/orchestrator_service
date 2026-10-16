package validation

import (
	"strings"
	"time"

	"github.com/AndrewK4758/shared_utils/canonical"
)

// EvaluateTier1 performs immediate normalized exact matching and SHA-256 fingerprint comparison.
func EvaluateTier1(fieldKey, fieldType, dbVal, docVal string) (*FieldValidationResult, bool) {
	s1 := strings.TrimSpace(dbVal)
	s2 := strings.TrimSpace(docVal)

	if s1 == "" && s2 == "" {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   1,
			Algorithm:  "EXACT_EMPTY",
			Score:      1.0,
			IsMatch:    true,
			Confidence: 1.0,
			LatencyMs:  0,
		}, true
	}

	if s1 == "" || s2 == "" {
		return nil, false
	}

	if strings.EqualFold(s1, s2) {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   1,
			Algorithm:  "EXACT_STRING",
			Score:      1.0,
			IsMatch:    true,
			Confidence: 1.0,
			LatencyMs:  0,
		}, true
	}

	if canonical.Fingerprint(s1) == canonical.Fingerprint(s2) {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   1,
			Algorithm:  "EXACT_SHA256",
			Score:      1.0,
			IsMatch:    true,
			Confidence: 1.0,
			LatencyMs:  0,
		}, true
	}

	return nil, false
}

// EvaluateTier2 performs deterministic domain-specific distance and rule evaluation.
func EvaluateTier2(fieldKey, fieldType, dbVal, docVal string) (*FieldValidationResult, bool) {
	start := time.Now()
	s1 := strings.TrimSpace(dbVal)
	s2 := strings.TrimSpace(docVal)

	if s1 == "" || s2 == "" {
		return nil, false
	}

	typeLower := strings.ToLower(strings.TrimSpace(fieldType))
	keyLower := strings.ToLower(strings.TrimSpace(fieldKey))

	if typeLower == "currency" || strings.Contains(keyLower, "amount") || strings.Contains(keyLower, "fee") || strings.Contains(keyLower, "price") || strings.Contains(keyLower, "balance") {
		matched, delta := canonical.MatchCurrencies(s1, s2)
		if matched && delta == 0 {
			return &FieldValidationResult{
				FieldKey:   fieldKey,
				FieldType:  fieldType,
				DbValue:    dbVal,
				DocValue:   docVal,
				TierUsed:   2,
				Algorithm:  "CURRENCY_EXACT_CENTS",
				Score:      1.0,
				IsMatch:    true,
				Confidence: 1.0,
				LatencyMs:  time.Since(start).Milliseconds(),
			}, true
		}
		// Numeric currency mismatches are rejected directly without falling back to the LLM.
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   2,
			Algorithm:  "CURRENCY_DELTA",
			Score:      0.0,
			IsMatch:    false,
			Confidence: 1.0,
			LatencyMs:  time.Since(start).Milliseconds(),
		}, true
	}

	if typeLower == "date" || strings.Contains(keyLower, "date") || strings.Contains(keyLower, "dob") || strings.Contains(keyLower, "timestamp") {
		matched, delta := canonical.MatchDates(s1, s2)
		if matched && delta == 0 {
			return &FieldValidationResult{
				FieldKey:   fieldKey,
				FieldType:  fieldType,
				DbValue:    dbVal,
				DocValue:   docVal,
				TierUsed:   2,
				Algorithm:  "DATE_EXACT_SECONDS",
				Score:      1.0,
				IsMatch:    true,
				Confidence: 1.0,
				LatencyMs:  time.Since(start).Milliseconds(),
			}, true
		}
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   2,
			Algorithm:  "DATE_DELTA",
			Score:      0.0,
			IsMatch:    false,
			Confidence: 1.0,
			LatencyMs:  time.Since(start).Milliseconds(),
		}, true
	}

	if typeLower == "id" || strings.Contains(keyLower, "ssn") || strings.Contains(keyLower, "tax_id") || strings.Contains(keyLower, "tin") || strings.Contains(keyLower, "ein") || strings.Contains(keyLower, "loan_number") {
		matched, score := canonical.MatchIdentifiers(s1, s2)
		if matched {
			return &FieldValidationResult{
				FieldKey:   fieldKey,
				FieldType:  fieldType,
				DbValue:    dbVal,
				DocValue:   docVal,
				TierUsed:   2,
				Algorithm:  "IDENTIFIER_SANITIZED_EXACT",
				Score:      score,
				IsMatch:    true,
				Confidence: 1.0,
				LatencyMs:  time.Since(start).Milliseconds(),
			}, true
		}
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   2,
			Algorithm:  "IDENTIFIER_MISMATCH",
			Score:      0.0,
			IsMatch:    false,
			Confidence: 1.0,
			LatencyMs:  time.Since(start).Milliseconds(),
		}, true
	}

	if typeLower == "address" || strings.Contains(keyLower, "address") || strings.Contains(keyLower, "street") || strings.Contains(keyLower, "property") {
		matched, score := canonical.MatchAddresses(s1, s2)
		if matched {
			return &FieldValidationResult{
				FieldKey:   fieldKey,
				FieldType:  fieldType,
				DbValue:    dbVal,
				DocValue:   docVal,
				TierUsed:   2,
				Algorithm:  "ADDRESS_CASS_JACCARD",
				Score:      score,
				IsMatch:    true,
				Confidence: score,
				LatencyMs:  time.Since(start).Milliseconds(),
			}, true
		}
		// Fall through to Tier 3 to evaluate complex unit/suite or naming variations.
		return nil, false
	}

	matched, score := canonical.MatchNames(s1, s2)
	if matched {
		return &FieldValidationResult{
			FieldKey:   fieldKey,
			FieldType:  fieldType,
			DbValue:    dbVal,
			DocValue:   docVal,
			TierUsed:   2,
			Algorithm:  "JARO_WINKLER",
			Score:      score,
			IsMatch:    true,
			Confidence: score,
			LatencyMs:  time.Since(start).Milliseconds(),
		}, true
	}

	return nil, false
}
