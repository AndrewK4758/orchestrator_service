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

	// Exact string equality check (case-insensitive, trimmed)
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

	// SHA-256 fingerprint match
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

	// 1. Currency & Financial amounts
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
		// If delta > 0 for currency, Tier 2 deterministically declares mismatch (no LLM ambiguity for numbers)
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

	// 2. Dates & Timestamps
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
		// Date mismatch
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

	// 3. Strict Identifiers (Tax ID, SSN, EIN, Loan Number)
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

	// 4. Physical / Property Addresses
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
		// If address score is low, let Tier 3 evaluate if there's complex unit/suite or developer naming variation
		return nil, false
	}

	// 5. Names & General Text
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
