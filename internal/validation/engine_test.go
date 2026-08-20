package validation

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

// mockJetStream for testing drift event emission
type mockJetStream struct {
	publishedSubjects []string
	publishedPayloads [][]byte
}

func (m *mockJetStream) Publish(ctx context.Context, subj string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	m.publishedSubjects = append(m.publishedSubjects, subj)
	m.publishedPayloads = append(m.publishedPayloads, data)
	return &jetstream.PubAck{Sequence: 1}, nil
}

func TestCascadeEngine_Tier1_ExactBypass(t *testing.T) {
	arbiterCalled := false
	mockClient := func(ctx context.Context, prompt string, temperature float64) (string, error) {
		arbiterCalled = true
		return `{"is_match": true, "confidence": 1.0}`, nil
	}

	arbiter := NewDefaultLLMArbiter(mockClient)
	engine := NewCascadeEngine(arbiter, nil)

	ctx := context.Background()

	// Identical strings
	res, err := engine.ValidateField(ctx, "borrower_name", "text", "Jane Smith", "Jane Smith", nil)
	assert.NoError(t, err)
	assert.True(t, res.IsMatch)
	assert.Equal(t, 1, res.TierUsed)
	assert.Equal(t, "EXACT_STRING", res.Algorithm)
	assert.False(t, arbiterCalled)

	// Identical dates
	resDate, err := engine.ValidateField(ctx, "closing_date", "date", "2026-03-15", "2026-03-15", nil)
	assert.NoError(t, err)
	assert.True(t, resDate.IsMatch)
	assert.Equal(t, 1, resDate.TierUsed)
	assert.False(t, arbiterCalled)
}

func TestCascadeEngine_Tier2_DomainEvaluators(t *testing.T) {
	arbiterCalled := false
	mockClient := func(ctx context.Context, prompt string, temperature float64) (string, error) {
		arbiterCalled = true
		return `{"is_match": true, "confidence": 1.0}`, nil
	}

	arbiter := NewDefaultLLMArbiter(mockClient)
	engine := NewCascadeEngine(arbiter, nil)
	ctx := context.Background()

	// 1. Name typo / middle initial via Jaro-Winkler
	resName, err := engine.ValidateField(ctx, "borrower_name", "name", "John Doe", "John A. Doe", nil)
	assert.NoError(t, err)
	assert.True(t, resName.IsMatch)
	assert.Equal(t, 2, resName.TierUsed)
	assert.Equal(t, "JARO_WINKLER", resName.Algorithm)
	assert.False(t, arbiterCalled)

	// 2. Address CASS / Jaccard
	resAddr, err := engine.ValidateField(ctx, "property_address", "address", "123 North Main Street, Suite 400, Atlanta, GA 30303", "123 n main st ste 400, atlanta ga 30303", nil)
	assert.NoError(t, err)
	assert.True(t, resAddr.IsMatch)
	assert.Equal(t, 2, resAddr.TierUsed)
	assert.Equal(t, "ADDRESS_CASS_JACCARD", resAddr.Algorithm)
	assert.False(t, arbiterCalled)

	// 3. Currency exact cents
	resCurr, err := engine.ValidateField(ctx, "loan_amount", "currency", "$450,000.00", "450000.00", nil)
	assert.NoError(t, err)
	assert.True(t, resCurr.IsMatch)
	assert.Equal(t, 2, resCurr.TierUsed)
	assert.Equal(t, "CURRENCY_EXACT_CENTS", resCurr.Algorithm)
	assert.False(t, arbiterCalled)

	// 4. Currency delta mismatch (deterministic rejection at Tier 2)
	resCurrMismatch, err := engine.ValidateField(ctx, "loan_amount", "currency", "$450,000.00", "$450,050.00", nil)
	assert.NoError(t, err)
	assert.False(t, resCurrMismatch.IsMatch)
	assert.Equal(t, 2, resCurrMismatch.TierUsed)
	assert.Equal(t, "CURRENCY_DELTA", resCurrMismatch.Algorithm)
	assert.False(t, arbiterCalled)

	// 5. Date multi-format parsing
	resDate, err := engine.ValidateField(ctx, "closing_date", "date", "March 15, 2026", "2026-03-15", nil)
	assert.NoError(t, err)
	assert.True(t, resDate.IsMatch)
	assert.Equal(t, 2, resDate.TierUsed)
	assert.Equal(t, "DATE_EXACT_SECONDS", resDate.Algorithm)
	assert.False(t, arbiterCalled)

	// 6. Identifier sanitized matching
	resSSN, err := engine.ValidateField(ctx, "ssn", "id", "123-45-6789", "123456789", nil)
	assert.NoError(t, err)
	assert.True(t, resSSN.IsMatch)
	assert.Equal(t, 2, resSSN.TierUsed)
	assert.Equal(t, "IDENTIFIER_SANITIZED_EXACT", resSSN.Algorithm)
	assert.False(t, arbiterCalled)
}

func TestCascadeEngine_Tier3_LLMArbiter(t *testing.T) {
	t.Setenv("VALIDATION_LLM_CONFIDENCE_THRESHOLD", "0.85")

	tests := []struct {
		name          string
		clientResp    string
		expectedMatch bool
		expectedScore float64
	}{
		{
			name:          "High confidence semantic match accepted",
			clientResp:    `{"is_match": true, "confidence": 0.95, "rationale": "First Amer is standard abbreviation for First American", "resolved_canonical_value": "First American Title"}`,
			expectedMatch: true,
			expectedScore: 0.95,
		},
		{
			name:          "Low confidence match rejected",
			clientResp:    `{"is_match": true, "confidence": 0.70, "rationale": "Likely different entities"}`,
			expectedMatch: false,
			expectedScore: 0.70,
		},
		{
			name:          "Explicit semantic mismatch",
			clientResp:    `{"is_match": false, "confidence": 0.98, "rationale": "Completely unrelated entities"}`,
			expectedMatch: false,
			expectedScore: 0.98,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := func(ctx context.Context, prompt string, temperature float64) (string, error) {
				return tt.clientResp, nil
			}

			arbiter := NewDefaultLLMArbiter(mockClient)
			engine := NewCascadeEngine(arbiter, nil)

			res, err := engine.ValidateField(context.Background(), "settlement_agent", "text", "First American Title Insurance Company", "Acme Title Corp", nil)
			assert.NoError(t, err)
			assert.Equal(t, 3, res.TierUsed)
			assert.Equal(t, tt.expectedMatch, res.IsMatch)
			assert.Equal(t, tt.expectedScore, res.Score)
		})
	}
}

func TestCascadeEngine_ValidateDocument_ReportCompilation(t *testing.T) {
	mockClient := func(ctx context.Context, prompt string, temperature float64) (string, error) {
		return `{"is_match": true, "confidence": 0.95, "rationale": "Semantic alias match"}`, nil
	}

	arbiter := NewDefaultLLMArbiter(mockClient)
	engine := NewCascadeEngine(arbiter, nil)

	fields := map[string]FieldPair{
		"borrower_name":    {FieldType: "name", DbValue: "Jane Doe", DocValue: "Jane Doe"},                                                        // Tier 1
		"property_address": {FieldType: "address", DbValue: "123 N Main St, Atlanta GA 30303", DocValue: "123 North Main Street, Atlanta, GA 30303"}, // Tier 2
		"closing_agent":    {FieldType: "text", DbValue: "First American Title", DocValue: "First Amer. Title Ins. Co."},                          // Tier 3
	}

	report, err := engine.ValidateDocument(context.Background(), "tenant-001", "app-artemis", "job-999", fields, nil)
	assert.NoError(t, err)
	assert.Equal(t, "PASSED", report.OverallStatus)
	assert.Equal(t, 3, report.TotalFields)
	assert.Equal(t, 3, report.MatchedFields)
	assert.Equal(t, 0, report.MismatchedFields)
	assert.Equal(t, 1, report.Tier1Count)
	assert.Equal(t, 1, report.Tier2Count)
	assert.Equal(t, 1, report.Tier3Count)
}

func TestCascadeEngine_DecisionDriftTelemetry(t *testing.T) {
	t.Setenv("VALIDATION_DRIFT_WINDOW_SIZE", "10")
	t.Setenv("VALIDATION_DRIFT_TIER3_THRESHOLD_PERCENT", "20.0")

	mockJS := &mockJetStream{}
	tracker := NewDriftTracker(mockJS)

	ctx := context.Background()

	// Record 10 items with 3 Tier-3 calls (30% > 20% threshold)
	tiers := []int{1, 1, 2, 2, 1, 2, 1, 3, 3, 3}
	event, err := tracker.RecordAndCheck(ctx, "tenant-test", "app-test", "job-001", tiers)

	assert.NoError(t, err)
	assert.NotNil(t, event)
	assert.Equal(t, "DECISION_DRIFT", event.EventType)
	assert.Equal(t, 30.0, event.Tier3Percentage)
	assert.Equal(t, 1, len(mockJS.publishedSubjects))
	assert.Equal(t, "trace.orchestrator.tenant-test.app-test", mockJS.publishedSubjects[0])
}

func TestCascadeEngine_FailFast_MissingIdentity(t *testing.T) {
	engine := NewCascadeEngine(nil, nil)

	assert.Panics(t, func() {
		_, _ = engine.ValidateDocument(context.Background(), "", "app-001", "job-001", nil, nil)
	}, "must panic on missing tenantID")

	assert.Panics(t, func() {
		_, _ = engine.ValidateDocument(context.Background(), "tenant-001", "", "job-001", nil, nil)
	}, "must panic on missing appID")

	assert.Panics(t, func() {
		_, _ = engine.ValidateDocument(context.Background(), "tenant-001", "app-001", "", nil, nil)
	}, "must panic on missing jobID")
}
