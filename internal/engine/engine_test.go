package engine

import (
	"context"
	pb "doc_processor/shared_protos"
	"testing"
)

func TestEngine_UnsupportedAction(t *testing.T) {
	eng := NewEngine(nil, nil, nil)
	state := &ProcessingState{JobID: "job_123"}
	req := &pb.SubmitStepRequest{
		Steps: []*pb.WorkflowStep{
			{ActionType: "UNKNOWN_ACTION"},
		},
	}

	handlers, _ := eng.BuildRuntimeHandlers(nil)
	_, _, _, _, err := eng.ExecuteWorkflow(context.Background(), state, req, handlers)
	if err == nil {
		t.Fatalf("Expected error for unsupported action type, got nil")
	}
}

func TestProcessingState_SyncMetadata(t *testing.T) {
	tests := []struct {
		name                string
		globalState         map[string]interface{}
		expectedDocType     string
		expectedTenantID    string
		expectedUserID      string
		expectedAppID       string
		expectedCorrelation string
	}{
		{
			name: "Flat string values",
			globalState: map[string]interface{}{
				"document_type":  "Invoice",
				"tenant_id":      "tenant_abc",
				"user_id":        "user_123",
				"app_id":         "app_xyz",
				"correlation_id": "corr_999",
			},
			expectedDocType:     "Invoice",
			expectedTenantID:    "tenant_abc",
			expectedUserID:      "user_123",
			expectedAppID:       "app_xyz",
			expectedCorrelation: "corr_999",
		},
		{
			name: "PascalCase flat string values",
			globalState: map[string]interface{}{
				"DocumentType":  "Contract",
				"TenantID":      "tenant_pascal",
				"UserID":        "user_pascal",
				"AppID":         "app_pascal",
				"CorrelationID": "corr_pascal",
			},
			expectedDocType:     "Contract",
			expectedTenantID:    "tenant_pascal",
			expectedUserID:      "user_pascal",
			expectedAppID:       "app_pascal",
			expectedCorrelation: "corr_pascal",
		},
		{
			name: "Nested map structures like ClassificationInfo",
			globalState: map[string]interface{}{
				"ClassificationInfo": map[string]interface{}{
					"Classification": "Receipt",
				},
				"tenant_id": "nested_tenant",
			},
			expectedDocType:     "Receipt",
			expectedTenantID:    "nested_tenant",
			expectedUserID:      "",
			expectedAppID:       "",
			expectedCorrelation: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := &ProcessingState{
				GlobalState: tt.globalState,
			}
			ps.SyncMetadata()

			if ps.DocumentType != tt.expectedDocType {
				t.Errorf("expected DocumentType %q, got %q", tt.expectedDocType, ps.DocumentType)
			}
			if ps.TenantID != tt.expectedTenantID {
				t.Errorf("expected TenantID %q, got %q", tt.expectedTenantID, ps.TenantID)
			}
			if ps.UserID != tt.expectedUserID {
				t.Errorf("expected UserID %q, got %q", tt.expectedUserID, ps.UserID)
			}
			if ps.AppID != tt.expectedAppID {
				t.Errorf("expected AppID %q, got %q", tt.expectedAppID, ps.AppID)
			}
			if ps.CorrelationID != tt.expectedCorrelation {
				t.Errorf("expected CorrelationID %q, got %q", tt.expectedCorrelation, ps.CorrelationID)
			}
		})
	}
}
