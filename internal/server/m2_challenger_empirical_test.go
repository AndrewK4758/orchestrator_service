package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"orchestrator_service/internal/events"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
)

// TestM2_Challenger_EmpiricalPanicVerification verifies that missing or malformed required inputs trigger panics.
func TestM2_Challenger_EmpiricalPanicVerification(t *testing.T) {
	srv := &OrchestratorServer{
		webhookConns: make(map[string]*clientConnEntry),
		grpcMaxMsgMb: 4,
		grpcTimeout:  30,
	}

	t.Run("ExecuteTask_NilRequest_Panics", func(t *testing.T) {
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), nil)
		})
	})

	t.Run("ExecuteTask_MissingIdentity_Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			TaskSubject: "worker.test",
			StepId:      "step-1",
		}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ExecuteTask_IncompleteIdentity_Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant-1",
			},
			TaskSubject: "worker.test",
			StepId:      "step-1",
		}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ExecuteTask_MissingTaskSubject_Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant-1",
				AppId:    "app-1",
				JobId:    "job-1",
			},
			StepId:          "step-1",
			CallbackAddress: "localhost:1234",
		}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ExecuteTask_MissingCallbackAddress_Panics", func(t *testing.T) {
		req := &pb.ExecuteTaskRequest{
			Identity: &pb.InfrastructureIdentity{
				TenantId: "tenant-1",
				AppId:    "app-1",
				JobId:    "job-1",
			},
			TaskSubject: "worker.test",
			StepId:      "step-1",
		}
		assert.Panics(t, func() {
			_, _ = srv.ExecuteTask(context.Background(), req)
		})
	})

	t.Run("ProcessWebhookMsg_CorruptedJSON_Panics", func(t *testing.T) {
		msg := mockMsg{data: []byte("{invalid-json-data}")}
		assert.Panics(t, func() { srv.processWebhookMsg(context.Background(), msg) })
	})

	t.Run("ProcessWebhookMsg_NilRequest_Panics", func(t *testing.T) {
		evtBytes, err := json.Marshal(&events.WebhookEvent{Request: nil})
		assert.NoError(t, err)
		msg := mockMsg{data: evtBytes}
		assert.Panics(t, func() { srv.processWebhookMsg(context.Background(), msg) })
	})

	t.Run("ProcessWebhookMsg_InvalidIdentity_Panics", func(t *testing.T) {
		evtBytes, err := json.Marshal(&events.WebhookEvent{
			Request: &pb.ExecuteTaskRequest{
				Identity: &pb.InfrastructureIdentity{TenantId: "t1"},
			},
		})
		assert.NoError(t, err)
		msg := mockMsg{data: evtBytes}
		assert.Panics(t, func() { srv.processWebhookMsg(context.Background(), msg) })
	})

	t.Run("ProcessWebhookMsg_MissingCallbackAddress_Panics", func(t *testing.T) {
		evtBytes, err := json.Marshal(&events.WebhookEvent{
			Request: &pb.ExecuteTaskRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "tenant-1",
					AppId:    "app-1",
					JobId:    "job-1",
				},
				StepId: "step-1",
			},
		})
		assert.NoError(t, err)
		msg := mockMsg{data: evtBytes}
		assert.Panics(t, func() { srv.processWebhookMsg(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_MalformedSubject_Panics", func(t *testing.T) {
		msg := mockMsg{subject: "orchestrator.response.app"}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_CorruptedProtobuf_Panics", func(t *testing.T) {
		msg := mockMsg{
			subject: "orchestrator.response.app1.tenant1.job1.step1",
			data:    []byte("\xff\xff\xff\xff invalid proto bytes"),
		}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_EmptyAppId_Panics", func(t *testing.T) {
		msg := mockMsg{subject: "orchestrator.response..tenant1.job1.step1.callback"}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_EmptyTenantId_Panics", func(t *testing.T) {
		msg := mockMsg{subject: "orchestrator.response.app1..job1.step1.callback"}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_EmptyJobId_Panics", func(t *testing.T) {
		msg := mockMsg{subject: "orchestrator.response.app1.tenant1..step1.callback"}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_EmptyStepId_Panics", func(t *testing.T) {
		msg := mockMsg{subject: "orchestrator.response.app1.tenant1.job1..callback"}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_EmptyCallbackAddress_Panics", func(t *testing.T) {
		msg := mockMsg{subject: "orchestrator.response.app1.tenant1.job1.step1."}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})

	t.Run("ProcessOrchestratorResponses_WhitespaceCallbackAddress_Panics", func(t *testing.T) {
		wsCallback := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte("   "))
		msg := mockMsg{subject: "orchestrator.response.app1.tenant1.job1.step1." + wsCallback}
		assert.Panics(t, func() { srv.processOrchestratorResponses(context.Background(), msg) })
	})
}
