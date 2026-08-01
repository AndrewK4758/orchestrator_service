package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"orchestrator_service/internal/events"
)

type MockTracePublisher struct {
	mock.Mock
}

func (m *MockTracePublisher) PublishTrace(ctx context.Context, event events.TraceabilityEvent) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockTracePublisher) PublishCacheStore(ctx context.Context, data []byte) error {
	args := m.Called(ctx, data)
	return args.Error(0)
}

func (m *MockTracePublisher) PublishWebhook(ctx context.Context, event *events.WebhookEvent) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}



type MockMsg struct {
	mock.Mock
	jetstream.Msg
}

func (m *MockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}

func (m *MockMsg) Data() []byte {
	args := m.Called()
	return args.Get(0).([]byte)
}

func (m *MockMsg) Subject() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockMsg) Ack() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockMsg) Nak() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockMsg) NakWithDelay(delay time.Duration) error {
	args := m.Called(delay)
	return args.Error(0)
}

type MockKV struct {
	mock.Mock
	jetstream.KeyValue
}

func (m *MockKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	args := m.Called(ctx, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(jetstream.KeyValueEntry), args.Error(1)
}

func (m *MockKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	args := m.Called(ctx, key, value)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	args := m.Called(ctx, key, opts)
	return args.Error(0)
}

type MockKVEntry struct {
	val []byte
}

func (m MockKVEntry) Bucket() string                  { return "" }
func (m MockKVEntry) Key() string                     { return "" }
func (m MockKVEntry) Value() []byte                   { return m.val }
func (m MockKVEntry) Revision() uint64                { return 1 }
func (m MockKVEntry) Created() time.Time              { return time.Now() }
func (m MockKVEntry) Delta() uint64                   { return 0 }
func (m MockKVEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }

func TestProcessJetStreamMessage_WebhookFailure_DoesNotAck(t *testing.T) {
	mockPub := new(MockTracePublisher)
	mockPub.On("PublishTrace", mock.Anything, mock.Anything).Return(nil)
	mockKV := new(MockKV)
	mockMsg := new(MockMsg)

	engine := &Engine{
		pub:      mockPub,
		kvStore:  mockKV,
		clientID: "test-client",
		maxInferenceRetries: 3,
		publishMaxRetries: 3,
	}

	jobID := "job123"
	req := &pb.ExecuteWorkflowNodeRequest{
		
		Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: jobID},
		CallbackAddress: "http://webhook",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
	}
	reqBytes, _ := proto.Marshal(req)

	mockKV.On("Get", mock.Anything, jobID).Return(MockKVEntry{val: reqBytes}, nil)

	// Simulate failure in PublishWebhook
	mockPub.On("PublishTrace", mock.Anything, mock.Anything).Return(nil)
	mockPub.On("PublishWebhook", mock.Anything, mock.Anything).Return(errors.New("webhook failure"))

	// Setup msg
	mockMsg.On("Subject").Return("orchestrator.response.client." + jobID)

	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResult: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"key": structpb.NewStringValue("value"),
			},
		},
	}
	respBytes, _ := proto.Marshal(resp)
	mockMsg.On("Data").Return(respBytes)

	// We expect NakWithDelay to be called on failure, not Ack
	mockMsg.On("NakWithDelay", mock.Anything).Return(nil)

	// Run
	engine.ProcessJetStreamMessage(mockMsg)

	// Assert Ack was NOT called
	mockMsg.AssertNotCalled(t, "Ack")
	// Assert NakWithDelay WAS called
	mockMsg.AssertCalled(t, "NakWithDelay", mock.Anything)
}

func TestProcessJetStreamMessage_SchemaValidationFailure_Retries(t *testing.T) {
	mockPub := new(MockTracePublisher)
	mockPub.On("PublishTrace", mock.Anything, mock.Anything).Return(nil)
	mockPub.On("PublishWebhook", mock.Anything, mock.Anything).Return(nil)
	mockKV := new(MockKV)
	mockMsg := new(MockMsg)

	engine := &Engine{
		pub:      mockPub,
		kvStore:  mockKV,
		clientID: "test-client",
		maxInferenceRetries: 3,
		publishMaxRetries: 3,
	}

	jobID := "job123"
	req := &pb.ExecuteWorkflowNodeRequest{
		
		Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: jobID},
		CallbackAddress: "http://webhook",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:            "Test Node",
			ExpectedSchemaStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"type": structpb.NewStringValue("object"),
					"properties": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"name": structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"type": structpb.NewStringValue("string"),
								},
							}),
						},
					}),
					"required": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("name"),
						},
					}),
				},
			},
			TargetWorkerSubject: "worker.test",
		},
		InferenceRetryCount: 0,
	}
	reqBytes, _ := proto.Marshal(req)

	mockKV.On("Get", mock.Anything, jobID).Return(MockKVEntry{val: reqBytes}, nil)

	// In the retry block, it puts the new request in KV and publishes to Action
	mockKV.On("Put", mock.Anything, jobID, mock.Anything).Return(uint64(2), nil)


	// Setup msg with BAD schema (missing 'name' string)
	mockMsg.On("Subject").Return("orchestrator.response.client." + jobID)
	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResult: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"invalid": structpb.NewStringValue("schema"),
			},
		},
	}
	respBytes, _ := proto.Marshal(resp)
	mockMsg.On("Data").Return(respBytes)

	// It should Ack the bad response to remove it from the response stream
	mockMsg.On("Ack").Return(nil)
	mockMsg.On("NakWithDelay", mock.Anything).Return(nil)
	mockMsg.On("Term").Return(nil)

	engine.ProcessJetStreamMessage(mockMsg)

	// Assert KV Put was called to save the incremented retry count
	mockKV.AssertCalled(t, "Put", mock.Anything, jobID, mock.Anything)
	// Assert it re-published to worker

	// Assert it acked the BAD message so it doesn't loop forever
	mockMsg.AssertCalled(t, "Ack")
}

func TestProcessJetStreamMessage_SchemaValidationFailure_Exhausted(t *testing.T) {
	mockPub := new(MockTracePublisher)
	mockPub.On("PublishTrace", mock.Anything, mock.Anything).Return(nil)
	mockPub.On("PublishWebhook", mock.Anything, mock.Anything).Return(nil)
	mockKV := new(MockKV)
	mockMsg := new(MockMsg)

	engine := &Engine{
		pub:      mockPub,
		kvStore:  mockKV,
		clientID: "test-client",
		maxInferenceRetries: 3,
		publishMaxRetries: 3,
	}

	jobID := "job123"
	req := &pb.ExecuteWorkflowNodeRequest{
		
		Identity:        &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: jobID},
		CallbackAddress: "http://webhook",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:            "Test Node",
			ExpectedSchemaStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"type": structpb.NewStringValue("object"),
					"properties": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"name": structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"type": structpb.NewStringValue("string"),
								},
							}),
						},
					}),
					"required": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("name"),
						},
					}),
				},
			},
			TargetWorkerSubject: "worker.test",
		},
		InferenceRetryCount: 3, // Already at max!
	}
	reqBytes, _ := proto.Marshal(req)

	mockKV.On("Get", mock.Anything, jobID).Return(MockKVEntry{val: reqBytes}, nil)

	mockPub.On("PublishTrace", mock.Anything, mock.Anything).Return(nil)
	mockPub.On("PublishWebhook", mock.Anything, mock.Anything).Return(nil)
	mockPub.On("PublishWebhook", mock.Anything, mock.MatchedBy(func(evt *events.WebhookEvent) bool {
		return evt.IsSuspended == true
	})).Return(nil)

	// It should delete the KV entry
	mockKV.On("Delete", mock.Anything, jobID, mock.Anything).Return(nil)

	// Setup msg with BAD schema
	mockMsg.On("Subject").Return("orchestrator.response.client." + jobID)
	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResult: &structpb.Struct{Fields: map[string]*structpb.Value{"invalid": structpb.NewStringValue("schema")}},
	}
	respBytes, _ := proto.Marshal(resp)
	mockMsg.On("Data").Return(respBytes)

	mockMsg.On("Ack").Return(nil)
	mockMsg.On("NakWithDelay", mock.Anything).Return(nil)
	mockMsg.On("Term").Return(nil)

	engine.ProcessJetStreamMessage(mockMsg)

	// Assert it published a Suspended webhook
	mockPub.AssertCalled(t, "PublishWebhook", mock.Anything, mock.Anything)
	mockMsg.AssertCalled(t, "Ack")
}

func TestEngine_ProcessJetStreamMessage_WebhookCallback_TableDriven(t *testing.T) {
	tests := []struct {
		name                   string
		jobID                  string
		callbackAddress        string
		inferenceRetryCount    int32
		maxInferenceRetries    int32
		expectedSchemaStruct   *structpb.Struct
		workerResponse         *pb.PerformActionResponse
		expectedEventType      events.EventType
		expectedIsSuspended    bool
		expectErrorContract    bool
		expectedErrorCode      string
		expectedErrorMsgPrefix string
	}{
		{
			name:                 "Successful worker execution triggers success webhook event",
			jobID:                "job-success-123",
			callbackAddress:      "http://client-callback.internal/webhook",
			inferenceRetryCount:  0,
			maxInferenceRetries:  3,
			expectedSchemaStruct: nil,
			workerResponse: &pb.PerformActionResponse{
				Success: true,
				ActionResult: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"status": structpb.NewStringValue("completed"),
					},
				},
			},
			expectedEventType:   events.EventActionCompleted,
			expectedIsSuspended: false,
			expectErrorContract: false,
		},
		{
			name:                 "Worker error contract triggers failure webhook event",
			jobID:                "job-err-456",
			callbackAddress:      "http://client-callback.internal/webhook",
			inferenceRetryCount:  0,
			maxInferenceRetries:  3,
			expectedSchemaStruct: nil,
			workerResponse: &pb.PerformActionResponse{
				Success: false,
				ErrorContract: &pb.ErrorContract{
					Code:    "500",
					Message: "Worker internal failure",
				},
			},
			expectedEventType:      events.EventActionFailed,
			expectedIsSuspended:    false,
			expectErrorContract:    true,
			expectedErrorCode:      "500",
			expectedErrorMsgPrefix: "Worker internal failure",
		},
		{
			name:                 "Validation retry exhaustion triggers job suspension event",
			jobID:                "job-exhausted-789",
			callbackAddress:      "http://client-callback.internal/webhook",
			inferenceRetryCount:  3, // Already at max retries (3 >= 3)
			maxInferenceRetries:  3,
			expectedSchemaStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"type": structpb.NewStringValue("object"),
					"properties": structpb.NewStructValue(&structpb.Struct{
						Fields: map[string]*structpb.Value{
							"required_field": structpb.NewStructValue(&structpb.Struct{
								Fields: map[string]*structpb.Value{
									"type": structpb.NewStringValue("string"),
								},
							}),
						},
					}),
					"required": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("required_field"),
						},
					}),
				},
			},
			workerResponse: &pb.PerformActionResponse{
				Success: true,
				ActionResult: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"unexpected_field": structpb.NewStringValue("invalid_payload"),
					},
				},
			},
			expectedEventType:      events.EventActionFailed,
			expectedIsSuspended:    true,
			expectErrorContract:    true,
			expectedErrorCode:      "400",
			expectedErrorMsgPrefix: "ValidationRetryExhausted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPub := new(MockTracePublisher)
			mockKV := new(MockKV)
			mockMsg := new(MockMsg)

			engine := &Engine{
				pub:                 mockPub,
				kvStore:             mockKV,
				clientID:            "test-client-id",
				maxInferenceRetries: tt.maxInferenceRetries,
				publishMaxRetries:   3,
			}

			req := &pb.ExecuteWorkflowNodeRequest{
				Identity: &pb.InfrastructureIdentity{
					TenantId: "tenant-1",
					AppId:    "app-1",
					JobId:    tt.jobID,
				},
				CallbackAddress:     tt.callbackAddress,
				InferenceRetryCount: tt.inferenceRetryCount,
				NodeConfig: &pb.NodeConfiguration{
					NodeName:             "Test_Node",
					ExpectedSchemaStruct: tt.expectedSchemaStruct,
					TargetWorkerSubject:  "worker.test",
				},
			}
			reqBytes, err := proto.Marshal(req)
			if err != nil {
				t.Fatalf("failed to marshal request: %v", err)
			}

			respBytes, err := proto.Marshal(tt.workerResponse)
			if err != nil {
				t.Fatalf("failed to marshal worker response: %v", err)
			}

			// Mock KV Setup
			mockKV.On("Get", mock.Anything, tt.jobID).Return(MockKVEntry{val: reqBytes}, nil)
			mockKV.On("Delete", mock.Anything, tt.jobID, mock.Anything).Return(nil)

			// Mock Trace & Webhook Publisher Setup
			mockPub.On("PublishTrace", mock.Anything, mock.MatchedBy(func(evt events.TraceabilityEvent) bool {
				return evt.EventType == tt.expectedEventType && evt.Identity.JobId == tt.jobID
			})).Return(nil)

			mockPub.On("PublishWebhook", mock.Anything, mock.MatchedBy(func(evt *events.WebhookEvent) bool {
				if evt.Request.CallbackAddress != tt.callbackAddress {
					return false
				}
				if evt.IsSuspended != tt.expectedIsSuspended {
					return false
				}
				if tt.expectErrorContract {
					if evt.ErrorContract == nil {
						return false
					}
					if tt.expectedErrorCode != "" && evt.ErrorContract.Code != tt.expectedErrorCode {
						return false
					}
					if tt.expectedErrorMsgPrefix != "" && !strings.HasPrefix(evt.ErrorContract.Message, tt.expectedErrorMsgPrefix) {
						return false
					}
				} else {
					if evt.ErrorContract != nil {
						return false
					}
				}
				return true
			})).Return(nil)

			// Mock JetStream Msg Setup
			mockMsg.On("Subject").Return("orchestrator.response.test-client-id." + tt.jobID)
			mockMsg.On("Data").Return(respBytes)
			mockMsg.On("Ack").Return(nil)

			// Execute ProcessJetStreamMessage
			engine.ProcessJetStreamMessage(mockMsg)

			// Assertions: Webhook, Trace, KV Cleanup, Ack
			mockPub.AssertCalled(t, "PublishWebhook", mock.Anything, mock.Anything)
			mockPub.AssertCalled(t, "PublishTrace", mock.Anything, mock.Anything)
			mockKV.AssertCalled(t, "Get", mock.Anything, tt.jobID)
			mockKV.AssertCalled(t, "Delete", mock.Anything, tt.jobID, mock.Anything)
			mockMsg.AssertCalled(t, "Ack")

			// Structural Assertions: Zero Graph Traversal or Next-Step Lookups
			mockKV.AssertNotCalled(t, "Put", mock.Anything, mock.Anything, mock.Anything)
			mockMsg.AssertNotCalled(t, "Nak")
			mockMsg.AssertNotCalled(t, "NakWithDelay", mock.Anything)
			mockPub.AssertNotCalled(t, "PublishCacheStore", mock.Anything, mock.Anything)
		})
	}
}
