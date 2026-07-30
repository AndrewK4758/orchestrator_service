package engine

import (
	"context"
	"errors"
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
