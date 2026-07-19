package engine

import (
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStateMerging_GlobalState_DeepMerge(t *testing.T) {
	initialJSON := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"level4": map[string]any{
						"level5_a": "valueA",
						"level5_b": "valueB",
					},
					"other3": "keep",
				},
			},
		},
		"unrelated": "untouched",
	}

	initialStruct, err := structpb.NewStruct(initialJSON)
	require.NoError(t, err)

	state := &EngineState{
		Req: &pb.ExecuteWorkflowNodeRequest{GlobalState: initialStruct},
	}

	// The incoming JSON has a new value for level5_a, a new field level5_c, and keeps the rest
	incomingJSON := `
	{
		"level1": {
			"level2": {
				"level3": {
					"level4": {
						"level5_a": "newValueA",
						"level5_c": "valueC"
					}
				}
			}
		}
	}
	`

	// Calling the unexported merge function (or its replacement)
	// Right now we test the current implementation, which we expect to fail on deep merge
	err = MergeStateContext(state.Req, incomingJSON, "")
	require.NoError(t, err, "Merging should not return an error for valid JSON")

	mergedMap := state.Req.GlobalState.AsMap()

	// Assertions for deep merge
	require.Contains(t, mergedMap, "unrelated")
	assert.Equal(t, "untouched", mergedMap["unrelated"])

	l1, ok := mergedMap["level1"].(map[string]any)
	require.True(t, ok)
	l2, ok := l1["level2"].(map[string]any)
	require.True(t, ok)
	l3, ok := l2["level3"].(map[string]any)
	require.True(t, ok)
	l4, ok := l3["level4"].(map[string]any)
	require.True(t, ok)

	// Assert deep fields
	assert.Equal(t, "keep", l3["other3"])
	assert.Equal(t, "newValueA", l4["level5_a"])
	assert.Equal(t, "valueB", l4["level5_b"])
	assert.Equal(t, "valueC", l4["level5_c"])
}

func TestStateMerging_MalformedJSON(t *testing.T) {
	state := &EngineState{
		Req: &pb.ExecuteWorkflowNodeRequest{GlobalState: nil},
	}

	malformedJSON := `{"broken": "json`

	err := MergeStateContext(state.Req, malformedJSON, "")
	assert.Error(t, err, "Should safely wrap and return an error for malformed JSON")
	assert.Nil(t, state.Req.GlobalState, "State should be untouched")
}

func TestStateMerging_IdempotentSubmissions(t *testing.T) {
	state := &EngineState{
		Req: &pb.ExecuteWorkflowNodeRequest{GlobalState: nil},
	}

	payload := `{"key": "value", "nested": {"a": 1}}`

	// First submit
	err := MergeStateContext(state.Req, payload, "")
	require.NoError(t, err)

	firstMap := state.Req.GlobalState.AsMap()

	// Second submit with identical payload
	err = MergeStateContext(state.Req, payload, "")
	require.NoError(t, err)

	secondMap := state.Req.GlobalState.AsMap()

	assert.Equal(t, firstMap, secondMap, "Idempotent identical payloads should result in the exact same state")
}

func FuzzMergeStateContext(f *testing.F) {
	f.Add(`{"key": "value"}`)
	f.Add(`{"Context": null}`)
	f.Add(`{"nested": {"a": [1, 2, 3]}}`)
	f.Add(`{"emoji": "🚀"}`)
	f.Add(`{"broken": "json`)
	f.Add(`"just a string"`)

	f.Fuzz(func(t *testing.T, payload string) {
		req := &pb.ExecuteWorkflowNodeRequest{
			Identity:    &pb.InfrastructureIdentity{JobId: "job-123"},
			StepId:      "step-456",
			GlobalState: nil,
		}

		err := MergeStateContext(req, payload, "")

		// We don't care if there's an error, as long as it doesn't panic.
		if err == nil {
			require.NotNil(t, req.GlobalState, "GlobalState should not be nil")
		}
	})
}

func TestStateMerging_TypeConflictResolution(t *testing.T) {
	initialJSON := map[string]any{"status": "pending"}
	initialStruct, err := structpb.NewStruct(initialJSON)
	require.NoError(t, err)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: "job-123"},
		StepId:      "step-456",
		GlobalState: initialStruct,
	}

	payload := `{"status": {"code": 200}}`
	err = MergeStateContext(req, payload, "")
	require.NoError(t, err)

	m := req.GlobalState.AsMap()
	statusMap, ok := m["status"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(200), statusMap["code"])
}

func TestStateMerging_SystemKeyOverwrites(t *testing.T) {
	// Deprecated: System key context injection is handled dynamically by client state.
}

func TestStateMerging_UnicodeAndMalformed(t *testing.T) {
	req := &pb.ExecuteWorkflowNodeRequest{}

	// Valid Unicode
	err := MergeStateContext(req, `{"msg": "Hello 🚀 🌍"}`, "")
	require.NoError(t, err)
	assert.Equal(t, "Hello 🚀 🌍", req.GlobalState.AsMap()["msg"])

	// Malformed
	err = MergeStateContext(req, `{"msg": "Hello \xFF"}`, "")
	assert.Error(t, err)

	err = MergeStateContext(req, `{bad}`, "")
	assert.Error(t, err)
}

func TestStateMerging_DeeplyNestedArraysAndObjects(t *testing.T) {
	initialJSON := map[string]any{
		"arr": []any{
			map[string]any{"id": 1, "val": "a"},
			map[string]any{"id": 2, "val": "b"},
		},
		"nested": map[string]any{
			"a": map[string]any{
				"b": map[string]any{
					"c": map[string]any{
						"d": "keep",
					},
				},
			},
		},
	}
	initialStruct, err := structpb.NewStruct(initialJSON)
	require.NoError(t, err)

	req := &pb.ExecuteWorkflowNodeRequest{
		GlobalState: initialStruct,
	}

	payload := `
	{
		"arr": [
			{"id": 3, "val": "c"}
		],
		"nested": {
			"a": {
				"b": {
					"c": {
						"e": "new"
					}
				}
			}
		}
	}
	`
	err = MergeStateContext(req, payload, "")
	require.NoError(t, err)

	m := req.GlobalState.AsMap()

	// array should be overwritten (since deepMerge doesn't merge slices)
	arr := m["arr"].([]any)
	require.Len(t, arr, 1)
	assert.Equal(t, float64(3), arr[0].(map[string]any)["id"])

	// objects should be deeply merged
	c := m["nested"].(map[string]any)["a"].(map[string]any)["b"].(map[string]any)["c"].(map[string]any)
	assert.Equal(t, "keep", c["d"])
	assert.Equal(t, "new", c["e"])
}

// TestGenericState_ArbitraryJSONEvaluate validates that the Orchestrator safely handles
// and merges completely unknown, deep, and inverted JSON shapes into GlobalState without crashing,
// which is required for the C# Client's EvaluateAndProceedAsync JSONPath routing to function correctly.
func TestGenericState_ArbitraryJSONEvaluate(t *testing.T) {
	initialStruct, err := structpb.NewStruct(map[string]any{"base": "state"})
	require.NoError(t, err)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity:    &pb.InfrastructureIdentity{JobId: "job-arbitrary-evaluate"},
		StepId:      "step-evaluate",
		GlobalState: initialStruct,
	}

	// Complex, arbitrary, and inverted JSON shape
	payload := `
	{
		"ConditionPathRoot": {
			"InvertedIfs": [
				{"Match": false, "Value": "skip"},
				{"Match": true, "Value": "proceed_async"}
			],
			"DeeplyNested": {
				"Level1": {
					"Level2": {
						"TargetValue": 42.0,
						"CompletelyUnknownShape": {
							"dynamic_key_1": [1, 2, 3],
							"dynamic_key_2": null
						}
					}
				}
			}
		}
	}`

	// Execute
	err = MergeStateContext(req, payload, "")
	require.NoError(t, err, "Should not crash on arbitrary JSON shapes")

	// Validate shape is preserved for EvaluateAndProceedAsync
	m := req.GlobalState.AsMap()

	root, ok := m["ConditionPathRoot"].(map[string]any)
	require.True(t, ok)

	ifs, ok := root["InvertedIfs"].([]any)
	require.True(t, ok)
	require.Len(t, ifs, 2)
	assert.Equal(t, true, ifs[1].(map[string]any)["Match"])
	assert.Equal(t, "proceed_async", ifs[1].(map[string]any)["Value"])

	deep, ok := root["DeeplyNested"].(map[string]any)["Level1"].(map[string]any)["Level2"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 42.0, deep["TargetValue"])

	unknown, ok := deep["CompletelyUnknownShape"].(map[string]any)
	require.True(t, ok)

	arr, ok := unknown["dynamic_key_1"].([]any)
	require.True(t, ok)
	assert.Equal(t, 1.0, arr[0])

	// Ensure it handles nulls
	assert.Nil(t, unknown["dynamic_key_2"])
}
