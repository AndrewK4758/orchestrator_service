package engine

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStateMerging_GlobalState_DeepMerge(t *testing.T) {
	initialJSON := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": map[string]interface{}{
					"level4": map[string]interface{}{
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

	state := &pb.WorkflowContext{
		GlobalState: initialStruct,
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
	err = mergeIntoGlobalState(state, incomingJSON, "")
	require.NoError(t, err, "Merging should not return an error for valid JSON")

	mergedMap := state.GlobalState.AsMap()
	
	// Assertions for deep merge
	require.Contains(t, mergedMap, "unrelated")
	assert.Equal(t, "untouched", mergedMap["unrelated"])

	l1, ok := mergedMap["level1"].(map[string]interface{})
	require.True(t, ok)
	l2, ok := l1["level2"].(map[string]interface{})
	require.True(t, ok)
	l3, ok := l2["level3"].(map[string]interface{})
	require.True(t, ok)
	l4, ok := l3["level4"].(map[string]interface{})
	require.True(t, ok)
	
	// Assert deep fields
	assert.Equal(t, "keep", l3["other3"])
	assert.Equal(t, "newValueA", l4["level5_a"])
	assert.Equal(t, "valueB", l4["level5_b"])
	assert.Equal(t, "valueC", l4["level5_c"])
}

func TestStateMerging_MalformedJSON(t *testing.T) {
	state := &pb.WorkflowContext{
		GlobalState: nil,
	}

	malformedJSON := `{"broken": "json`

	err := mergeIntoGlobalState(state, malformedJSON, "")
	assert.Error(t, err, "Should safely wrap and return an error for malformed JSON")
	assert.Nil(t, state.GlobalState, "State should be untouched")
}

func TestStateMerging_IdempotentSubmissions(t *testing.T) {
	state := &pb.WorkflowContext{
		GlobalState: nil,
	}

	payload := `{"key": "value", "nested": {"a": 1}}`

	// First submit
	err := mergeIntoGlobalState(state, payload, "")
	require.NoError(t, err)

	firstMap := state.GlobalState.AsMap()

	// Second submit with identical payload
	err = mergeIntoGlobalState(state, payload, "")
	require.NoError(t, err)

	secondMap := state.GlobalState.AsMap()

	assert.Equal(t, firstMap, secondMap, "Idempotent identical payloads should result in the exact same state")
}
