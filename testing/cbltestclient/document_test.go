// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestReplaceBodyUpdate(t *testing.T) {
	testCases := []struct {
		name            string
		current         map[string]any
		newBody         map[string]any
		expectedUpdated map[string]any
		expectedRemoved []string
	}{
		{
			name:            "create",
			current:         nil,
			newBody:         map[string]any{"a": 1},
			expectedUpdated: map[string]any{"a": 1},
		},
		{
			name:            "add a property",
			current:         map[string]any{"a": 1},
			newBody:         map[string]any{"a": 1, "b": 2},
			expectedUpdated: map[string]any{"a": 1, "b": 2},
		},
		{
			name:            "drop a property",
			current:         map[string]any{"a": 1, "b": 2},
			newBody:         map[string]any{"a": 1},
			expectedUpdated: map[string]any{"a": 1},
			expectedRemoved: []string{"b"},
		},
		{
			name:            "replace a nested object wholesale",
			current:         map[string]any{"a": map[string]any{"x": 1, "y": 2}},
			newBody:         map[string]any{"a": map[string]any{"z": 3}},
			expectedUpdated: map[string]any{"a": map[string]any{"z": 3}},
		},
		{
			name:            "replace an array",
			current:         map[string]any{"a": []any{1, 2, 3}},
			newBody:         map[string]any{"a": []any{4}},
			expectedUpdated: map[string]any{"a": []any{4}},
		},
		{
			name:    "delete every property",
			current: map[string]any{"b": 2, "a": 1},
			newBody: map[string]any{},
			// removals are sorted so that the same pair of bodies always produces the same request
			expectedRemoved: []string{"a", "b"},
		},
		{
			name:            "the server's own metadata is never removed",
			current:         map[string]any{DocumentIDProperty: "doc1", DocumentRevsProperty: "1@src", "a": 1},
			newBody:         map[string]any{"a": 1},
			expectedUpdated: map[string]any{"a": 1},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			item, err := ReplaceBodyUpdate("scope.collection", "doc1", testCase.current, testCase.newBody)
			require.NoError(t, err)

			assert.Equal(t, UpdateTypeUpdate, item.Type)
			assert.Equal(t, "scope.collection", item.Collection)
			assert.Equal(t, "doc1", item.DocumentID)
			assert.Equal(t, testCase.expectedRemoved, item.RemovedProperties)
			if testCase.expectedUpdated == nil {
				assert.Nil(t, item.UpdatedProperties)
				return
			}
			require.Len(t, item.UpdatedProperties, 1, "a whole-body replace has to be a single update, or the document gets two current versions")
			assert.Equal(t, testCase.expectedUpdated, item.UpdatedProperties[0])
		})
	}
}

func TestReplaceBodyUpdateRejectsKeypaths(t *testing.T) {
	// A property name containing a keypath metacharacter would address something other than that
	// top-level property, silently writing to the wrong place.
	for _, key := range []string{"a.b", "a[0]", `a\b`, ""} {
		t.Run("new body "+key, func(t *testing.T) {
			_, err := ReplaceBodyUpdate("scope.collection", "doc1", nil, map[string]any{key: 1})
			require.Error(t, err)
		})
		t.Run("current body "+key, func(t *testing.T) {
			_, err := ReplaceBodyUpdate("scope.collection", "doc1", map[string]any{key: 1}, map[string]any{"a": 1})
			require.Error(t, err)
		})
	}
}

func TestParseRevisionHistory(t *testing.T) {
	// The format is the same one Sync Gateway uses on the blip wire: the current version first,
	// then zero or two merge versions after commas, then the historical versions after a single
	// semicolon.
	testCases := []struct {
		name          string
		revs          string
		expectedCV    string
		expectedPVLen int
		expectedMVLen int
		expectError   bool
	}{
		{
			name:       "current version only",
			revs:       "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w",
			expectedCV: "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w",
		},
		{
			name:          "current and historical versions",
			revs:          "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w;17f4ed7b42a70000@ScJAVJf3TdOUanAcByIcXg",
			expectedCV:    "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w",
			expectedPVLen: 1,
		},
		{
			name:          "whitespace after the delimiters",
			revs:          "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w; 17f4ed7b42a70000@ScJAVJf3TdOUanAcByIcXg, 17f4ed7b42a70000@MlAW1NbbT8KcTRO8oPnpgw",
			expectedCV:    "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w",
			expectedPVLen: 2,
		},
		{
			name:          "merge versions",
			revs:          "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w,17f4ed7b42a70000@ScJAVJf3TdOUanAcByIcXg,17f4ed7b42a70000@MlAW1NbbT8KcTRO8oPnpgw",
			expectedCV:    "18d5ceeba9bd0000@QIRvnyk+QaCikdy0Z6915w",
			expectedMVLen: 2,
		},
		{
			name:        "empty",
			revs:        "",
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			hlv, legacyRevs, err := ParseRevisionHistory(testCase.revs)
			if testCase.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Empty(t, legacyRevs)
			assert.Equal(t, testCase.expectedCV, hlv.GetCurrentVersionString())
			assert.Len(t, hlv.PreviousVersions, testCase.expectedPVLen)
			assert.Len(t, hlv.MergeVersions, testCase.expectedMVLen)
		})
	}
}

func TestDocumentProperties(t *testing.T) {
	doc := Document{DocumentIDProperty: "doc1", DocumentRevsProperty: "1@src", "foo": "bar"}
	assert.Equal(t, "doc1", doc.ID())
	assert.Equal(t, "1@src", doc.Revs())
	assert.Equal(t, map[string]any{"foo": "bar"}, doc.Properties())
}
