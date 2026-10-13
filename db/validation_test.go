// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/testing/require"
)

func TestHasRemovedProperty(t *testing.T) {
	testCases := []struct {
		name        string
		body        string
		expected    bool
		expectedErr bool
	}{
		{name: "empty body", body: `{}`, expected: false},
		{name: "top level removed", body: `{"_removed":true}`, expected: true},
		{name: "top level removed false", body: `{"_removed":false}`, expected: true},
		{name: "top level removed null", body: `{"_removed":null}`, expected: false},
		{name: "top level removed string", body: `{"_removed":"true"}`, expected: true},
		{name: "top level removed after nested removed", body: `{"foo":{"_removed":false},"_removed":true}`, expected: true},
		{name: "top level removed with whitespace", body: `{ "_removed": true }`, expected: true},
		{name: "top level removed with space before colon", body: `{"_removed" : true}`, expected: true},
		{name: "removed in string value", body: `{"foo":"\"_removed\":true"}`, expected: false},
		{name: "removed key suffix", body: `{"_removed:":true}`, expected: false},
		{name: "removed key prefix", body: `{"x_removed":true}`, expected: false},
		{name: "removed in array value", body: `{"foo":["_removed","\"_removed\":"]}`, expected: false},
		{name: "removed in nested object", body: `{"foo":{"_removed":true}}`, expected: false},
		{name: "removed in object in array", body: `{"foo":[{"_removed":true}]}`, expected: false},
		{name: "different case top level with nested removed", body: `{"_REMOVED":true,"foo":{"_removed":true}}`, expected: true},
		{name: "invalid json", body: `{"_removed":true`, expectedErr: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			removed, err := hasRemovedProperty([]byte(testCase.body))
			if testCase.expectedErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expected, removed, "body: %s", testCase.body)
		})
	}
}
