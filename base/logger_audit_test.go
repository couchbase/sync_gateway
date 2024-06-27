// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuditLogger(t *testing.T) {
	ResetGlobalTestLogging(t)

	tmpdir := t.TempDir()

	ctx := TestCtx(t)

	globalFields := AuditFields{
		"global": "field",
	}
	testCases := []struct {
		name         string
		globalFields AuditFields
	}{
		{
			name: "no global fields",
		},
		{
			name:         "with global fields",
			globalFields: globalFields,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var err error
			auditLogger, err = NewAuditLogger(ctx, nil, tmpdir, 0, nil, testCase.globalFields)
			require.NoError(t, err)

			output := AuditLogContents(t, func() {
				// Test basic audit event
				Audit(ctx, AuditIDPublicUserAuthenticated, map[string]any{"method": "basic"})
			},
			)
			var event map[string]any
			require.NoError(t, json.Unmarshal(output, &event))
			method, ok := event["method"].(string)
			require.True(t, ok)
			require.Equal(t, "basic", method)
			for k := range testCase.globalFields {
				if testCase.globalFields == nil {
					require.NotContains(t, event, k)
				} else {
					require.Contains(t, event, k)
				}
			}
		})
	}

}
