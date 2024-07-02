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

func TestAuditLoggerGlobalFields(t *testing.T) {
	ResetGlobalTestLogging(t)

	tmpdir := t.TempDir()

	testCases := []struct {
		name           string
		functionFields AuditFields
		globalFields   AuditFields
		finalFields    AuditFields
	}{
		{
			name: "no global fields",
			functionFields: AuditFields{
				"method": "basic",
			},
			globalFields: AuditFields{
				"method": "basic",
			},
			finalFields: AuditFields{
				"method": "basic",
			},
		},
		{
			name: "with global fields",
			functionFields: AuditFields{
				"method": "basic",
			},
			globalFields: AuditFields{
				"global": "field",
			},
			finalFields: AuditFields{
				"method": "basic",
				"global": "field",
			},
		},
		{
			name: "overwrite global fields",
			functionFields: AuditFields{
				"method": "basic",
			},
			globalFields: AuditFields{
				"method": "global",
			},
			finalFields: AuditFields{
				"method": "global",
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
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
			require.NotNil(t, testCase.finalFields)
			for k, v := range testCase.finalFields {
				require.Equal(t, v, event[k])
			}
		})
	}

}

func TestAuditFieldsMerge(t *testing.T) {
	testCases := []struct {
		name      string
		base      AuditFields
		overwrite AuditFields
		output    AuditFields
	}{
		{
			name:      "no overwrites",
			base:      map[string]any{"foo": "bar"},
			overwrite: nil,
			output:    map[string]any{"foo": "bar"},
		},
		{
			name:      "new fields",
			base:      map[string]any{"foo": "bar"},
			overwrite: map[string]any{"baz": "qux"},
			output: map[string]any{
				"foo": "bar",
				"baz": "qux",
			},
		},
		{
			name:      "overwrite fields",
			base:      map[string]any{"foo": "bar"},
			overwrite: map[string]any{"foo": "baz"},
			output:    map[string]any{"foo": "baz"},
		},
		{
			name: "add to deep map",
			base: map[string]any{
				"lvl1": map[string]any{
					"lvl2": map[string]any{
						"foo": "baz",
					},
				},
			},
			overwrite: map[string]any{
				"lvl1": map[string]any{
					"lvl2": map[string]any{
						"bar": "qux",
					},
				},
			},
			output: map[string]any{
				"lvl1": map[string]any{
					"lvl2": map[string]any{
						"foo": "baz",
						"bar": "qux",
					},
				},
			},
		},
		{
			name: "overwrite overwrite deep map with int",
			base: map[string]any{
				"lvl1": map[string]any{
					"lvl2": map[string]any{
						"foo": "baz",
					},
				},
			},
			overwrite: map[string]any{
				"lvl1": map[string]any{
					"lvl2": 1,
				},
			},
			output: map[string]any{
				"lvl1": map[string]any{
					"lvl2": 1,
				},
			},
		},
		{
			name: "overwrite overwrite deep map with string",
			base: map[string]any{
				"lvl1": map[string]any{
					"lvl2": 1,
				},
			},
			overwrite: map[string]any{
				"lvl1": map[string]any{
					"lvl2": map[string]any{
						"foo": "baz",
					},
				},
			},
			output: map[string]any{
				"lvl1": map[string]any{
					"lvl2": map[string]any{
						"foo": "baz",
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.base.Merge(testCase.overwrite)
			require.Equal(t, testCase.output, testCase.base)
		})
	}

}
