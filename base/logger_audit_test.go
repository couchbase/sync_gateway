// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLoggerGlobalFields(t *testing.T) {
	ResetGlobalTestLogging(t)

	tmpdir := t.TempDir()

	testCases := []struct {
		name           string
		functionFields AuditFields
		globalFields   AuditFields
		contextFields  AuditFields
		finalFields    AuditFields
		warningCount   int64
	}{
		{
			name: "no global fields",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: nil,
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
		},
		{
			name: "with global fields",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: AuditFields{
				"global": "field",
			},
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
				"global":             "field",
			},
		},
		{
			name: "overwrite global fields",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: AuditFields{
				AuditFieldAuthMethod: "global",
			},
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			warningCount: 1,
		},
		{
			name: "context fields only",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: nil,
			contextFields: AuditFields{
				"context": "field",
			},
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
				"context":            "field",
			},
		},
		{
			name: "context fields overwrite fields",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: nil,
			contextFields: AuditFields{
				AuditFieldAuthMethod: "context",
			},
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			warningCount: 1,
		},
		{
			name: "global fields and context fields",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: AuditFields{
				"global": "field",
			},
			contextFields: AuditFields{
				"context": "field",
			},
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
				"global":             "field",
				"context":            "field",
			},
		},
		{
			name: "global fields and context fields overwrite fields",
			functionFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			globalFields: AuditFields{
				AuditFieldAuthMethod: "global",
			},
			contextFields: AuditFields{
				AuditFieldAuthMethod: "context",
			},
			finalFields: AuditFields{
				AuditFieldAuthMethod: "basic",
			},
			warningCount: 2, // error from global and error from context
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ResetGlobalTestLogging(t)
			ctx := TestCtx(t)
			if testCase.contextFields != nil {
				ctx = AuditLogCtx(ctx, testCase.contextFields)
			}
			logger, err := NewAuditLogger(ctx, &AuditLoggerConfig{FileLoggerConfig: FileLoggerConfig{Enabled: Ptr(true)}}, tmpdir, 0, nil, testCase.globalFields)
			require.NoError(t, err)
			auditLogger.Store(logger)

			startWarnCount := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
			output := AuditLogContents(t, func(tb testing.TB) {
				// Test basic audit event
				Audit(ctx, AuditIDPublicUserAuthenticated, map[string]any{AuditFieldAuthMethod: "basic"})
			},
			)
			var event map[string]any
			require.NoError(t, json.Unmarshal(output, &event))
			require.NotNil(t, testCase.finalFields)
			for k, v := range testCase.finalFields {
				require.Equal(t, v, event[k])
			}
			require.Equal(t, startWarnCount+testCase.warningCount, SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value())
		})
	}

}

func TestAuditFieldsMerge(t *testing.T) {
	testCases := []struct {
		name      string
		base      AuditFields
		overwrite AuditFields
		output    AuditFields
		hasError  bool
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
			output:    map[string]any{"foo": "bar"},
			hasError:  true,
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
					},
				},
			},
			hasError: true,
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
					"lvl2": map[string]any{
						"foo": "baz",
					},
				},
			},
			hasError: true,
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
					"lvl2": 1,
				},
			},
			hasError: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			base := make(AuditFields, len(testCase.base))
			maps.Copy(base, testCase.base)
			ctx := TestCtx(t)
			startWarnCount := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
			if testCase.hasError {
				AssertLogContains(t, "already exist in base audit fields", func() {
					base.merge(ctx, testCase.overwrite)
				})
				require.Equal(t, startWarnCount+1, SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value())
			} else {
				base.merge(ctx, testCase.overwrite)
				require.Equal(t, startWarnCount, SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value())
			}
			require.Equal(t, testCase.output, base)
		})
	}

}

func BenchmarkAuditFieldwork(b *testing.B) {
	if !IsEnterpriseEdition() {
		b.Skip("Enterprise Edition only")
	}

	originalLogger := auditLogger.Load()
	b.Cleanup(func() {
		auditLogger.Store(originalLogger)
	})

	// discard audits to benchmark field expansion only
	buf := io.Discard
	al, err := NewAuditLogger(TestCtx(b), &AuditLoggerConfig{
		FileLoggerConfig: FileLoggerConfig{
			Enabled:             Ptr(true),
			Output:              buf,
			CollationBufferSize: Ptr(0),
		},
	}, b.TempDir(), auditMinAge, nil, map[string]any{"foo": "bar", "buzz": 1234})
	require.NoError(b, err)
	auditLogger.Store(al)

	ctx := TestCtx(b)
	ctx = DatabaseLogCtx(ctx, "db", nil)
	ctx = KeyspaceLogCtx(ctx, "bucket", "scope", "collection")
	ctx = CorrelationIDLogCtx(ctx, FormatBlipContextID("a0b1c2"))
	ctx = RequestLogCtx(ctx, RequestData{
		CorrelationID:     "#1234",
		RequestHost:       "1.2.3.4:4985",
		RequestRemoteAddr: "9.8.7.6:12345",
	})
	ctx = UserLogCtx(ctx, "user", "domain", []string{"role1"})

	docID, revID := "docID", "revID"

	type args struct {
		ctx            context.Context
		id             AuditID
		additionalData AuditFields
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "additional fields",
			args: args{
				ctx: ctx,
				id:  AuditIDDocumentRead,
				additionalData: AuditFields{
					AuditFieldDocID:      docID,
					AuditFieldDocVersion: revID,
				},
			},
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Audit(test.args.ctx, test.args.id, test.args.additionalData)
			}
		})
	}
}

func Test_expandFieldsAdditionalDataReadOnly(t *testing.T) {
	additionalData := AuditFields{"foo": "bar"}
	for i := 0; i < 5; i++ {
		fields := expandFields(AuditIDDocumentRead, TestCtx(t), AuditFields{"global": true}, additionalData)
		// id, name, description, timestamp, global, foo
		assert.Len(t, fields, 6)
	}
	// additionalData should not be modified
	assert.Len(t, additionalData, 1)
}
