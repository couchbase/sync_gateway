// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !race

package rest

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"

	"github.com/stretchr/testify/require"
)

// AssertLogContains can hit the race detector due to swapping the global loggers

func TestBadCORSValuesConfig(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	// expect database to be created with bad CORS values, but do log a warning
	dbConfig := rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.com", "1http://example.com"},
	}
	base.AssertLogContains(t, "cors.origin contains values", func() {
		rt.CreateDatabase("db", dbConfig)
	})
}

// TestAuditLoggingGlobals modifies all the global loggers
func TestAuditLoggingGlobals(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test can panic with gocb logging CBG-4076")
	}
	globalFields := map[string]any{
		"global":  "field",
		"global2": "field2",
	}

	globalEnvVarName := "SG_TEST_GLOBAL_AUDIT_LOGGING"

	testCases := []struct {
		name              string
		globalAuditEvents *string
		startupErrorMsg   string
	}{
		{
			name: "no global fields",
		},
		{
			name:              "with global fields",
			globalAuditEvents: base.StringPtr(string(base.MustJSONMarshal(t, globalFields))),
		},
		{
			name:              "invalid json",
			globalAuditEvents: base.StringPtr(`notjson`),
			startupErrorMsg:   "Unable to unmarshal",
		},
		{
			name:              "empty env var",
			globalAuditEvents: base.StringPtr(""),
			startupErrorMsg:   "Unable to unmarshal",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			base.ResetGlobalTestLogging(t)
			base.InitializeMemoryLoggers()
			ctx := base.TestCtx(t)
			if testCase.globalAuditEvents != nil {
				require.NoError(t, os.Setenv(globalEnvVarName, *testCase.globalAuditEvents))
				defer func() {
					require.NoError(t, os.Unsetenv(globalEnvVarName))
				}()
			} else {
				require.NoError(t, os.Unsetenv(globalEnvVarName))
			}
			startupConfig := DefaultStartupConfig("")
			startupConfig.Logging = base.LoggingConfig{
				LogFilePath: t.TempDir(),
				Audit: &base.AuditLoggerConfig{
					FileLoggerConfig: base.FileLoggerConfig{
						Enabled: base.BoolPtr(true),
					},
				},
			}
			if testCase.globalAuditEvents != nil {
				startupConfig.Unsupported.AuditInfoProvider = &AuditInfoProviderConfig{
					GlobalInfoEnvVarName: base.StringPtr(globalEnvVarName),
				}
			}
			err := startupConfig.SetupAndValidateLogging(ctx)
			if testCase.startupErrorMsg != "" {
				require.ErrorContains(t, err, testCase.startupErrorMsg)
				return
			}
			require.NoError(t, err)
			output := base.AuditLogContents(t, func(tb testing.TB) {
				base.Audit(ctx, base.AuditIDPublicUserAuthenticated, map[string]any{base.AuditFieldAuthMethod: "basic"})
			})
			var event map[string]any
			require.NoError(t, json.Unmarshal(output, &event))
			require.Contains(t, event, base.AuditFieldAuthMethod)
			for k, v := range globalFields {
				if testCase.globalAuditEvents != nil {
					require.Equal(t, v, event[k])
				} else {
					require.NotContains(t, event, k)
				}
			}
		})
	}

}
