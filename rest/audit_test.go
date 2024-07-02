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
	"bytes"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestAuditInjectableHeader(t *testing.T) {
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()
	const headerName = "extra-audit-logging-header"
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Unsupported.AuditInfoProvider = &AuditInfoProviderConfig{
				RequestInfoHeaderName: base.StringPtr(headerName),
			}
			config.Logging = base.LoggingConfig{
				LogFilePath: t.TempDir(),
				Audit: &base.AuditLoggerConfig{
					FileLoggerConfig: base.FileLoggerConfig{
						Enabled: base.BoolPtr(true),
					},
				},
			}
			require.NoError(t, config.SetupAndValidateLogging(base.TestCtx(t)))
		},
	})
	defer rt.Close()

	// initialize RestTester
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	rt.CreateUser("alice", nil)
	testCases := []struct {
		name string
		fn   func(method, resource, body string, headers map[string]string) *TestResponse
	}{
		/* FIXME: admin requests are not currently audited
		{
			name: "admin request",
			fn:   rt.SendAdminRequestWithHeaders,
		},
		*/
		{
			name: "user request",
			fn:   rt.SendRequestWithHeaders,
		},
		/* FIXME: diagnostic requests are not currently audited
		{
			name: "diagnostic request",
			fn:   rt.SendDiagnosticRequestWithHeaders,
		},
		*/
		/* FIXME: metrics requests are not currently audited
		{
			name: "metrics request",
			fn:   rt.SendMetricsRequestWithHeaders,
		},
		*/
	}
	for _, testCase := range testCases {
		headers := map[string]string{
			headerName:      `{"extra":"field"}`,
			"Authorization": getBasicAuthHeader("alice", RestTesterDefaultUserPassword),
		}
		t.Run(testCase.name, func(t *testing.T) {
			output := base.AuditLogContents(t, func() {
				// FIXME: this test only works with /db because not all http requests are logged
				RequireStatus(t, testCase.fn(http.MethodGet, "/db/", "", headers), http.StatusOK)
			})
			events := bytes.Split(output, []byte("\n"))
			foundEvent := false
			for _, rawEvent := range events {
				if bytes.TrimSpace(rawEvent) == nil {
					continue
				}
				var event map[string]any
				require.NoError(t, base.JSONUnmarshal(rawEvent, &event))
				require.Contains(t, event, "extra")
				require.Contains(t, "field", event["extra"].(string))
				foundEvent = true
			}
			require.True(t, foundEvent, "expected audit event not found")
		})
	}
}
