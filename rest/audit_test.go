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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLoggingFields(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test can panic with gocb logging CBG-4076")
	}

	// get tempdir before resetting global loggers, since the logger cleanup needs to happen before deletion
	tempdir := t.TempDir()
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()
	const (
		requestInfoHeaderName = "extra-audit-logging-header"
		requestUser           = "alice"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		AdminInterfaceAuthentication: true,
		PersistentConfig:             true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Unsupported.AuditInfoProvider = &AuditInfoProviderConfig{
				RequestInfoHeaderName: base.StringPtr(requestInfoHeaderName),
			}
			config.Logging = base.LoggingConfig{
				LogFilePath: tempdir,
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
	rt.CreateUser(requestUser, nil)

	// auditFieldValueIgnored is a special value for an audit field to skip value-specific checks whilst still ensuring the field property is set
	// used for unpredictable values, for example timestamps or request IDs (when the test is making concurrent or unordered requests)
	const auditFieldValueIgnored = "sg_test_audit_field_value_ignored"

	testCases := []struct {
		name string
		// auditableAction is a function that performs an action that should've been audited
		auditableAction func(t testing.TB)
		// expectedAuditEvents is a list of expected audit events and their fields for the given action... can be more than one event produced for a given action
		expectedAuditEventFields map[base.AuditID]base.AuditFields
	}{
		{
			name: "public silent request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendRequest(http.MethodGet, "/_ping", ""), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{},
		},
		{
			name: "guest request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendRequest(http.MethodGet, "/db/_session", ""), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDPublicUserAuthenticated: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldAuthMethod:    "guest",
				},
				// TODO: GUEST view session event?
			},
		},
		{
			name: "user request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/db/", "", requestUser), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDPublicUserAuthenticated: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "sgw", "user": "alice"},
				},
				base.AuditIDReadDatabase: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "sgw", "user": "alice"},
				},
			},
		},
		{
			name: "injected request data",
			auditableAction: func(t testing.TB) {
				headers := map[string]string{
					requestInfoHeaderName: `{"extra":"field"}`,
					"Authorization":       getBasicAuthHeader(requestUser, RestTesterDefaultUserPassword),
				}
				RequireStatus(t, rt.SendRequestWithHeaders(http.MethodGet, "/db/", "", headers), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDPublicUserAuthenticated: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "sgw", "user": "alice"},
					"extra":                      "field",
				},
				base.AuditIDReadDatabase: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldDatabase:      "db",
					base.AuditFieldRealUserID:    map[string]any{"domain": "sgw", "user": "alice"},
					"extra":                      "field",
				},
			},
		},
		{
			name: "rejected public request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/db/", "", "incorrect"), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDPublicUserAuthenticationFailed: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldAuthMethod:    "basic",
					base.AuditFieldDatabase:      "db",
					"username":                   "incorrect",
				},
			},
		},
		{
			name: "anon admin request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/db/", ""), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				// TODO: Admin auth event
				//base.AuditIDAdminUserAuthenticated: {
				//	base.AuditFieldCorrelationID:         auditFieldValueIgnored,
				//},
				base.AuditIDReadDatabase: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
				},
			},
		},
		{
			name: "authed admin request",
			auditableAction: func(t testing.TB) {
				if base.UnitTestUrlIsWalrus() {
					// Skip this subtest if running with walrus - it has no support for admin auth
					t.Skip("Skipping test that requires admin auth - Walrus not supported")
				}
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", base.TestClusterUsername(), base.TestClusterPassword()), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				// TODO: Admin auth event
				//base.AuditIDAdminUserAuthenticated: {
				//	base.AuditFieldCorrelationID:         auditFieldValueIgnored,
				//	base.AuditFieldRealUserID: map[string]any{"domain": "cbs", "user": base.TestClusterUsername()},
				//},
				base.AuditIDReadDatabase: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "cbs", "user": base.TestClusterUsername()},
				},
			},
		},
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			output := base.AuditLogContents(t, testCase.auditableAction)
			events := jsonLines(t, output)

			assert.Equalf(t, len(testCase.expectedAuditEventFields), len(events), "expected exactly %d audit events, got %d", len(testCase.expectedAuditEventFields), len(events))

			// for each event, check the fields match what we expected
			for _, event := range events {
				id, ok := event["id"]
				if !assert.Truef(t, ok, "audit event did not contain \"id\" field: %v", event) {
					continue
				}

				auditID := base.AuditID(id.(float64))
				for k, expectedVal := range testCase.expectedAuditEventFields[auditID] {
					eventField, ok := event[k]
					if !assert.Truef(t, ok, "missing field %q in audit event %q (%s)", k, auditID, base.AuditEvents[auditID].Name) {
						continue
					}
					if expectedVal != auditFieldValueIgnored {
						assert.Equalf(t, expectedVal, eventField, "unexpected value for field %q in audit event %q (%s)", k, auditID, base.AuditEvents[auditID].Name)
					}
				}
			}
		})
	}
}

// jsonLines unmarshals each line in data as JSON
func jsonLines(t testing.TB, data []byte) []map[string]any {
	lines := bytes.Split(data, []byte("\n"))
	output := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if bytes.TrimSpace(line) == nil {
			continue
		}
		var data map[string]any
		require.NoError(t, base.JSONUnmarshal(line, &data))
		output = append(output, data)
	}
	return output
}

func TestAuditDatabaseUpdate(t *testing.T) {
	// get tempdir before resetting global loggers, since the logger cleanup needs to happen before deletion
	tempdir := t.TempDir()
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Logging = base.LoggingConfig{
				LogFilePath: tempdir,
				Audit: &base.AuditLoggerConfig{
					FileLoggerConfig: base.FileLoggerConfig{
						Enabled:             base.BoolPtr(true),
						CollationBufferSize: base.IntPtr(0), // avoid data race in collation with FlushLogBuffers test code
					},
				},
				Console: &base.ConsoleLoggerConfig{
					FileLoggerConfig: base.FileLoggerConfig{
						Enabled: base.BoolPtr(true),
					},
				},
				Info: &base.FileLoggerConfig{
					Enabled:             base.BoolPtr(false),
					CollationBufferSize: base.IntPtr(0), // avoid data race in collation with FlushLogBuffers test code
				},
				Debug: &base.FileLoggerConfig{
					Enabled:             base.BoolPtr(false),
					CollationBufferSize: base.IntPtr(0), // avoid data race in collation with FlushLogBuffers test code
				},
				Trace: &base.FileLoggerConfig{
					Enabled:             base.BoolPtr(false),
					CollationBufferSize: base.IntPtr(0), // avoid data race in collation with FlushLogBuffers test code
				},
			}
			require.NoError(t, config.SetupAndValidateLogging(base.TestCtx(t)))
		},
	})
	defer rt.Close()

	// initialize RestTester
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	testCases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{
			name:   "update sync function",
			method: http.MethodPut,
			path:   "/{{.keyspace}}/_config/sync",
			body:   `function(doc){}`,
		},
		{
			name:   "update import filter",
			method: http.MethodPut,
			path:   "/{{.keyspace}}/_config/import_filter",
			body:   `function(doc){}`,
		},
		{
			name:   "delete sync function",
			method: http.MethodDelete,
			path:   "/{{.keyspace}}/_config/sync",
			body:   ``,
		},
		{
			name:   "delete import filter",
			method: http.MethodDelete,
			path:   "/{{.keyspace}}/_config/import_filter",
			body:   ``,
		},
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			output := base.AuditLogContents(t, func(t testing.TB) {
				resp := rt.SendAdminRequest(testCase.method, testCase.path, testCase.body)
				RequireStatus(t, resp, http.StatusOK)
			})
			requireValidDatabaseUpdatedEventPayload(rt, output)
		})
	}
}

// requireValidDatabaseUpdatedEventPayload checks the audit log output for at least one database updated event, and validates that each payload can be used validly
func requireValidDatabaseUpdatedEventPayload(rt *RestTester, output []byte) {
	events := bytes.Split(output, []byte("\n"))
	foundEvent := false
	for _, rawEvent := range events {
		if bytes.TrimSpace(rawEvent) == nil {
			continue
		}
		var event struct {
			Payload string `json:"payload"`
		}
		require.NoError(rt.TB(), base.JSONUnmarshal(rawEvent, &event))
		require.NotEmpty(rt.TB(), event.Payload)
		RequireStatus(rt.TB(), rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", event.Payload), http.StatusCreated)
		foundEvent = true
	}
	require.True(rt.TB(), foundEvent, "expected audit event not found")
}

func TestRedactConfigAsStr(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
			hasError: true,
		},
		{
			name:     "simple",
			input:    `{"key":"value"}`,
			expected: `{"key":"value"}`,
		},
		{
			name:     "dbConfig",
			input:    string(base.MustJSONMarshal(t, DbConfig{})),
			expected: `{}`,
		},
		{
			name: "dbConfig with password",
			input: string(base.MustJSONMarshal(t, DbConfig{
				BucketConfig: BucketConfig{
					Password: "password",
				},
			})),
			expected: `{"password":"xxxxx"}`,
		},
		{
			name: "dbConfig with password and username",
			input: string(base.MustJSONMarshal(t, DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					"alice": {
						Password: base.StringPtr("password1"),
					},
					"bob": {
						Password: base.StringPtr("password2"),
					},
				},
			})),
			expected: `{"users":{"alice":{"password":"xxxxx"},"bob":{"password":"xxxxx"}}}`,
		},
		{
			name: "dbConfig with replication username and password",
			input: string(base.MustJSONMarshal(t, DbConfig{
				Replications: map[string]*db.ReplicationConfig{
					"replication1": &db.ReplicationConfig{
						Username:       "alice1",
						Password:       "password1",
						RemotePassword: "hunter2",
					},
				},
			})),
			expected: `{"replications":{"replication1":{"replication_id":"","remote":"","username":"alice1","password":"xxxxx","remote_password":"xxxxx","direction":"","continuous":false}}}`,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			output, err := redactConfigAsStr(ctx, testCase.input)
			if testCase.hasError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expected, output)

		})
	}
}
