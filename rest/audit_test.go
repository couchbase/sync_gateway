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
	"fmt"
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

	if !base.IsEnterpriseEdition() {
		t.Skip("Audit logging only works in EE")
	}

	// get tempdir before resetting global loggers, since the logger cleanup needs to happen before deletion
	tempdir := t.TempDir()
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()

	const (
		requestInfoHeaderName  = "extra-audit-logging-header"
		requestUser            = "alice"
		filteredPublicUsername = "bob"
		filteredAdminUsername  = "TestAuditLoggingFields-charlie"
		filteredAdminPassword  = "password"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:                 true,
		AdminInterfaceAuthentication: !base.UnitTestUrlIsWalrus(), // disable admin auth for walrus so we can get coverage of both subtests
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

	runServerRBACTests := base.TestCanUseMobileRBAC(t)

	dbConfig := rt.NewDbConfig()
	dbConfig.Logging = &DbLoggingConfig{
		Audit: &DbAuditLoggingConfig{
			Enabled: base.BoolPtr(true),
			DisabledUsers: []base.AuditLoggingPrincipal{
				{Name: filteredPublicUsername, Domain: string(base.UserDomainSyncGateway)},
				{Name: filteredAdminUsername, Domain: string(base.UserDomainCBServer)},
			},
		},
	}

	// initialize RestTester
	RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	rt.CreateUser(requestUser, nil)
	rt.CreateUser(filteredPublicUsername, nil)
	if runServerRBACTests {
		eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
		require.NoError(t, err)
		MakeUser(t, httpClient, eps[0], filteredAdminUsername, filteredAdminPassword, []string{fmt.Sprintf("%s[%s]", MobileSyncGatewayRole.RoleName, rt.Bucket().GetName())})
		defer DeleteUser(t, httpClient, eps[0], filteredAdminUsername)
	}

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
		},
		{
			name: "public request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendRequest(http.MethodGet, "/", ""), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDPublicHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/",
				},
			},
		},
		{
			name: "guest request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendRequest(http.MethodGet, "/db/", ""), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDPublicUserAuthenticated: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldAuthMethod:    "guest",
				},
				base.AuditIDReadDatabase: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "sgw", "user": base.GuestUsername},
				},
				base.AuditIDPublicHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
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
				base.AuditIDPublicHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
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
				base.AuditIDPublicHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
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
				base.AuditIDPublicHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
			},
		},
		{
			name: "anon admin request",
			auditableAction: func(t testing.TB) {
				if rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be disabled")
				}
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
				base.AuditIDAdminHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
			},
		},
		{
			name: "authed admin request",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth")
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
				base.AuditIDAdminHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
			},
		},
		{
			name: "filtered public request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/db/", "", filteredPublicUsername), http.StatusOK)
			},
		},
		{
			name: "filtered admin request",
			auditableAction: func(t testing.TB) {
				if !runServerRBACTests {
					t.Skip("Skipping subtest that requires admin RBAC")
				}
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth")
				}
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", filteredAdminUsername, filteredAdminPassword), http.StatusOK)
			},
		},
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			output := base.AuditLogContents(t, testCase.auditableAction)
			events := jsonLines(t, output)

			assert.Equalf(t, len(testCase.expectedAuditEventFields), len(events), "expected exactly %d audit events, got %d", len(testCase.expectedAuditEventFields), len(events))

			// for each expected event, check that it's present and the fields match what we expected
			for expectedAuditID, expectedEventFields := range testCase.expectedAuditEventFields {
				eventFound := false
				for _, event := range events {
					id, ok := event["id"]
					if !assert.Truef(t, ok, "audit event did not contain \"id\" field: %v", event) {
						continue
					}

					auditID := base.AuditID(id.(float64))
					if expectedAuditID == auditID {
						eventFound = true
						for k, expectedVal := range expectedEventFields {
							eventField, ok := event[k]
							if !assert.Truef(t, ok, "missing field %q in audit event %q (%s)", k, auditID, base.AuditEvents[auditID].Name) {
								continue
							}
							if expectedVal != auditFieldValueIgnored {
								assert.Equalf(t, expectedVal, eventField, "unexpected value for field %q in audit event %q (%s)", k, auditID, base.AuditEvents[auditID].Name)
							}
						}
					}
				}
				assert.Truef(t, eventFound, "expected event %v not found in set of events", expectedAuditID)
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
			ID      base.AuditID `json:"id"`
			Payload string       `json:"payload"`
		}
		require.NoError(rt.TB(), base.JSONUnmarshal(rawEvent, &event))
		if event.ID != base.AuditIDUpdateDatabaseConfig {
			continue
		}
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
			require.JSONEq(t, testCase.expected, output)

		})
	}
}

func TestEffectiveUserID(t *testing.T) {
	tempdir := t.TempDir()
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()
	const (
		user       = "user"
		domain     = "domain"
		cnfDomain  = "myDomain"
		cnfUser    = "bob"
		realUser   = "alice"
		realDomain = "sgw"
	)
	reqHeaders := map[string]string{
		"user_header":   fmt.Sprintf(`{"%s": "%s", "%s":"%s"}`, domain, cnfDomain, user, cnfUser),
		"Authorization": getBasicAuthHeader(realUser, RestTesterDefaultUserPassword),
	}

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:     true,
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Unsupported.EffectiveUserHeaderName = base.StringPtr("user_header")
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
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	rt.CreateUser(realUser, nil)

	action := func(t testing.TB) {
		RequireStatus(t, rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/", "", reqHeaders), http.StatusOK)
	}
	output := base.AuditLogContents(t, action)
	events := jsonLines(t, output)

	for _, event := range events {
		effective := event[base.AuditEffectiveUserID].(map[string]any)
		assert.Equal(t, cnfDomain, effective[domain])
		assert.Equal(t, cnfUser, effective[user])
		realUserEvent := event[base.AuditFieldRealUserID].(map[string]any)
		assert.Equal(t, realDomain, realUserEvent[domain])
		assert.Equal(t, realUser, realUserEvent[user])
	}
}
