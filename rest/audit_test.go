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
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLoggingFields(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("Audit logging only works in EE")
	}

	// get tempdir before resetting global loggers, since the logger cleanup needs to happen before deletion
	tempdir := t.TempDir()
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()

	const (
		requestInfoHeaderName       = "extra-audit-logging-header"
		requestUser                 = "alice"
		filteredPublicUsername      = "bob"
		filteredPublicRoleUsername  = "charlie"
		filteredPublicRoleName      = "observer"
		filteredAdminUsername       = "TestAuditLoggingFields-charlie"
		unfilteredAdminRoleUsername = "TestAuditLoggingFields-diana"
		filteredAdminRoleUsername   = "TestAuditLoggingFields-bob"
		unauthorizedAdminUsername   = "TestAuditLoggingFields-alice"
	)
	var (
		filteredAdminRoleName = BucketFullAccessRole.RoleName
	)

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:                   true,
		AdminInterfaceAuthentication:   !base.UnitTestUrlIsWalrus(), // disable admin auth for walrus so we can get coverage of both subtests
		metricsInterfaceAuthentication: true,
		PersistentConfig:               true,
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
					EnabledEvents: base.AllGlobalAuditeventIDs, // enable everything for testing
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
			Enabled:       base.BoolPtr(true),
			EnabledEvents: &base.AllDbAuditeventIDs, // enable everything for testing
			DisabledUsers: []base.AuditLoggingPrincipal{
				{Name: filteredPublicUsername, Domain: string(base.UserDomainSyncGateway)},
				{Name: filteredAdminUsername, Domain: string(base.UserDomainCBServer)},
			},
			DisabledRoles: []base.AuditLoggingPrincipal{
				{Name: filteredPublicRoleName, Domain: string(base.UserDomainSyncGateway)},
				{Name: filteredAdminRoleName, Domain: string(base.UserDomainCBServer)},
			},
		},
	}

	// initialize RestTester
	RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	rt.CreateUser(requestUser, nil)
	rt.CreateUser(filteredPublicUsername, nil)
	rt.CreateRole(filteredPublicRoleName, []string{channels.AllChannelWildcard})
	rt.CreateUser(filteredPublicRoleUsername, nil, filteredPublicRoleName)
	if runServerRBACTests {
		eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
		require.NoError(t, err)

		MakeUser(t, httpClient, eps[0], filteredAdminUsername, RestTesterDefaultUserPassword, []string{
			fmt.Sprintf("%s[%s]", MobileSyncGatewayRole.RoleName, rt.Bucket().GetName()),
		})
		defer DeleteUser(t, httpClient, eps[0], filteredAdminUsername)
		MakeUser(t, httpClient, eps[0], filteredAdminRoleUsername, RestTesterDefaultUserPassword, []string{
			fmt.Sprintf("%s[%s]", filteredAdminRoleName, rt.Bucket().GetName()),
		})
		defer DeleteUser(t, httpClient, eps[0], filteredAdminRoleUsername)
		MakeUser(t, httpClient, eps[0], unauthorizedAdminUsername, RestTesterDefaultUserPassword, []string{})
		defer DeleteUser(t, httpClient, eps[0], unauthorizedAdminUsername)

		// if we have another bucket available, use it to test cross-bucket role filtering (to ensure it doesn't)
		if base.GTestBucketPool.NumUsableBuckets() >= 2 {
			differentBucket := base.GetTestBucket(t)
			defer differentBucket.Close(base.TestCtx(t))
			differentBucketName := differentBucket.GetName()

			MakeUser(t, httpClient, eps[0], unfilteredAdminRoleUsername, RestTesterDefaultUserPassword, []string{
				fmt.Sprintf("%s[%s]", filteredAdminRoleName, differentBucketName),
				fmt.Sprintf("%s[%s]", MobileSyncGatewayRole.RoleName, rt.Bucket().GetName()),
			})
			defer DeleteUser(t, httpClient, eps[0], unfilteredAdminRoleUsername)
		}
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
			name: "admin silent request, admin authentication enabled",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be enabled")
				}
				RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/_expvar", ""), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthenticationFailed: {
					base.AuditFieldUserName: "",
				},
			},
		},
		{
			name: "admin silent request authenticated",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/_expvar", "", base.TestClusterUsername(), base.TestClusterPassword()), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDSyncGatewayStats: {
					base.AuditFieldStatsFormat: "expvar",
				},
			},
		},
		{
			name: "admin silent request bad creds",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be enabled")
				}
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/_expvar", "", "not a real user", base.TestClusterPassword()), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthenticationFailed: {
					base.AuditFieldUserName: "not a real user",
				},
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
			name: "anon admin request, admin authentication disabled",
			auditableAction: func(t testing.TB) {
				if rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be disabled")
				}
				RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/db/", ""), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
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
			name: "anon admin request, rejected",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be enabled")
				}
				RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/db/", ""), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
				base.AuditIDAdminUserAuthenticationFailed: {
					base.AuditFieldUserName: "",
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
				base.AuditIDAdminUserAuthenticated: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "cbs", "user": base.TestClusterUsername()},
				},
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
			name: "admin request bad credentials",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth")
				}
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", "not_a_user", "not_a_password"), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthenticationFailed: {
					base.AuditFieldUserName: "not_a_user",
				},
				base.AuditIDAdminHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
			},
		},
		{
			name: "admin request not authorized",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth")
				}
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", unauthorizedAdminUsername, RestTesterDefaultUserPassword), http.StatusForbidden)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthorizationFailed: {
					base.AuditFieldUserName: unauthorizedAdminUsername,
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
			name: "filtered public role request",
			auditableAction: func(t testing.TB) {
				RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/db/", "", filteredPublicRoleUsername), http.StatusOK)
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
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", filteredAdminUsername, RestTesterDefaultUserPassword), http.StatusOK)
			},
		},
		{
			name: "filtered admin role request",
			auditableAction: func(t testing.TB) {
				if !runServerRBACTests {
					t.Skip("Skipping subtest that requires admin RBAC")
				}
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth")
				}
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", filteredAdminRoleUsername, RestTesterDefaultUserPassword), http.StatusOK)
			},
		},
		{
			name: "authed admin request role filtered on different bucket",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth")
				}
				base.RequireNumTestBuckets(t, 2)
				RequireStatus(t, rt.SendAdminRequestWithAuth(http.MethodGet, "/db/", "", unfilteredAdminRoleUsername, RestTesterDefaultUserPassword), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthenticated: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "cbs", "user": unfilteredAdminRoleUsername},
				},
				base.AuditIDReadDatabase: {
					base.AuditFieldCorrelationID: auditFieldValueIgnored,
					base.AuditFieldRealUserID:    map[string]any{"domain": "cbs", "user": unfilteredAdminRoleUsername},
				},
				base.AuditIDAdminHTTPAPIRequest: {
					base.AuditFieldHTTPMethod: http.MethodGet,
					base.AuditFieldHTTPPath:   "/db/",
				},
			},
		},
		{
			name: "metrics request authenticated",
			auditableAction: func(t testing.TB) {
				headers := map[string]string{
					"Authorization": getBasicAuthHeader(base.TestClusterUsername(), base.TestClusterPassword()),
				}
				RequireStatus(t, rt.SendMetricsRequestWithHeaders(http.MethodGet, "/_metrics", "", headers), http.StatusOK)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDSyncGatewayStats: {
					base.AuditFieldStatsFormat: "prometheus",
				},
			},
		},
		{
			name: "metrics request no authentication",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be enabled")
				}
				RequireStatus(t, rt.SendMetricsRequestWithHeaders(http.MethodGet, "/_metrics", "", nil), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthenticationFailed: {
					base.AuditFieldUserName: "",
				},
			},
		},
		{
			name: "metrics request bad credentials",
			auditableAction: func(t testing.TB) {
				if !rt.AdminInterfaceAuthentication {
					t.Skip("Skipping subtest that requires admin auth to be enabled")
				}
				headers := map[string]string{
					"Authorization": getBasicAuthHeader("notauser", base.TestClusterPassword()),
				}
				RequireStatus(t, rt.SendMetricsRequestWithHeaders(http.MethodGet, "/_metrics", "", headers), http.StatusUnauthorized)
			},
			expectedAuditEventFields: map[base.AuditID]base.AuditFields{
				base.AuditIDAdminUserAuthenticationFailed: {
					base.AuditFieldUserName: "notauser",
				},
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
				assert.Truef(t, eventFound, "expected event %s:%v not found in set of events", base.AuditEvents[expectedAuditID].Name, expectedAuditID)
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
		headerUser   = "user"
		headerDomain = "domain"
		cnfDomain    = "myDomain"
		cnfUser      = "bob"
		realUser     = "alice"
		realDomain   = "sgw"
	)
	reqHeaders := map[string]string{
		"user_header":   fmt.Sprintf(`{"%s": "%s", "%s":"%s"}`, headerDomain, cnfDomain, headerUser, cnfUser),
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
		eventID := base.AuditID(event[base.AuditFieldID].(float64))
		// ignore events that don't have user info ()
		if eventID != base.AuditIDPublicUserAuthenticated && eventID != base.AuditIDReadDatabase {
			continue
		}

		effective := event[base.AuditEffectiveUserID].(map[string]any)
		assert.Equal(t, cnfDomain, effective[base.AuditFieldEffectiveUserIDDomain])
		assert.Equal(t, cnfUser, effective[base.AuditFieldEffectiveUserIDUser])
		realUserEvent := event[base.AuditFieldRealUserID].(map[string]any)
		assert.Equal(t, realDomain, realUserEvent[base.AuditFieldRealUserIDDomain])
		assert.Equal(t, realUser, realUserEvent[base.AuditFieldRealUserIDUser])
	}
}

func TestAuditDocumentRead(t *testing.T) {
	rt := createAuditLoggingRestTester(t)
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	const docID = "doc1"
	docVersion := rt.CreateTestDoc(docID)

	type testCase struct {
		name                 string
		method               string
		path                 string
		requestBody          string
		docID                string
		docReadVersions      []string
		docMetadataReadCount int
	}
	testCases := []testCase{
		{
			name:            "get doc",
			method:          http.MethodGet,
			path:            "/{{.keyspace}}/doc1",
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		{
			name:            "get doc with rev",
			method:          http.MethodGet,
			path:            "/{{.keyspace}}/doc1?rev=" + docVersion.RevID,
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		{
			name:            "get doc with openrevs",
			method:          http.MethodGet,
			path:            "/{{.keyspace}}/doc1?open_revs=all",
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		{
			name:   "_bulk_get",
			method: http.MethodPost,
			path:   "/{{.keyspace}}/_bulk_get",
			requestBody: string(base.MustJSONMarshal(t, db.Body{
				"docs": []db.Body{
					{"id": "doc1", "rev": docVersion.RevID},
				},
			})),
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		{
			// this doesn't actually provide the document body, no audit events
			name:   "_revs_diff",
			method: http.MethodPost,
			path:   "/{{.keyspace}}/_revs_diff",
			requestBody: string(base.MustJSONMarshal(t, db.Body{
				"doc1": []string{docVersion.RevID},
			})),
			docID:           "doc1",
			docReadVersions: nil,
		},
		{
			name:                 "all_docs without body",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_all_docs",
			docID:                "doc1",
			docReadVersions:      nil,
			docMetadataReadCount: 0,
		},
		{
			name:            "all_docs",
			method:          http.MethodGet,
			path:            "/{{.keyspace}}/_all_docs?include_docs=true",
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		{
			name:                 "all_docs with include_docs=true&channels=true",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_all_docs?include_docs=true&channels=true",
			docID:                "doc1",
			docReadVersions:      []string{docVersion.RevID},
			docMetadataReadCount: 1,
		},
		{
			name:                 "all_docs with channels=true",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_all_docs?channels=true",
			docID:                "doc1",
			docReadVersions:      nil,
			docMetadataReadCount: 1,
		},
		{
			name:                 "all_docs with update_seq=true",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_all_docs?update_seq=true",
			docID:                "doc1",
			docReadVersions:      nil,
			docMetadataReadCount: 1,
		},
		{
			name:                 "all_docs with revs=true",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_all_docs?revs=true",
			docID:                "doc1",
			docReadVersions:      nil,
			docMetadataReadCount: 1,
		},
		{
			name:   "changes no bodies, no audit",
			method: http.MethodGet,
			path:   "/{{.keyspace}}/_changes?since=0",
			docID:  "doc1",
		},
		{
			name:            "changes with include_docs",
			method:          http.MethodGet,
			path:            "/{{.keyspace}}/_changes?since=0&include_docs=true",
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		{
			name:                 "raw",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_raw/doc1",
			docID:                "doc1",
			docReadVersions:      []string{docVersion.RevID},
			docMetadataReadCount: 1,
		},
		{
			name:   "raw, include doc=false",
			method: http.MethodGet,
			path:   "/{{.keyspace}}/_raw/doc1?include_doc=false",
			docID:  "doc1",
			// raw endpoint issues metadata and not doc read events
			docReadVersions:      nil,
			docMetadataReadCount: 1,
		},
		{
			name:                 "revtree",
			method:               http.MethodGet,
			path:                 "/{{.keyspace}}/_revtree/doc1",
			docID:                "doc1",
			docReadVersions:      nil,
			docMetadataReadCount: 1,
		},
	}
	if base.IsEnterpriseEdition() {
		testCases = append(testCases, testCase{
			name:            "get doc with replicator2",
			method:          http.MethodGet,
			path:            "/{{.keyspace}}/doc1?replicator2=true",
			docID:           "doc1",
			docReadVersions: []string{docVersion.RevID},
		},
		)
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			output := base.AuditLogContents(t, func(t testing.TB) {
				resp := rt.SendAdminRequest(testCase.method, testCase.path, testCase.requestBody)
				RequireStatus(t, resp, http.StatusOK)
			})
			requireDocumentReadEvents(rt, output, testCase.docID, testCase.docReadVersions)
			requireDocumentMetadataReadEvents(rt, output, testCase.docID, docVersion.RevID, testCase.docMetadataReadCount)
		})
	}
}

func TestAuditAttachmentEvents(t *testing.T) {
	rt := createAuditLoggingRestTester(t)
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	testCases := []struct {
		name                  string
		setupCode             func(t testing.TB, docID string) DocVersion
		auditableCode         func(t testing.TB, docID string, docVersion DocVersion)
		attachmentCreateCount int
		attachmentReadCount   int
		attachmentUpdateCount int
		attachmentDeleteCount int
	}{
		{
			name: "add attachment",
			setupCode: func(_ testing.TB, docID string) DocVersion {
				return rt.CreateTestDoc(docID)
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+docVersion.RevID, "content"), http.StatusCreated)
			},
			attachmentCreateCount: 1,
		},
		{
			name: "add inline attachment",
			setupCode: func(_ testing.TB, docID string) DocVersion {
				return rt.CreateTestDoc(docID)
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+docVersion.RevID, `{"_attachments":{"attachment1":{"data": "YQ=="}}}`), http.StatusCreated)
			},
			attachmentCreateCount: 1,
		},
		{
			name: "get attachment with rev",
			setupCode: func(t testing.TB, docID string) DocVersion {
				initialDocVersion := rt.CreateTestDoc(docID)
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+initialDocVersion.RevID, "contentdoc2"), http.StatusCreated)
				docVersion, _ := rt.GetDoc(docID)
				return docVersion
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"/attachment1?rev="+docVersion.RevID, ""), http.StatusOK)
			},
			attachmentReadCount: 1,
		},
		{
			name: "bulk_get attachment with rev",
			setupCode: func(t testing.TB, docID string) DocVersion {
				initialDocVersion := rt.CreateTestDoc(docID)
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+initialDocVersion.RevID, "contentdoc2"), http.StatusCreated)
				docVersion, _ := rt.GetDoc(docID)
				return docVersion
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				body := string(base.MustJSONMarshal(t, db.Body{
					"docs": []db.Body{
						{"id": docID, "rev": docVersion.RevID},
					},
				}))
				RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.keyspace}}/_bulk_get?attachments=true", body), http.StatusOK)
			},

			attachmentReadCount: 1,
		},
		{
			name: "all_docs attachment with rev",
			setupCode: func(_ testing.TB, docID string) DocVersion {
				return rt.CreateTestDoc(docID)
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_all_docs?include_docs=true", ""), http.StatusOK)
			},
		},
		{
			name: "update attachment",
			setupCode: func(t testing.TB, docID string) DocVersion {
				initialDocVersion := rt.CreateTestDoc(docID)
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+initialDocVersion.RevID, "contentdoc2"), http.StatusCreated)
				docVersion, _ := rt.GetDoc(docID)
				return docVersion
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+docVersion.RevID, "content-update"), http.StatusCreated)
			},
			attachmentUpdateCount: 1,
		},
		{
			name: "update inline attachment",
			setupCode: func(t testing.TB, docID string) DocVersion {
				initialDocVersion := rt.CreateTestDoc(docID)
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+initialDocVersion.RevID, "contentdoc2"), http.StatusCreated)
				docVersion, _ := rt.GetDoc(docID)
				return docVersion
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+docVersion.RevID, `{"_attachments":{"attachment1":{"data": "YQ=="}}}`), http.StatusCreated)
			},
			attachmentUpdateCount: 1,
		},
		{
			name: "delete attachment",
			setupCode: func(t testing.TB, docID string) DocVersion {
				initialDocVersion := rt.CreateTestDoc(docID)
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+initialDocVersion.RevID, "contentdoc2"), http.StatusCreated)
				docVersion, _ := rt.GetDoc(docID)
				return docVersion
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/"+docID+"/attachment1?rev="+docVersion.RevID, ""), http.StatusOK)
			},
			attachmentDeleteCount: 1,
		},
		{
			name: "delete inline attachment",
			setupCode: func(t testing.TB, docID string) DocVersion {
				initialDocVersion := rt.CreateTestDoc(docID)
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"/attachment1?rev="+initialDocVersion.RevID, "contentdoc2"), http.StatusCreated)
				docVersion, _ := rt.GetDoc(docID)
				return docVersion
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+docVersion.RevID, `{"foo": "bar", "_attachments":{}}`), http.StatusCreated)
			},
			attachmentDeleteCount: 1,
		},
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			attachmentName := "attachment1"
			docID := strings.ReplaceAll(testCase.name, " ", "_")
			docVersion := testCase.setupCode(t, docID)
			output := base.AuditLogContents(t, func(t testing.TB) {
				testCase.auditableCode(t, docID, docVersion)
			})
			postAttachmentVersion, _ := rt.GetDoc(docID)

			requireAttachmentEvents(rt, base.AuditIDAttachmentDelete, output, docID, postAttachmentVersion.RevID, attachmentName, testCase.attachmentDeleteCount)
		})
	}
}

func TestAuditDocumentCreateUpdateEvents(t *testing.T) {
	rt := createAuditLoggingRestTester(t)
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	if base.TestUseXattrs() {
		// this is not set automatically for CE
		dbConfig.AutoImport = base.BoolPtr(true)
	}
	RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)
	type testCase struct {
		name                string
		setupCode           func(t testing.TB, docID string) DocVersion
		auditableCode       func(t testing.TB, docID string, docVersion DocVersion)
		documentCreateCount int
		documentUpdateCount int
	}
	testCases := []testCase{
		{
			name: "create doc",
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"foo": "bar"}`), http.StatusCreated)
			},
			documentCreateCount: 1,
		},
		{
			name: "update doc",
			setupCode: func(_ testing.TB, docID string) DocVersion {
				return rt.CreateTestDoc(docID)
			},
			auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+docVersion.RevID, `{"foo": "bar"}`), http.StatusCreated)
			},
			documentUpdateCount: 1,
		},
	}
	if base.TestUseXattrs() {
		testCases = append(testCases, []testCase{
			{
				name: "import doc",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					importCount := rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value()
					_, err := rt.GetSingleDataStore().Add(docID, 0, db.Body{"foo": "bar"})
					require.NoError(t, err)
					base.RequireWaitForStat(t, func() int64 {
						return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value()
					}, importCount+1)
				},
			},
			{
				name: "import doc with inline sync meta",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					_, err := rt.GetSingleDataStore().Add(docID, 0, []byte(db.RawDocWithInlineSyncData(t)))
					require.NoError(t, err)
					// this may get picked up by auto import or on demand import
					_, _ = rt.GetDoc(docID)
				},
			}}...)
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			docID := strings.ReplaceAll(testCase.name, " ", "_")
			var docVersion DocVersion
			if testCase.setupCode != nil {
				docVersion = testCase.setupCode(t, docID)
			}
			output := base.AuditLogContents(t, func(t testing.TB) {
				testCase.auditableCode(t, docID, docVersion)
			})
			postAttachmentVersion, _ := rt.GetDoc(docID)
			requireDocumentEvents(rt, base.AuditIDDocumentCreate, output, docID, postAttachmentVersion.RevID, testCase.documentCreateCount)
			requireDocumentEvents(rt, base.AuditIDDocumentUpdate, output, docID, postAttachmentVersion.RevID, testCase.documentUpdateCount)
		})
	}
}

func TestAuditChangesFeedStart(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {

		rt := createAuditLoggingRestTester(t)
		defer rt.Close()

		RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		const (
			requestUser = "alice"
		)

		rt.CreateUser(requestUser, []string{"*"})

		type testCase struct {
			name           string
			auditableCode  func(t testing.TB, docID string, docVersion DocVersion)
			expectedFields map[string]any // expected fields on changes audit event
		}
		testCases := []testCase{
			{
				name: "get changes",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes", "", requestUser), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType: "normal",
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "get changes since normal, feed type",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes?since=10&feed=normal", "", requestUser), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType: "normal",
					base.AuditFieldSince:    "10",
				},
			},
			{
				name: "get changes compound since, include_docs",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes?since=5:10&include_docs=true", "", requestUser), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType:    "normal",
					base.AuditFieldIncludeDocs: true,
					base.AuditFieldSince:       "5:10",
				},
			},
			{
				name: "get changes channel filters",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes?filter="+base.ByChannelFilter+"&channels=A,B", "", requestUser), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldChannels: []any{"A", "B"},
					base.AuditFieldFeedType: "normal",
					base.AuditFieldFilter:   base.ByChannelFilter,
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "get changes docid filters",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes?filter="+base.DocIDsFilter+"&doc_ids=doc1,doc2", "", requestUser), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldDocIDs:   []any{"doc1", "doc2"},
					base.AuditFieldFeedType: "normal",
					base.AuditFieldFilter:   base.DocIDsFilter,
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "post changes",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					requestBody := `{"feed":"normal", "since":10,"filter":"sync_gateway/bychannel","channels":"A,B"}`
					RequireStatus(t, rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", requestBody, requestUser), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldChannels: []any{"A", "B"},
					base.AuditFieldFeedType: "normal",
					base.AuditFieldFilter:   base.ByChannelFilter,
					base.AuditFieldSince:    "10",
				},
			},
			{
				name: "get changes admin",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_changes?since=10", ""), http.StatusOK)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType: "normal",
					base.AuditFieldSince:    "10",
				},
			},
			{
				name: "blip changes continuous",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					require.NoError(t, btcRunner.StartPull(btc.id))
					btcRunner.WaitForVersion(btc.id, docID, docVersion)
					_, err := btcRunner.UnsubPullChanges(btc.id)
					require.NoError(t, err)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType: "continuous",
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "blip changes one shot",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					require.NoError(t, btcRunner.StartOneshotPull(btc.id))
					btcRunner.WaitForVersion(btc.id, docID, docVersion)
					_, err := btcRunner.UnsubPullChanges(btc.id)
					require.NoError(t, err)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType: "normal",
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "blip changes with channels",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					require.NoError(t, btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Since: "0", Channels: "A,B"}))
					btcRunner.WaitForVersion(btc.id, docID, docVersion)
					_, err := btcRunner.UnsubPullChanges(btc.id)
					require.NoError(t, err)
				},
				expectedFields: map[string]any{
					base.AuditFieldChannels: []any{"A", "B"},
					base.AuditFieldFeedType: "normal",
					base.AuditFieldFilter:   base.ByChannelFilter,
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "blip changes with docids",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					require.NoError(t, btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Since: "0", DocIDs: []string{docID, "non_existent"}}))
					btcRunner.WaitForVersion(btc.id, docID, docVersion)
					_, err := btcRunner.UnsubPullChanges(btc.id)
					require.NoError(t, err)
				},
				expectedFields: map[string]any{
					base.AuditFieldDocIDs:   []any{"blip_changes_with_docids", "non_existent"},
					base.AuditFieldFeedType: "normal",
					base.AuditFieldFilter:   base.DocIDsFilter,
					base.AuditFieldSince:    "0",
				},
			},
			{
				// invalid specification, this would ignore channel filter and only use docids filter
				name: "blip changes with docids and channels",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					require.NoError(t, btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Since: "0", DocIDs: []string{docID, "non_existent"}, Channels: "A,B"}))
					btcRunner.WaitForVersion(btc.id, docID, docVersion)
					_, err := btcRunner.UnsubPullChanges(btc.id)
					require.NoError(t, err)
				},
				expectedFields: map[string]any{
					base.AuditFieldDocIDs:   []any{"blip_changes_with_docids_and_channels", "non_existent"},
					base.AuditFieldChannels: []any{"A", "B"},
					base.AuditFieldFeedType: "normal",
					base.AuditFieldFilter:   base.DocIDsFilter,
					base.AuditFieldSince:    "0",
				},
			},
			{
				name: "blip changes with compound since",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					require.NoError(t, btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Since: "1:10"}))
					btcRunner.WaitForVersion(btc.id, docID, docVersion)
					_, err := btcRunner.UnsubPullChanges(btc.id)
					require.NoError(t, err)
				},
				expectedFields: map[string]any{
					base.AuditFieldFeedType: "normal",
					base.AuditFieldSince:    "1:10",
				},
			},
		}
		for _, testCase := range testCases {
			rt.Run(testCase.name, func(t *testing.T) {
				docID := strings.ReplaceAll(testCase.name, " ", "_")
				docVersion := rt.PutDoc(docID, `{"channels": "A"}`)
				output := base.AuditLogContents(t, func(t testing.TB) {
					testCase.auditableCode(t, docID, docVersion)
				})

				requireChangesStartEvent(rt.TB(), output, testCase.expectedFields)
			})
		}
	})
}

// requireDocumentMetadataReadEvents validates that there read events for each doc version specified. There should be only audit events for a given docid.
func requireDocumentMetadataReadEvents(rt *RestTester, output []byte, docID string, revid string, count int) {
	events := jsonLines(rt.TB(), output)
	countFound := 0
	for _, event := range events {
		// skip events that are the target eventID
		if base.AuditID(event[base.AuditFieldID].(float64)) != base.AuditIDDocumentMetadataRead {
			continue
		}
		require.Equal(rt.TB(), event[base.AuditFieldDocID], docID)
		countFound++
	}
	require.Equal(rt.TB(), count, countFound)
}

// requireDocumentReadEvents validates that there read events for each doc version specified. There should be only audit events for a given docid and it should be revid specified.
func requireDocumentReadEvents(rt *RestTester, output []byte, docID string, docVersions []string) {
	events := jsonLines(rt.TB(), output)
	var docVersionsFound []string
	for _, event := range events {
		// skip events that are the target eventID
		if base.AuditID(event[base.AuditFieldID].(float64)) != base.AuditIDDocumentRead {
			continue
		}
		require.Equal(rt.TB(), event[base.AuditFieldDocID], docID)
		docVersionsFound = append(docVersionsFound, event[base.AuditFieldDocVersion].(string))
	}
	require.Len(rt.TB(), docVersions, len(docVersionsFound), "expected exactly %d document read events, got %d", len(docVersions), len(docVersionsFound))
	require.Equal(rt.TB(), docVersions, docVersionsFound)
}

// requireAttachmentEvents validates that an attachment CRUD event occurred in the right number only on the correct document.
func requireAttachmentEvents(rt *RestTester, eventID base.AuditID, output []byte, docID, docVersion string, attachmentName string, count int) {
	events := jsonLines(rt.TB(), output)
	countFound := 0
	for _, event := range events {
		// skip events that are the target eventID
		if base.AuditID(event[base.AuditFieldID].(float64)) != eventID {
			continue
		}
		require.Equal(rt.TB(), event[base.AuditFieldDocID], docID)
		require.Equal(rt.TB(), docVersion, event[base.AuditFieldDocVersion].(string))
		require.Equal(rt.TB(), attachmentName, event[base.AuditFieldAttachmentID])
		countFound++
	}
	require.Equal(rt.TB(), count, countFound, "expected exactly %d %s events, got %d", count, base.AuditEvents[eventID].Name, countFound)
}

// requireDocumentEvents validates that a document CRUD event occurred on the right doc ID with the correct channels.
func requireDocumentEvents(rt *RestTester, eventID base.AuditID, output []byte, docID, docVersion string, count int) {
	events := jsonLines(rt.TB(), output)
	countFound := 0
	for _, event := range events {
		// skip events that are the target eventID
		if base.AuditID(event[base.AuditFieldID].(float64)) != eventID {
			continue
		}
		require.Equal(rt.TB(), event[base.AuditFieldDocID], docID)
		require.Equal(rt.TB(), docVersion, event[base.AuditFieldDocVersion].(string))
		countFound++
	}
	require.Equal(rt.TB(), count, countFound, "expected exactly %d %s events, got %d", count, base.AuditEvents[eventID].Name, countFound)
}

// requireChangesStartEvent validates that there is a changes start event with the specified fields
func requireChangesStartEvent(t testing.TB, output []byte, expectedFields map[string]any) {
	events := jsonLines(t, output)
	found := false
	for _, event := range events {

		if base.AuditID(event[base.AuditFieldID].(float64)) != base.AuditIDChangesFeedStarted {
			continue
		}
		require.False(t, found, "Received more than one changes feed started event")
		found = true

		for fieldID, expectedValue := range expectedFields {
			value, ok := event[fieldID]
			require.True(t, ok, fmt.Sprintf("Expected field %v not present", fieldID))
			require.Equal(t, expectedValue, value)
		}
		for _, fieldName := range []string{
			base.AuditFieldChannels,
			base.AuditFieldDocIDs,
			base.AuditFieldFeedType,
			base.AuditFieldFilter,
			base.AuditFieldIncludeDocs,
			base.AuditFieldSince,
		} {
			if _, ok := expectedFields[fieldName]; !ok {
				require.NotContains(t, event, fieldName, fmt.Sprintf("Unexpected field %v present", fieldName))
			}
		}
	}
	require.True(t, found, "Did not receive expected changeFeedStart audit event")
}

func createAuditLoggingRestTester(t *testing.T) *RestTester {
	// get tempdir before resetting global loggers, since the logger cleanup needs to happen before deletion
	tempdir := t.TempDir()
	base.ResetGlobalTestLogging(t)
	base.InitializeMemoryLoggers()
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled:     true, // for blip testing
		PersistentConfig: true,
		SyncFn:           `function(doc) {channel(doc.channels);}`,
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
	return rt
}

func TestAuditBlipCRUD(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {

		rt := createAuditLoggingRestTester(t)
		defer rt.Close()

		RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		testCases := []struct {
			name                  string
			setupCode             func(t testing.TB, docID string) DocVersion
			auditableCode         func(t testing.TB, docID string, docVersion DocVersion)
			attachmentCreateCount int
			attachmentReadCount   int
			attachmentUpdateCount int
			attachmentDeleteCount int
			attachmentName        string
		}{
			{
				name:           "add attachment",
				attachmentName: "attachment1",
				auditableCode: func(t testing.TB, docID string, docVersion DocVersion) {
					attData := base64.StdEncoding.EncodeToString([]byte("attach"))

					version, err := btcRunner.PushRev(btc.id, docID, EmptyDocVersion(), []byte(`{"key":"val","_attachments":{"attachment1":{"data":"`+attData+`"}}}`))
					require.NoError(t, err)
					btcRunner.WaitForVersion(btc.id, docID, version)
				},
				attachmentCreateCount: 1,
			},
		}
		for _, testCase := range testCases {
			rt.Run(testCase.name, func(t *testing.T) {
				docID := strings.ReplaceAll(testCase.name, " ", "_")
				var docVersion DocVersion
				if testCase.setupCode != nil {
					docVersion = testCase.setupCode(t, docID)
				}
				output := base.AuditLogContents(t, func(t testing.TB) {
					testCase.auditableCode(t, docID, docVersion)
				})
				postAttachmentVersion, _ := rt.GetDoc(docID)

				requireAttachmentEvents(rt, base.AuditIDAttachmentCreate, output, docID, postAttachmentVersion.RevID, testCase.attachmentName, testCase.attachmentCreateCount)
				requireAttachmentEvents(rt, base.AuditIDAttachmentRead, output, docID, postAttachmentVersion.RevID, testCase.attachmentName, testCase.attachmentReadCount)
				requireAttachmentEvents(rt, base.AuditIDAttachmentUpdate, output, docID, postAttachmentVersion.RevID, testCase.attachmentName, testCase.attachmentUpdateCount)
				requireAttachmentEvents(rt, base.AuditIDAttachmentDelete, output, docID, postAttachmentVersion.RevID, testCase.attachmentName, testCase.attachmentDeleteCount)
			})
		}
	})
}

// TestDatabaseAuditChanges verifies that the expect events are raised when the audit configuration is changed.
// Note: test cases are run sequentially, and depend on ordering, as events are only raised for changes in state
func TestDatabaseAuditChanges(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skip("Audit logging only works in EE")
	}

	db.DisableSequenceWaitOnDbRestart(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	rt := createAuditLoggingRestTester(t)
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	// Create full db config payloads with audit enabled/disabled, for PUT /_config test cases
	auditEnabledFullConfig := rt.NewDbConfig()
	auditEnabledFullConfig.Logging = &DbLoggingConfig{
		Audit: &DbAuditLoggingConfig{
			Enabled: base.BoolPtr(true),
		},
	}
	auditEnabledPutConfigPayload := base.MustJSONMarshal(t, auditEnabledFullConfig)
	auditDisabledPutConfigPayload := base.MustJSONMarshal(t, rt.NewDbConfig())

	type testCase struct {
		name                     string
		method                   string
		path                     string
		requestBody              string
		expectedStatus           int
		expectedEvents           []base.AuditID
		expectedEnabledEventList []any
	}
	testCases := []testCase{
		{
			name:           "disable via _config POST, already disabled",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config",
			requestBody:    `{"logging":{"audit":{"enabled":false}}}`,
			expectedStatus: http.StatusCreated,
			expectedEvents: []base.AuditID{},
		},
		{
			name:           "enable via _config POST",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config",
			requestBody:    `{"logging":{"audit":{"enabled":true}}}`,
			expectedStatus: http.StatusCreated,
			expectedEvents: []base.AuditID{base.AuditIDAuditEnabled},
		},
		{
			name:           "enable via _config POST, already enabled",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config",
			requestBody:    `{"logging":{"audit":{"enabled":true}}}`,
			expectedStatus: http.StatusCreated,
			expectedEvents: []base.AuditID{},
		},
		{
			name:           "disable via _config POST",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config",
			requestBody:    `{"logging":{"audit":{"enabled":false}}}`,
			expectedStatus: http.StatusCreated,
			expectedEvents: []base.AuditID{base.AuditIDAuditDisabled},
		},
		{
			name:           "enable via _config PUT",
			method:         http.MethodPut,
			path:           "/{{.db}}/_config",
			requestBody:    string(auditEnabledPutConfigPayload),
			expectedStatus: http.StatusCreated,
			expectedEvents: []base.AuditID{base.AuditIDAuditEnabled},
		},
		{
			name:           "disable via _config PUT",
			method:         http.MethodPut,
			path:           "/{{.db}}/_config",
			requestBody:    string(auditDisabledPutConfigPayload),
			expectedStatus: http.StatusCreated,
			expectedEvents: []base.AuditID{base.AuditIDAuditDisabled},
		},
		{
			name:           "enable via _config/audit",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config/audit",
			requestBody:    `{"enabled":true}`,
			expectedStatus: http.StatusOK,
			expectedEvents: []base.AuditID{base.AuditIDAuditEnabled},
		},
		{
			name:           "enable via _config/audit, already enabled",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config/audit",
			requestBody:    `{"enabled":true}`,
			expectedStatus: http.StatusOK,
			expectedEvents: []base.AuditID{},
		},
		{
			name:           "disable via _config/audit",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config/audit",
			requestBody:    `{"enabled":false}`,
			expectedStatus: http.StatusOK,
			expectedEvents: []base.AuditID{base.AuditIDAuditDisabled},
		},
		{
			name:           "disable via _config/audit, already disabled",
			method:         http.MethodPost,
			path:           "/{{.db}}/_config/audit",
			requestBody:    `{"enabled":false}`,
			expectedStatus: http.StatusOK,
			expectedEvents: []base.AuditID{},
		},
		{
			name:                     "enable via _config/audit with events PUT",
			method:                   http.MethodPut,
			path:                     "/{{.db}}/_config/audit",
			requestBody:              `{"enabled":true, "events":{"53282":true}}`, // AuditIDPublicUserSessionCreated
			expectedStatus:           http.StatusOK,
			expectedEvents:           []base.AuditID{base.AuditIDAuditEnabled},
			expectedEnabledEventList: []any{float64(base.AuditIDPublicUserSessionCreated)},
		},
		{
			name:           "disable via _config/audit with events PUT",
			method:         http.MethodPut,
			path:           "/{{.db}}/_config/audit",
			requestBody:    `{"enabled":false, "events":{"53282":true}}`, // AuditIDPublicUserSessionCreated
			expectedStatus: http.StatusOK,
			expectedEvents: []base.AuditID{base.AuditIDAuditDisabled},
		},
		{
			name:                     "enable via _config/audit with events POST",
			method:                   http.MethodPost,
			path:                     "/{{.db}}/_config/audit",
			requestBody:              `{"enabled":true, "events":{"53283":true}}`, // AuditIDPublicUserSessionDeleted
			expectedStatus:           http.StatusOK,
			expectedEvents:           []base.AuditID{base.AuditIDAuditEnabled},
			expectedEnabledEventList: []any{float64(base.AuditIDPublicUserSessionCreated), float64(base.AuditIDPublicUserSessionDeleted)},
		},
	}
	for _, testCase := range testCases {
		rt.Run(testCase.name, func(t *testing.T) {
			output := base.AuditLogContents(t, func(t testing.TB) {
				RequireStatus(t, rt.SendAdminRequestWithAuth(testCase.method, testCase.path, testCase.requestBody, base.TestClusterUsername(), base.TestClusterPassword()), testCase.expectedStatus)
			})
			events := jsonLines(t, output)
			for _, expectedEventID := range testCase.expectedEvents {
				found := false
				for _, event := range events {
					eventID := base.AuditID(event[base.AuditFieldID].(float64))
					if eventID == expectedEventID {
						require.Equal(rt.TB(), event[base.AuditFieldDatabase], "db")
						if testCase.expectedEnabledEventList != nil {
							require.Equal(rt.TB(), testCase.expectedEnabledEventList, event[base.AuditFieldEnabledEvents])
						}
						found = true
					}
				}
				require.True(t, found, fmt.Sprintf("Expected event %v not present", expectedEventID))
			}
		})
	}
}
