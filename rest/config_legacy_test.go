// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyConfigToStartupConfig(t *testing.T) {
	tests := []struct {
		name     string
		base     StartupConfig
		input    LegacyServerConfig
		expected StartupConfig
	}{
		{
			name:     "No overrides",
			base:     DefaultStartupConfig(""),
			input:    LegacyServerConfig{},
			expected: DefaultStartupConfig(""),
		},
		{
			name:     "Override *duration for StatsLogFrequency",
			base:     StartupConfig{Unsupported: UnsupportedConfig{StatsLogFrequency: base.NewConfigDuration(time.Minute)}},
			input:    LegacyServerConfig{Unsupported: &UnsupportedServerConfigLegacy{StatsLogFrequencySecs: base.Ptr(uint(10))}},
			expected: StartupConfig{Unsupported: UnsupportedConfig{StatsLogFrequency: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override duration zero ServerReadTimeout",
			base:     StartupConfig{API: APIConfig{ServerReadTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyServerConfig{ServerReadTimeout: base.Ptr(0)},
			expected: StartupConfig{API: APIConfig{ServerReadTimeout: base.NewConfigDuration(0)}},
		},
		{
			name:     "Override duration non-zero ServerWriteTimeout",
			base:     StartupConfig{API: APIConfig{ServerWriteTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyServerConfig{ServerWriteTimeout: base.Ptr(30)},
			expected: StartupConfig{API: APIConfig{ServerWriteTimeout: base.NewConfigDuration(time.Second * 30)}},
		},
		{
			name:     "Override duration nil ReadHeaderTimeout",
			base:     StartupConfig{API: APIConfig{ReadHeaderTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyServerConfig{ReadHeaderTimeout: nil},
			expected: StartupConfig{API: APIConfig{ReadHeaderTimeout: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override bool Pretty",
			base:     StartupConfig{API: APIConfig{Pretty: base.Ptr(true)}},
			input:    LegacyServerConfig{Pretty: false},
			expected: StartupConfig{API: APIConfig{Pretty: base.Ptr(true)}},
		},
		{
			name:     "Override *bool(false) CompressResponses",
			base:     StartupConfig{API: APIConfig{CompressResponses: base.Ptr(true)}},
			input:    LegacyServerConfig{CompressResponses: base.Ptr(false)},
			expected: StartupConfig{API: APIConfig{CompressResponses: base.Ptr(false)}},
		},
		{
			name:     "Override nil *bool HTTP2Enable",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Unsupported: &UnsupportedServerConfigLegacy{Http2Config: &HTTP2Config{Enabled: base.Ptr(false)}}},
			expected: StartupConfig{Unsupported: UnsupportedConfig{HTTP2: &HTTP2Config{Enabled: base.Ptr(false)}}},
		},
		{
			name:     "Absent property AdminInterfaceAuthentication",
			base:     StartupConfig{API: APIConfig{AdminInterfaceAuthentication: base.Ptr(true)}},
			input:    LegacyServerConfig{},
			expected: StartupConfig{API: APIConfig{AdminInterfaceAuthentication: base.Ptr(true)}},
		},
		{
			name:     "http:// to couchbase://",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("http://http.couchbase.com:8091,host2:8091,host1,[2001:db8::8811],[2001:db8::8822]:8091")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://http.couchbase.com,host2,host1,[2001:db8::8811],[2001:db8::8822]"}},
		},
		{
			name:     "Username and password in server URL",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("http://foo:bar@[2001:db8::8811]:8091,host2")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://[2001:db8::8811],host2", Username: "foo", Password: "bar"}},
		},
		{
			name:     "Keep couchbase:// port with args",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("couchbase://host1:123,host2:9911?test=true")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://host1:123,host2:9911?test=true"}},
		},
		{
			name:     "Prioritise username/password fields over credentials in host",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("couchbase://foo:bar@host1:123"), Username: "usr", Password: "pass"}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://host1:123", Username: "usr", Password: "pass"}},
		},
		{
			name:     "http:// to couchbase:// with args",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("http://host1,host2:8091?p1=v1&p2=v2&p3=v3")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://host1,host2?p1=v1&p2=v2&p3=v3"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := &test.input

			migratedStartupConfig, _, err := lc.ToStartupConfig(base.TestCtx(t))
			require.NoError(t, err)

			config := test.base
			err = config.Merge(migratedStartupConfig)
			require.NoError(t, err)

			assert.Equal(t, test.expected, config)
		})
	}
}

func TestLegacyServerAddressUpgrade(t *testing.T) {
	testCases := []struct {
		name             string
		server           string
		expectError      bool
		expectedServer   string
		expectedUsername string
		expectedPassword string
	}{
		{
			name:             "Keep couchbase ports",
			server:           "couchbase://localhost:8091,localhosttwo:1234?network=true",
			expectError:      false,
			expectedServer:   "couchbase://localhost:8091,localhosttwo:1234?network=true",
			expectedUsername: "",
			expectedPassword: "",
		},
		{
			name:           "Convert, do not keep trailing comma",
			server:         "http://localhost:8091,127.0.0.2:8091,",
			expectError:    false,
			expectedServer: "couchbase://localhost,127.0.0.2",
		},
		{
			name:           "Do not keep trailing comma, couchbases://",
			server:         "couchbases://localhost,127.0.0.2,",
			expectError:    false,
			expectedServer: "couchbases://localhost,127.0.0.2",
		},
		{
			name:             "Convert, strip ports, parse username and password, keep query params",
			server:           "http://foo:bar@localhost,127.0.0.2:8091?network=true",
			expectError:      false,
			expectedServer:   "couchbase://localhost,127.0.0.2?network=true",
			expectedUsername: "foo",
			expectedPassword: "bar",
		},
		{
			name:             "Couchbase:// with username and password",
			server:           "http://foo:bar@localhost:8091",
			expectError:      false,
			expectedServer:   "couchbase://localhost",
			expectedUsername: "foo",
			expectedPassword: "bar",
		},
		{
			name:             "http:// with username but no password (invalid for CBS)",
			server:           "http://foo@localhost",
			expectError:      false,
			expectedServer:   "couchbase://localhost",
			expectedUsername: "",
			expectedPassword: "",
		},
		{
			name:             "Couchbase:// with password but no username (invalid for CBS)",
			server:           "http://:foo@localhost:8091",
			expectError:      false,
			expectedServer:   "couchbase://localhost",
			expectedUsername: "",
			expectedPassword: "",
		},
		{
			name:             "Multi params with & separators, alphabetical order",
			server:           "http://foo:bar@localhost,127.0.0.2?c=3&a=1&b=2",
			expectError:      false,
			expectedServer:   "couchbase://localhost,127.0.0.2?a=1&b=2&c=3",
			expectedUsername: "foo",
			expectedPassword: "bar",
		},
		{
			name:        "Error due to unknown port",
			server:      "http://foo:bar@localhost:8091,127.0.0.2:1234?network=true",
			expectError: true,
		},
		{
			name:        "Gocbstr bad scheme error",
			server:      "ftp://test:123",
			expectError: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			s, u, p, err := legacyServerAddressUpgrade(test.server)
			assert.Equal(t, test.expectedServer, s)
			assert.Equal(t, test.expectedUsername, u)
			assert.Equal(t, test.expectedPassword, p)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// CBG-1415
func TestLegacyConfigXattrsDefault(t *testing.T) {
	tests := []struct {
		name           string
		xattrs         *bool
		expectedXattrs bool
	}{
		{
			name:           "Nil Xattrs",
			xattrs:         nil,
			expectedXattrs: false,
		},
		{
			name:           "False Xattrs",
			xattrs:         base.Ptr(false),
			expectedXattrs: false,
		},
		{
			name:           "True Xattrs",
			xattrs:         base.Ptr(true),
			expectedXattrs: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{EnableXattrs: test.xattrs}}}
			_, dbs, err := lc.ToStartupConfig(base.TestCtx(t))
			require.NoError(t, err)

			db, ok := dbs["db"]
			require.True(t, ok)

			dbc := db.ToDatabaseConfig()
			require.NotNil(t, dbc.EnableXattrs)
			assert.Equal(t, test.expectedXattrs, *dbc.EnableXattrs)
		})
	}
}

func TestSGReplicateValidation(t *testing.T) {
	errText := "cannot use SG replicate as it has been removed. Please use Inter-Sync Gateway Replication instead"
	configReader := strings.NewReader(`{
			"replications": [
				{
					"source": "db",
					"target": "db-copy"
				}
			]
		}`)

	_, err := readLegacyServerConfig(base.TestCtx(t), configReader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), errText)
}

// CBG-1754 - Guest user not enabled during legacy config migration
func TestLegacyGuestUserMigration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	ctx := base.TestCtx(t)
	expected := auth.PrincipalConfig{
		ExplicitChannels: base.SetFromArray([]string{"*"}),
		Disabled:         base.Ptr(false),
	}

	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	config := fmt.Sprintf(`{
	"server_tls_skip_verify": %t,
	"interface": ":4444",
	"adminInterface": ":4445",
	"databases": {
		"db": {
			"server": "%s",
			"username": "%s",
			"password": "%s",
			"bucket": "%s",
			"users": {
				"GUEST": {
					"disabled": false,
					"admin_channels": ["*"]
				}
			}
		}
	}
}`,
		base.TestTLSSkipVerify(),
		base.UnitTestUrl(),
		base.TestClusterUsername(),
		base.TestClusterPassword(),
		tb.GetName(),
	)

	tmpDir := t.TempDir()

	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(config), os.FileMode(0644))
	require.NoError(t, err)

	sc, _, _, _, err := automaticConfigUpgrade(ctx, configPath)
	require.NoError(t, err)

	cluster, err := CreateBootstrapConnectionFromStartupConfig(ctx, sc, base.PerUseClusterConnections)
	require.NoError(t, err)

	bootstrap := bootstrapContext{
		Connection: cluster,
	}
	var dbConfig DatabaseConfig
	_, err = bootstrap.GetConfig(base.TestCtx(t), tb.GetName(), PersistentConfigDefaultGroupID, "db", &dbConfig)
	require.NoError(t, err)

	assert.Equal(t, &expected, dbConfig.Guest)
}

// CBG-1751: Install legacy config principals prior to upgrade
func TestLegacyConfigPrinciplesMigration(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}
	rt := NewRestTester(t, nil)
	defer rt.Close()
	bucket := rt.Bucket()
	rt.GetDatabase().AllowEmptyPassword = true // So users don't have to have password set
	ctx := rt.Context()

	// Expected principle names that should exist on bucket after migration
	expectedUsers := []string{
		"NewUserStatic",      // Only in config
		"ExistingUserStatic", // Defined in bucket and config
		"ExistingUser",       // Already in bucket, not config
	}
	expectedRoles := []string{
		"NewRoleStatic",      // Only in config
		"ExistingRoleStatic", // Defined in bucket and config
		"ExistingRole",       // Already in bucket, not config
	}

	// Add principles already on bucket before migration
	existingUsers := map[string]*auth.PrincipalConfig{
		"ExistingUserStatic": {
			Name:             base.Ptr("ExistingUserStatic"),
			ExplicitChannels: base.SetOf("*"),
		},
		"ExistingUser": {
			Name:             base.Ptr("ExistingUser"),
			ExplicitChannels: base.SetOf("*"),
		},
	}
	err := rt.GetDatabase().InstallPrincipals(ctx, existingUsers, "user")
	require.NoError(t, err)

	existingRoles := map[string]*auth.PrincipalConfig{
		"ExistingRoleStatic": {
			Name:             base.Ptr("ExistingRoleStatic"),
			ExplicitChannels: base.SetOf("*"),
		},
		"ExistingRole": {
			Name:             base.Ptr("ExistingRole"),
			ExplicitChannels: base.SetOf("*"),
		},
	}
	err = rt.GetDatabase().InstallPrincipals(ctx, existingRoles, "role")
	require.NoError(t, err)

	// Config to migrate to persistent config on bucket
	config := `
	{
		"server_tls_skip_verify": %t,
		"databases": {
			"db": {
				"server": "%s",
				"username": "%s",
				"password": "%s",
				"bucket": "%s",
				"num_index_replicas": 0,
				"users": {
					"NewUserStatic": {
						"admin_channels": ["*"]
					},
					"ExistingUserStatic": {
						"admin_channels": ["*"]
					}
				},
				"roles": {
					"NewRoleStatic": {
						"admin_channels": ["*"]
					},
					"ExistingRoleStatic": {
						"admin_channels": ["*"]
					}
				}
			}
		}
	}`
	config = fmt.Sprintf(config, base.TestTLSSkipVerify(), base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), rt.Bucket().GetName())

	tmpDir := t.TempDir()

	configPath := filepath.Join(tmpDir, "config.json")
	err = os.WriteFile(configPath, []byte(config), os.FileMode(0644))
	require.NoError(t, err)

	// Copy behaviour of serverMainPersistentConfig - upgrade config, pass legacy users and roles in to addLegacyPrinciples (after server context is created)
	_, _, users, roles, err := automaticConfigUpgrade(ctx, configPath)
	require.NoError(t, err)
	rt.ServerContext().addLegacyPrincipals(ctx, users, roles)

	// Check that principles all exist on bucket
	authenticator := auth.NewAuthenticator(bucket.DefaultDataStore(), nil, rt.GetDatabase().AuthenticatorOptions(ctx))
	for _, name := range expectedUsers {
		user, err := authenticator.GetUser(name)
		assert.NoError(t, err)
		assert.NotNil(t, user)
	}
	for _, name := range expectedRoles {
		role, err := authenticator.GetRole(name)
		assert.NoError(t, err)
		assert.NotNil(t, role)
	}
}

// CBG-1929: Test fromConfig=true validation in ToStartupConfig()
func TestLegacyReplicationConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		input       map[string]*db.ReplicationConfig
		expectError bool
	}{
		{
			name:        "Valid config",
			input:       map[string]*db.ReplicationConfig{"repl": {ID: "repl"}},
			expectError: false,
		},
		{
			name:        "Replication key and ID mismatch", // Error from ToStartupConfig, not ValidateReplication
			input:       map[string]*db.ReplicationConfig{"repl": {ID: "repl_id"}},
			expectError: true,
		},
		{
			name:        "Setting adhoc when using config", // Only errors when set from legacy config (fromConfig=true)
			input:       map[string]*db.ReplicationConfig{"repl": {ID: "repl", Adhoc: true}},
			expectError: true,
		},
		{
			name:        "Setting username and remote username", // Error on API and legacy config
			input:       map[string]*db.ReplicationConfig{"repl": {ID: "repl", RemoteUsername: "username", Username: "username"}},
			expectError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set required replication fields to avoid errors
			test.input["repl"].Remote = "localhost"
			test.input["repl"].Direction = "pull"

			lc := LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{Replications: test.input}}}

			_, _, err := lc.ToStartupConfig(base.TestCtx(t))
			fmt.Println(err)
			if test.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
