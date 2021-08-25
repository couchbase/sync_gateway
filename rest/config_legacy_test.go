package rest

import (
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
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
			input:    LegacyServerConfig{Unsupported: &UnsupportedServerConfigLegacy{StatsLogFrequencySecs: base.UintPtr(10)}},
			expected: StartupConfig{Unsupported: UnsupportedConfig{StatsLogFrequency: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override duration zero ServerReadTimeout",
			base:     StartupConfig{API: APIConfig{ServerReadTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyServerConfig{ServerReadTimeout: base.IntPtr(0)},
			expected: StartupConfig{API: APIConfig{ServerReadTimeout: base.NewConfigDuration(0)}},
		},
		{
			name:     "Override duration non-zero ServerWriteTimeout",
			base:     StartupConfig{API: APIConfig{ServerWriteTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyServerConfig{ServerWriteTimeout: base.IntPtr(30)},
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
			base:     StartupConfig{API: APIConfig{Pretty: base.BoolPtr(true)}},
			input:    LegacyServerConfig{Pretty: false},
			expected: StartupConfig{API: APIConfig{Pretty: base.BoolPtr(true)}},
		},
		{
			name:     "Override *bool(false) CompressResponses",
			base:     StartupConfig{API: APIConfig{CompressResponses: base.BoolPtr(true)}},
			input:    LegacyServerConfig{CompressResponses: base.BoolPtr(false)},
			expected: StartupConfig{API: APIConfig{CompressResponses: base.BoolPtr(false)}},
		},
		{
			name:     "Override nil *bool HTTP2Enable",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Unsupported: &UnsupportedServerConfigLegacy{Http2Config: &HTTP2Config{Enabled: base.BoolPtr(false)}}},
			expected: StartupConfig{Unsupported: UnsupportedConfig{HTTP2: &HTTP2Config{Enabled: base.BoolPtr(false)}}},
		},
		{
			name:     "Absent property AdminInterfaceAuthentication",
			base:     StartupConfig{API: APIConfig{AdminInterfaceAuthentication: base.BoolPtr(true)}},
			input:    LegacyServerConfig{},
			expected: StartupConfig{API: APIConfig{AdminInterfaceAuthentication: base.BoolPtr(true)}},
		},
		{
			name:     "http:// to couchbase://",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("http://http.couchbase.com:8091,host2:8091,host1,[2001:db8::8811],[2001:db8::8822]:8091")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://http.couchbase.com,host2,host1,[2001:db8::8811],[2001:db8::8822]"}},
		},
		{
			name:     "Username and password in server URL",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("http://foo:bar@[2001:db8::8811]:8091,host2")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://[2001:db8::8811],host2", Username: "foo", Password: "bar"}},
		},
		{
			name:     "Keep couchbase:// port with args",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("couchbase://host1:123,host2:9911?test=true")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://host1:123,host2:9911?test=true"}},
		},
		{
			name:     "Prioritise username/password fields over credentials in host",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("couchbase://foo:bar@host1:123"), Username: "usr", Password: "pass"}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://host1:123", Username: "usr", Password: "pass"}},
		},
		{
			name:     "http:// to couchbase:// with args",
			base:     StartupConfig{},
			input:    LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("http://host1,host2:8091?p1=v1&p2=v2;p3=v3")}}}},
			expected: StartupConfig{Bootstrap: BootstrapConfig{Server: "couchbase://host1,host2?p1=v1&p2=v2&p3=v3"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := &test.input

			migratedStartupConfig, _, err := lc.ToStartupConfig()

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
			name:             "Multi params with & and ; separators, alphabetical order",
			server:           "http://foo:bar@localhost,127.0.0.2?c=3;a=1&b=2",
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
			xattrs:         base.BoolPtr(false),
			expectedXattrs: false,
		},
		{
			name:           "True Xattrs",
			xattrs:         base.BoolPtr(true),
			expectedXattrs: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := LegacyServerConfig{Databases: DbConfigMap{"db": &DbConfig{EnableXattrs: test.xattrs}}}
			_, dbs, err := lc.ToStartupConfig()
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

	config, err := readServerConfig(configReader)
	assert.Nil(t, config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), errText)
}
