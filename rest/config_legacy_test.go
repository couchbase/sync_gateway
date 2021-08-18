package rest

import (
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lc := &test.input

			migratedStartupConfig, _, err := lc.ToStartupConfig()

			// lc.MaxHeartbeat is (uint)0 if not set causing Replicator.MaxHeartbeat to always be 0 when migrating
			if lc.MaxHeartbeat == 0 {
				test.expected.Replicator.MaxHeartbeat = base.NewConfigDuration(0)
			}

			config := test.base
			err = config.Merge(migratedStartupConfig)
			require.NoError(t, err)

			assert.Equal(t, test.expected, config)
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
