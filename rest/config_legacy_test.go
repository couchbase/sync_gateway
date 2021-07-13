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
		input    LegacyConfig
		expected StartupConfig
	}{
		{
			name:     "No overrides",
			base:     DefaultStartupConfig(""),
			input:    LegacyConfig{},
			expected: DefaultStartupConfig(""),
		},
		{
			name:     "Override *duration for StatsLogFrequency",
			base:     StartupConfig{Unsupported: UnsupportedConfig{StatsLogFrequency: base.NewConfigDuration(time.Minute)}},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{Unsupported: &UnsupportedServerConfigLegacy{StatsLogFrequencySecs: base.UintPtr(10)}}},
			expected: StartupConfig{Unsupported: UnsupportedConfig{StatsLogFrequency: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override duration zero ServerReadTimeout",
			base:     StartupConfig{API: APIConfig{ServerReadTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{ServerReadTimeout: base.IntPtr(0)}},
			expected: StartupConfig{API: APIConfig{ServerReadTimeout: base.NewConfigDuration(0)}},
		},
		{
			name:     "Override duration non-zero ServerWriteTimeout",
			base:     StartupConfig{API: APIConfig{ServerWriteTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{ServerWriteTimeout: base.IntPtr(30)}},
			expected: StartupConfig{API: APIConfig{ServerWriteTimeout: base.NewConfigDuration(time.Second * 30)}},
		},
		{
			name:     "Override duration nil ReadHeaderTimeout",
			base:     StartupConfig{API: APIConfig{ReadHeaderTimeout: base.NewConfigDuration(time.Second * 10)}},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{ReadHeaderTimeout: nil}},
			expected: StartupConfig{API: APIConfig{ReadHeaderTimeout: base.NewConfigDuration(time.Second * 10)}},
		},
		{
			name:     "Override bool Pretty",
			base:     StartupConfig{API: APIConfig{Pretty: true}},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{Pretty: false}},
			expected: StartupConfig{API: APIConfig{Pretty: true}},
		},
		{
			name:     "Override *bool(false) CompressResponses",
			base:     StartupConfig{API: APIConfig{CompressResponses: base.BoolPtr(true)}},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{CompressResponses: base.BoolPtr(false)}},
			expected: StartupConfig{API: APIConfig{CompressResponses: base.BoolPtr(false)}},
		},
		{
			name:     "Override nil *bool HTTP2Enable",
			base:     StartupConfig{},
			input:    LegacyConfig{LegacyServerConfig: LegacyServerConfig{Unsupported: &UnsupportedServerConfigLegacy{Http2Config: &HTTP2Config{Enabled: base.BoolPtr(false)}}}},
			expected: StartupConfig{Unsupported: UnsupportedConfig{HTTP2: &HTTP2Config{Enabled: base.BoolPtr(false)}}},
		},
		{
			name:     "Absent property AdminInterfaceAuthentication",
			base:     StartupConfig{API: APIConfig{AdminInterfaceAuthentication: base.BoolPtr(true)}},
			input:    LegacyConfig{},
			expected: StartupConfig{API: APIConfig{AdminInterfaceAuthentication: base.BoolPtr(true)}},
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
