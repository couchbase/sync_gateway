package rest

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestReadServerConfig(t *testing.T) {

	tests := []struct {
		name   string
		config string
		err    string
	}{
		{
			name:   "nil",
			config: ``,
			err:    "EOF",
		},
		{
			name:   "valid empty",
			config: `{}`,
		},
		{
			name:   "valid minimal",
			config: `{"logging": {"console": {"enabled": true}}}`,
		},
		{
			name:   "unknown field",
			config: `{"invalid": true}`,
			err:    `json: unknown field "invalid"`,
		},
		{
			name:   "incorrect type",
			config: `{"logging": true}`,
			err:    `json: cannot unmarshal bool into Go struct field ServerConfig.Logging of type base.LoggingConfig`,
		},
		{
			name:   "invalid JSON",
			config: `{true}`,
			err:    `invalid character 't' looking for beginning of object key string`,
		},
		{
			name:   "sync fn backquotes",
			config: "{\"databases\": {\"db\": {\"sync\": `function(doc, oldDoc) {channel(doc.channels)}`}}}",
		},
		{
			name:   "db deprecated shadow",
			config: "{\"databases\": {\"db\": {\"shadow\": {}}}}",
			err:    "Bucket shadowing configuration has been moved to the 'deprecated' section of the config.  Please update your config and retry",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			buf := bytes.NewBufferString(test.config)
			_, err := readServerConfig(SyncGatewayRunModeNormal, buf)
			if test.err == "" {
				assert.NoError(tt, err, "unexpected error for test config")
			} else {
				assert.EqualError(tt, err, test.err, "expecting error for test config")
			}
		})
	}
}

// TestLoadServerConfigExamples will run LoadServerConfig for configs found under the examples directory.
func TestLoadServerConfigExamples(t *testing.T) {
	const exampleLogDirectory = "../examples/"
	const configSuffix = ".json"

	const enterpriseConfigPrefix = "ee_"
	const accelConfigPrefix = "accel_"

	err := filepath.Walk(exampleLogDirectory, func(configPath string, file os.FileInfo, err error) error {
		assert.NoError(t, err)

		runMode := SyncGatewayRunModeNormal

		// Skip directories or files that aren't configs
		if file.IsDir() || !strings.HasSuffix(file.Name(), configSuffix) {
			return nil
		}

		// Skip EE configs in CE
		if !base.IsEnterpriseEdition() && strings.HasPrefix(file.Name(), enterpriseConfigPrefix) {
			return nil
		}

		// Set the accel run mode for accel configs
		if strings.HasPrefix(file.Name(), accelConfigPrefix) {
			runMode = SyncGatewayRunModeAccel
		}

		t.Run(configPath, func(tt *testing.T) {
			_, err := LoadServerConfig(runMode, configPath)
			assert.NoError(tt, err, "unexpected error validating example config")
		})

		return nil
	})
	assert.NoError(t, err)
}
