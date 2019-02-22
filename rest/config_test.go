package rest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

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
