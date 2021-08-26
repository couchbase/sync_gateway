/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"flag"
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	defer base.SetUpGlobalTestLogging(m)()
	defer base.SetUpGlobalTestProfiling(m)()

	base.SkipPrometheusStatsRegistration = true

	base.GTestBucketPool = base.NewTestBucketPool(db.ViewsAndGSIBucketReadier, db.ViewsAndGSIBucketInit)

	status := m.Run()

	base.GTestBucketPool.Close()

	os.Exit(status)
}

func TestConfigOverwritesLegacyFlags(t *testing.T) {
	osArgs := []string{
		"sync_gateway",
		// Legacy
		"-verbose",
		"-url", "1.2.3.4",
		"-interface", "1.2.3.4",
		// Persistent config
		"-logging.console.log_level", "debug",
		"-bootstrap.server", "localhost",
		"-bootstrap.username", "test",

		"config.json",
	}
	sc, _, _, err := parseFlags(osArgs)
	assert.NoError(t, err)

	require.NotNil(t, sc)
	// Overwrote
	assert.Equal(t, base.LogLevelPtr(base.LevelDebug), sc.Logging.Console.LogLevel)
	assert.Equal(t, "localhost", sc.Bootstrap.Server)
	// Not overwrote
	assert.Equal(t, "1.2.3.4", sc.API.PublicInterface)
	assert.Equal(t, "test", sc.Bootstrap.Username)
}

func TestParseFlags(t *testing.T) {
	osArgsPrefix := []string{"sync_gateway"}
	testCases := []struct {
		name                            string
		osArgs                          []string
		expectError                     bool
		expectedError                   error // Leave blank to not check error text
		expectedDisablePersistentConfig *bool
	}{
		{
			name:                            "Help error returned on -h",
			osArgs:                          []string{"-h"},
			expectError:                     true,
			expectedError:                   flag.ErrHelp,
			expectedDisablePersistentConfig: nil,
		},
		{
			name:                            "Unknown flag",
			osArgs:                          []string{"-unknown-flag"},
			expectError:                     true,
			expectedError:                   nil,
			expectedDisablePersistentConfig: nil,
		},
		{
			name:                            "Disable persistent config",
			osArgs:                          []string{"-disable_persistent_config"},
			expectError:                     false,
			expectedError:                   nil,
			expectedDisablePersistentConfig: base.BoolPtr(true),
		},
		{
			name:                            "Config flag",
			osArgs:                          []string{"-bootstrap.server", "1.2.3.4"},
			expectError:                     false,
			expectedError:                   nil,
			expectedDisablePersistentConfig: base.BoolPtr(false),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			_, _, disablePersistentConfig, err := parseFlags(append(osArgsPrefix, test.osArgs...))
			if test.expectError {
				assert.Error(t, err)
				if test.expectedError != nil {
					assert.EqualError(t, test.expectedError, err.Error())
				}
			}
			assert.Equal(t, test.expectedDisablePersistentConfig, disablePersistentConfig)
		})
	}
}
