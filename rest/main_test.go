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
		expectedError                   *string // Text to check error contains
		expectedDisablePersistentConfig *bool
	}{
		{
			name:                            "Help error returned on -h",
			osArgs:                          []string{"-h"},
			expectedError:                   base.StringPtr("help requested"),
			expectedDisablePersistentConfig: nil,
		},
		{
			name:                            "Unknown flag",
			osArgs:                          []string{"-unknown-flag"},
			expectedError:                   base.StringPtr("flag provided but not defined: -unknown-flag"),
			expectedDisablePersistentConfig: nil,
		},
		{
			name:                            "Disable persistent config",
			osArgs:                          []string{"-disable_persistent_config"},
			expectedError:                   nil,
			expectedDisablePersistentConfig: base.BoolPtr(true),
		},
		{
			name:                            "Config flag",
			osArgs:                          []string{"-bootstrap.server", "1.2.3.4"},
			expectedError:                   nil,
			expectedDisablePersistentConfig: base.BoolPtr(false),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			_, _, disablePersistentConfig, err := parseFlags(append(osArgsPrefix, test.osArgs...))
			if test.expectedError != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), *test.expectedError)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.expectedDisablePersistentConfig, disablePersistentConfig)
		})
	}
}

func TestSanitizeDbConfigs(t *testing.T) {
	expectedError := "automatic upgrade to persistent config requires each database config to have a server address specified that are all matching in the 2.x config"
	testCases := []struct {
		name  string
		input DbConfigMap
		error bool
	}{
		{
			name:  "Nil server",
			input: DbConfigMap{"1": &DbConfig{}},
			error: true,
		},
		{
			name:  "Empty server",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("")}}},
			error: true,
		},
		{
			name: "Filled in server, and nil server",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("1.2.3.4")}},
				"2": &DbConfig{}},
			error: true,
		},
		{
			name: "Filled in server, and empty server",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("")}},
				"2": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("1.2.3.4")}}},
			error: true,
		},
		{
			name: "Filled in matching servers",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("1.2.3.4")}},
				"2": &DbConfig{BucketConfig: BucketConfig{Server: base.StringPtr("1.2.3.4")}}},
			error: false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			dbConfigMap, err := sanitizeDbConfigs(test.input)
			if test.error {
				assert.Nil(t, dbConfigMap)
				require.Error(t, err)
				assert.EqualError(t, err, expectedError)
				return
			}
			assert.NoError(t, err)
		})
	}
}
