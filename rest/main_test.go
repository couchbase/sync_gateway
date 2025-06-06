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
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 8192, NumCollectionsPerBucket: 3}
	TestBucketPoolRestWithIndexes(ctx, m, tbpOptions)
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
	sc, _, _, err := parseFlags(base.TestCtx(t), osArgs)
	assert.NoError(t, err)

	require.NotNil(t, sc)
	// Overwrote
	assert.Equal(t, base.Ptr(base.LevelDebug), sc.Logging.Console.LogLevel)
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
			expectedError:                   base.Ptr("help requested"),
			expectedDisablePersistentConfig: nil,
		},
		{
			name:                            "Unknown flag",
			osArgs:                          []string{"-unknown-flag"},
			expectedError:                   base.Ptr("flag provided but not defined: -unknown-flag"),
			expectedDisablePersistentConfig: nil,
		},
		{
			name:                            "Disable persistent config",
			osArgs:                          []string{"-disable_persistent_config"},
			expectedError:                   nil,
			expectedDisablePersistentConfig: base.Ptr(true),
		},
		{
			name:                            "Config flag",
			osArgs:                          []string{"-bootstrap.server", "1.2.3.4"},
			expectedError:                   nil,
			expectedDisablePersistentConfig: base.Ptr(false),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			_, _, disablePersistentConfig, err := parseFlags(base.TestCtx(t), append(osArgsPrefix, test.osArgs...))
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

func TestDefaultStartupConfig(t *testing.T) {
	ctx := base.TestCtx(t)
	sc, _, _, err := parseFlags(ctx, []string{"sync_gateway"})
	require.NoError(t, err)
	require.Nil(t, sc.HeapProfileCollectionThreshold)
}

func TestSanitizeDbConfigs(t *testing.T) {
	serverAddressErrorString := "automatic upgrade to persistent config requires each database config to have a server address specified that are all matching in the 2.x config"
	testCases := []struct {
		name          string
		input         DbConfigMap
		expectedError string
	}{
		{
			name:          "Nil server",
			input:         DbConfigMap{"1": &DbConfig{}},
			expectedError: serverAddressErrorString,
		},
		{
			name:          "Empty server",
			input:         DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("")}}},
			expectedError: serverAddressErrorString,
		},
		{
			name: "Filled in server, and nil server",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("1.2.3.4")}},
				"2": &DbConfig{}},
			expectedError: serverAddressErrorString,
		},
		{
			name: "Filled in server, and empty server",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("")}},
				"2": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("1.2.3.4")}}},
			expectedError: serverAddressErrorString,
		},
		{
			name: "Filled in matching servers",
			input: DbConfigMap{"1": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("1.2.3.4")}},
				"2": &DbConfig{BucketConfig: BucketConfig{Server: base.Ptr("1.2.3.4")}}},
		},
		{
			name: "Multiple buckets with same db",
			input: DbConfigMap{
				"db": &DbConfig{
					BucketConfig: BucketConfig{Server: base.Ptr("1.2.3.4"), Bucket: base.Ptr("bucket")},
				},
				"db2": &DbConfig{
					BucketConfig: BucketConfig{Server: base.Ptr("1.2.3.4"), Bucket: base.Ptr("bucket")},
				},
			},
			// Cannot specify bucket names exactly due to un-deterministic iteration over map
			expectedError: "automatic upgrade to persistent config failed. Only one database can target any given bucket. bucket used by",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			dbConfigMap, err := sanitizeDbConfigs(test.input)

			if test.expectedError == "" {
				assert.NoError(t, err)

				for cfgIdx, dbConfig := range dbConfigMap {
					assert.Nil(t, dbConfig.Server)
					assert.Nil(t, test.input[cfgIdx].Server)
				}

				return
			}

			assert.Nil(t, dbConfigMap)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedError)
			return

		})
	}
}
