package rest

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutomaticConfigUpgrade(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	tb := base.GetTestBucket(t)
	defer tb.Close()

	rawConfig := `
	{
		"databases": {
			"db": {
			  "server": "%s",
			  "username": "%s",
			  "password": "%s",
			  "bucket": "%s"
			}
		}
	}`

	config := fmt.Sprintf(rawConfig, base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), tb.GetName())

	tmpDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)

	configPath := filepath.Join(tmpDir, "config.json")
	err = ioutil.WriteFile(configPath, []byte(config), os.FileMode(0644))
	require.NoError(t, err)

	startupConfig, err := automaticConfigUpgrade(configPath)
	assert.NoError(t, err)

	assert.Equal(t, base.UnitTestUrl(), startupConfig.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), startupConfig.Bootstrap.Username)
	assert.Equal(t, base.TestClusterPassword(), startupConfig.Bootstrap.Password)

	cbs, err := EstablishCouchbaseClusterConnection(startupConfig)
	require.NoError(t, err)

	var dbConfig DbConfig
	_, err = cbs.GetConfig(tb.GetName(), "default", &dbConfig)
	assert.NoError(t, err)

	err = cbs.Close()
	assert.NoError(t, err)

	assert.Equal(t, "db", dbConfig.Name)
	assert.Equal(t, tb.GetName(), *dbConfig.Bucket)
	assert.Nil(t, dbConfig.Server)
	assert.Equal(t, base.TestClusterUsername(), dbConfig.Username)
	assert.Equal(t, base.TestClusterPassword(), dbConfig.Password)
}

func TestAutomaticConfigUpgradeError(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	testCases := []struct {
		Name   string
		Config string
	}{
		{
			"Multiple DBs different servers",
			`
				{
					"databases": {
						"db": {
						  "server": "%s",
						  "username": "%s",
						  "password": "%s",
						  "bucket": "%s"
						},
						"db2": {
						  "server": "rand",
						  "username": "",
						  "password": "",
						  "bucket": ""
						}
					}
				}`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			tb := base.GetTestBucket(t)
			defer tb.Close()

			config := fmt.Sprintf(testCase.Config, base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), tb.GetName())

			tmpDir, err := ioutil.TempDir("", strings.ReplaceAll(t.Name(), "/", ""))
			require.NoError(t, err)

			configPath := filepath.Join(tmpDir, "config.json")
			err = ioutil.WriteFile(configPath, []byte(config), os.FileMode(0644))
			require.NoError(t, err)

			_, err = automaticConfigUpgrade(configPath)
			assert.Error(t, err)
		})
	}
}
