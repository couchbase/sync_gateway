package rest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
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
		"server_tls_skip_verify": `+strconv.FormatBool(base.TestTLSSkipVerify())+`,
		"interface": ":4444",
		"adminInterface": ":4445",
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

	startupConfig, _, err := automaticConfigUpgrade(configPath)
	require.NoError(t, err)

	assert.Equal(t, "", startupConfig.Bootstrap.ConfigGroupID)
	assert.Equal(t, base.UnitTestUrl(), startupConfig.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), startupConfig.Bootstrap.Username)
	assert.Equal(t, base.TestClusterPassword(), startupConfig.Bootstrap.Password)
	assert.Equal(t, ":4444", startupConfig.API.PublicInterface)
	assert.Equal(t, ":4445", startupConfig.API.AdminInterface)

	writtenNewFile, err := ioutil.ReadFile(configPath)
	require.NoError(t, err)

	var writtenFileStartupConfig StartupConfig
	err = json.Unmarshal(writtenNewFile, &writtenFileStartupConfig)
	require.NoError(t, err)

	assert.Equal(t, "", startupConfig.Bootstrap.ConfigGroupID)
	assert.Equal(t, base.UnitTestUrl(), writtenFileStartupConfig.Bootstrap.Server)
	assert.Equal(t, base.TestClusterUsername(), writtenFileStartupConfig.Bootstrap.Username)
	assert.Equal(t, base.TestClusterPassword(), writtenFileStartupConfig.Bootstrap.Password)
	assert.Equal(t, ":4444", writtenFileStartupConfig.API.PublicInterface)
	assert.Equal(t, ":4445", writtenFileStartupConfig.API.AdminInterface)

	backupFileName := ""

	err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if strings.Contains(filepath.Base(path), "backup") {
			backupFileName = path
		}
		return nil
	})
	require.NoError(t, err)

	writtenBackupFile, err := ioutil.ReadFile(backupFileName)
	require.NoError(t, err)

	assert.Equal(t, config, string(writtenBackupFile))

	cbs, err := createCouchbaseClusterFromStartupConfig(startupConfig)
	require.NoError(t, err)

	var dbConfig DbConfig
	_, err = cbs.GetConfig(tb.GetName(), persistentConfigDefaultGroupID, &dbConfig)
	require.NoError(t, err)

	assert.Equal(t, "db", dbConfig.Name)
	assert.Equal(t, tb.GetName(), *dbConfig.Bucket)
	assert.Nil(t, dbConfig.Server)
	assert.Equal(t, "", dbConfig.Username)
	assert.Equal(t, "", dbConfig.Password)
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
					"server_tls_skip_verify": `+strconv.FormatBool(base.TestTLSSkipVerify())+`,
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

			_, _, err = automaticConfigUpgrade(configPath)
			assert.Error(t, err)
		})
	}
}

func TestAutomaticConfigUpgradeExistingConfigAndNewGroup(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("CBS required")
	}

	configRaw := `
	{
		"server_tls_skip_verify": `+strconv.FormatBool(base.TestTLSSkipVerify())+`,
		"databases": {
			"db": {
				"server": "%s",
				"username": "%s",
				"password": "%s",
				"bucket": "%s"
			}
		}
	}`

	tb := base.GetTestBucket(t)
	defer tb.Close()

	tmpDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)

	config := fmt.Sprintf(configRaw, base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), tb.GetName())
	configPath := filepath.Join(tmpDir, "config.json")
	err = ioutil.WriteFile(configPath, []byte(config), os.FileMode(0644))
	require.NoError(t, err)

	// Run migration once
	_, _, err = automaticConfigUpgrade(configPath)
	require.NoError(t, err)

	updatedConfigRaw := `
	{
		"server_tls_skip_verify": `+strconv.FormatBool(base.TestTLSSkipVerify())+`,
		"databases": {
			"db": {
				"revs_limit": 20000,
				"server": "%s",
				"username": "%s",
				"password": "%s",
				"bucket": "%s"
			}
		}
	}`

	updatedConfig := fmt.Sprintf(updatedConfigRaw, base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), tb.GetName())
	updatedConfigPath := filepath.Join(tmpDir, "config-updated.json")
	err = ioutil.WriteFile(updatedConfigPath, []byte(updatedConfig), os.FileMode(0644))
	require.NoError(t, err)

	// Run migration again to ensure no error and validate it doesn't actually update db
	startupConfig, _, err := automaticConfigUpgrade(updatedConfigPath)
	require.NoError(t, err)

	cbs, err := createCouchbaseClusterFromStartupConfig(startupConfig)
	require.NoError(t, err)

	var dbConfig DbConfig
	originalDefaultDbConfigCAS, err := cbs.GetConfig(tb.GetName(), persistentConfigDefaultGroupID, &dbConfig)
	assert.NoError(t, err)

	// Ensure that revs limit hasn't actually been set
	assert.Nil(t, dbConfig.RevsLimit)

	// Now attempt an upgrade for a non-default group ID, and ensure it's written correctly, and separately from the default group.
	const configUpgradeGroupID = "import"

	importConfigRaw := `
	{
		"config_upgrade_group_id": "%s",
		"databases": {
			"db": {
				"enable_shared_bucket_access": true,
				"import_docs": true,
				"server": "%s",
				"username": "%s",
				"password": "%s",
				"bucket": "%s"
			}
		}
	}`

	importConfig := fmt.Sprintf(importConfigRaw, configUpgradeGroupID, base.UnitTestUrl(), base.TestClusterUsername(), base.TestClusterPassword(), tb.GetName())
	importConfigPath := filepath.Join(tmpDir, "config-import.json")
	err = ioutil.WriteFile(importConfigPath, []byte(importConfig), os.FileMode(0644))
	require.NoError(t, err)

	startupConfig, _, err = automaticConfigUpgrade(importConfigPath)
	// only supported in EE
	if base.IsEnterpriseEdition() {
		require.NoError(t, err)

		// Ensure that startupConfig group ID has been set
		assert.Equal(t, configUpgradeGroupID, startupConfig.Bootstrap.ConfigGroupID)

		// Ensure dbConfig is saved as the specified config group ID
		var dbConfig DbConfig
		_, err = cbs.GetConfig(tb.GetName(), configUpgradeGroupID, &dbConfig)
		assert.NoError(t, err)

		// Ensure default has not changed
		dbConfig = DbConfig{}
		defaultDbConfigCAS, err := cbs.GetConfig(tb.GetName(), persistentConfigDefaultGroupID, &dbConfig)
		assert.NoError(t, err)
		assert.Equal(t, originalDefaultDbConfigCAS, defaultDbConfigCAS)
	} else {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only supported in enterprise edition")
		assert.Nil(t, startupConfig)
	}
}
