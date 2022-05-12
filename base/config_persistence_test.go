package base

import (
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigPersistence(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	sgCollection, ok := bucket.Bucket.(*Collection)
	require.True(t, ok)

	c := sgCollection.Collection

	testCases := []struct {
		name                  string
		configPersistenceImpl ConfigPersistence
	}{
		{
			name:                  "document body persistence",
			configPersistenceImpl: &DocumentBootstrapPersistence{},
		},
		{
			name:                  "xattr persistence",
			configPersistenceImpl: &XattrBootstrapPersistence{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cp := testCase.configPersistenceImpl
			configBody := make(map[string]interface{})
			configBody["sampleConfig"] = "value"
			configKey := "testConfigKey"
			rawConfigBody, marshalErr := JSONMarshal(configBody)
			require.NoError(t, marshalErr)

			insertCas, insertErr := cp.insertConfig(c, configKey, configBody)
			require.NoError(t, insertErr)

			var loadedConfig map[string]interface{}
			loadCas, loadErr := cp.loadConfig(c, configKey, &loadedConfig)
			require.NoError(t, loadErr)
			assert.Equal(t, insertCas, loadCas)
			assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

			rawConfig, rawCas, rawErr := cp.loadRawConfig(c, configKey)
			require.NoError(t, rawErr)
			assert.Equal(t, insertCas, uint64(rawCas))
			assert.Equal(t, rawConfigBody, rawConfig)

			configBody["updated"] = true
			updatedRawBody, marshalErr := JSONMarshal(configBody)
			require.NoError(t, marshalErr)

			// update with incorrect cas
			_, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, 1234)
			require.Error(t, updateErr)

			// update with correct cas
			updateCas, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, gocb.Cas(insertCas))
			require.NoError(t, updateErr)

			// retrieve config, validate updated value
			var updatedConfig map[string]interface{}
			loadCas, loadErr = cp.loadConfig(c, configKey, &updatedConfig)
			require.NoError(t, loadErr)
			assert.Equal(t, updateCas, gocb.Cas(loadCas))
			assert.Equal(t, configBody["updated"], updatedConfig["updated"])

			// retrieve raw config, validate updated value
			rawConfig, rawCas, rawErr = cp.loadRawConfig(c, configKey)
			require.NoError(t, rawErr)
			assert.Equal(t, updateCas, rawCas)
			assert.Equal(t, updatedRawBody, rawConfig)

			// delete with incorrect cas
			_, removeErr := cp.removeRawConfig(c, configKey, gocb.Cas(insertCas))
			require.Error(t, removeErr)

			// delete with correct cas
			_, removeErr = cp.removeRawConfig(c, configKey, updateCas)
			require.NoError(t, removeErr)

			// attempt to retrieve config, validate not found
			var deletedConfig map[string]interface{}
			loadCas, loadErr = cp.loadConfig(c, configKey, &deletedConfig)
			assert.Equal(t, ErrNotFound, loadErr)

			// attempt to retrieve raw config, validate updated value
			rawConfig, rawCas, rawErr = cp.loadRawConfig(c, configKey)
			assert.Equal(t, ErrNotFound, loadErr)
		})
	}

}
