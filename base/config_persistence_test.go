// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()

	sgCollection, ok := dataStore.(*Collection)
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

			insertCas, insertErr := cp.insertConfig(c, configKey, configBody)
			require.NoError(t, insertErr)

			// attempt to re-insert, must return ErrAlreadyExists
			_, reinsertErr := cp.insertConfig(c, configKey, configBody)
			require.ErrorIs(t, reinsertErr, ErrAlreadyExists)

			ctx := TestCtx(t)
			var loadedConfig map[string]interface{}
			loadCas, loadErr := cp.loadConfig(ctx, c, configKey, &loadedConfig)
			require.NoError(t, loadErr)
			assert.Equal(t, insertCas, loadCas)
			assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])
			rawConfig, rawCas, rawErr := cp.loadRawConfig(ctx, c, configKey)
			require.NoError(t, rawErr)
			assert.Equal(t, insertCas, uint64(rawCas))
			require.JSONEq(t, string(MustJSONMarshal(t, configBody)), string(rawConfig))

			configBody["updated"] = true
			updatedRawBody := MustJSONMarshal(t, configBody)

			// update with incorrect cas
			_, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, 1234)
			require.Error(t, updateErr)

			// update with correct cas
			updateCas, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, gocb.Cas(insertCas))
			require.NoError(t, updateErr)

			// retrieve config, validate updated value
			var updatedConfig map[string]interface{}
			loadCas, loadErr = cp.loadConfig(ctx, c, configKey, &updatedConfig)
			require.NoError(t, loadErr)
			assert.Equal(t, updateCas, gocb.Cas(loadCas))
			assert.Equal(t, configBody["updated"], updatedConfig["updated"])

			// retrieve raw config, validate updated value
			rawConfig, rawCas, rawErr = cp.loadRawConfig(ctx, c, configKey)
			require.NoError(t, rawErr)
			assert.Equal(t, updateCas, rawCas)
			require.JSONEq(t, string(updatedRawBody), string(rawConfig))

			// delete with incorrect cas
			_, removeErr := cp.removeRawConfig(c, configKey, gocb.Cas(insertCas))
			require.Error(t, removeErr)

			// delete with correct cas
			_, removeErr = cp.removeRawConfig(c, configKey, updateCas)
			require.NoError(t, removeErr)

			// attempt to retrieve config, validate not found
			var deletedConfig map[string]interface{}
			_, loadErr = cp.loadConfig(ctx, c, configKey, &deletedConfig)
			require.ErrorIs(t, loadErr, ErrNotFound)

			// attempt to retrieve raw config, validate updated value
			_, _, rawErr = cp.loadRawConfig(ctx, c, configKey)
			require.Error(t, rawErr)
		})
	}

}

func TestXattrConfigPersistence(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()

	sgCollection, ok := dataStore.(*Collection)
	require.True(t, ok)

	// create config
	c := sgCollection.Collection
	cp := &XattrBootstrapPersistence{}
	configBody := make(map[string]interface{})
	configBody["sampleConfig"] = "value"
	configKey := "testConfigKey"

	insertCas, insertErr := cp.insertConfig(c, configKey, configBody)
	require.NoError(t, insertErr)

	// modify the document body directly in the bucket
	updatedBody := make(map[string]interface{})
	updatedBody["unexpected"] = "value"
	err := dataStore.Set(configKey, 0, nil, updatedBody)
	require.NoError(t, err)

	// attempt to re-insert, must return ErrAlreadyExists
	_, reinsertErr := cp.insertConfig(c, configKey, configBody)
	require.ErrorIs(t, reinsertErr, ErrAlreadyExists)

	// Retrieve the config, cas should still match insertCas
	var loadedConfig map[string]interface{}
	loadCas, loadErr := cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, insertCas, loadCas)
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// set the document to an empty body, shouldn't be treated as delete
	err = dataStore.Set(configKey, 0, nil, nil)
	require.NoError(t, err)

	// Retrieve the config, cas should still match insertCas
	loadCas, loadErr = cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, insertCas, loadCas)
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// Fetch the document directly from the bucket to verify resurrect handling didn't occur
	var docBody map[string]interface{}
	_, err = dataStore.Get(configKey, &docBody)
	require.NoError(t, err)
	require.Nil(t, docBody)

	// delete the document directly in the bucket (system xattr will be preserved)
	deleteErr := dataStore.Delete(configKey)
	require.NoError(t, deleteErr)

	// Retrieve the config, cas should still match insertCas
	loadCas, loadErr = cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, insertCas, loadCas)
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// Fetch the document directly from the bucket to verify resurrect handling DID occur
	_, err = dataStore.Get(configKey, &docBody)
	assert.NoError(t, err)
	require.NotNil(t, docBody)

	// Retrieve the config, cas should still match insertCas
	loadCas, loadErr = cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, insertCas, loadCas)
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])
	/*
		rawConfig, rawCas, rawErr := cp.loadRawConfig(c, configKey)
		require.NoError(t, rawErr)
		assert.Equal(t, insertCas, uint64(rawCas))
		assert.Equal(t, rawConfigBody, rawConfig)

		configBody["updated"] = true
		updatedRawBody, marshalErr := JSONMarshal(configBody)
		require.NoError(t, marshalErr)

		// update with incorrect cas
		_, _, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, 1234)
		require.Error(t, updateErr)

		// update with correct cas
		updateCas, _, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, gocb.Cas(insertCas))
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
	*/

}
