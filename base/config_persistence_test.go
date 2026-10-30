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
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestConfigPersistence ensures that all implementations of ConfigPersistence behave as expected for common operations.
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
		// failedWriteKVOps is the KV op count of an insert of an existing doc, a replace with a stale CAS, and a
		// remove of a missing doc. kv_ops counts these for plain KV commands, but not for subdoc commands.
		failedWriteKVOps int
	}{
		{
			name:                  "document body persistence",
			configPersistenceImpl: &DocumentBootstrapPersistence{},
			failedWriteKVOps:      1,
		},
		{
			name:                  "xattr persistence",
			configPersistenceImpl: &XattrBootstrapPersistence{},
			failedWriteKVOps:      0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cp := testCase.configPersistenceImpl
			configBody := make(map[string]any)
			configBody["sampleConfig"] = "value"
			configKey := "testConfigKey"
			rawConfigBody, marshalErr := JSONMarshal(configBody)
			require.NoError(t, marshalErr)

			insertCas, kvOps, insertErr := cp.insertConfig(c, configKey, configBody)
			require.NoError(t, insertErr)
			assert.Equal(t, 1, kvOps)

			// attempt to re-insert, must return ErrAlreadyExists
			_, kvOps, reinsertErr := cp.insertConfig(c, configKey, configBody)
			require.Equal[error](t, ErrAlreadyExists, reinsertErr)
			assert.Equal(t, testCase.failedWriteKVOps, kvOps)

			ctx := TestCtx(t)
			var loadedConfig map[string]any
			loadCas, _, loadErr := cp.loadConfig(ctx, c, configKey, &loadedConfig)
			require.NoError(t, loadErr)
			assert.Equal(t, insertCas, loadCas)
			assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])
			rawConfig, rawCas, _, rawErr := cp.loadRawConfig(ctx, c, configKey)
			require.NoError(t, rawErr)
			assert.Equal(t, insertCas, uint64(rawCas))
			assert.Equal(t, rawConfigBody, rawConfig)

			configBody["updated"] = true
			updatedRawBody, marshalErr := JSONMarshal(configBody)
			require.NoError(t, marshalErr)

			// update with incorrect cas
			_, kvOps, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, 1234)
			require.Error(t, updateErr)
			assert.Equal(t, testCase.failedWriteKVOps, kvOps)

			// update with correct cas
			updateCas, kvOps, updateErr := cp.replaceRawConfig(c, configKey, updatedRawBody, gocb.Cas(insertCas))
			require.NoError(t, updateErr)
			assert.Equal(t, 1, kvOps)

			// retrieve config, validate updated value
			var updatedConfig map[string]any
			loadCas, _, loadErr = cp.loadConfig(ctx, c, configKey, &updatedConfig)
			require.NoError(t, loadErr)
			assert.Equal(t, updateCas, gocb.Cas(loadCas))
			assert.Equal(t, configBody["updated"], updatedConfig["updated"])

			// retrieve raw config, validate updated value
			rawConfig, rawCas, _, rawErr = cp.loadRawConfig(ctx, c, configKey)
			require.NoError(t, rawErr)
			assert.Equal(t, updateCas, rawCas)
			assert.JSONEq(t, string(updatedRawBody), string(rawConfig))

			// delete with incorrect cas, which kv_ops leaves out for a plain remove as well
			_, kvOps, removeErr := cp.removeRawConfig(c, configKey, gocb.Cas(insertCas))
			require.Error(t, removeErr)
			assert.Equal(t, 0, kvOps)

			// delete with correct cas
			_, kvOps, removeErr = cp.removeRawConfig(c, configKey, updateCas)
			require.NoError(t, removeErr)
			assert.Equal(t, 1, kvOps)

			// delete again
			_, kvOps, removeErr = cp.removeRawConfig(c, configKey, 0)
			require.Error(t, removeErr)
			assert.Equal(t, testCase.failedWriteKVOps, kvOps)

			// attempt to retrieve config, validate not found
			var deletedConfig map[string]any
			_, _, loadErr = cp.loadConfig(ctx, c, configKey, &deletedConfig)
			assert.Equal[error](t, ErrNotFound, loadErr)

			// attempt to retrieve raw config, validate updated value
			_, _, _, rawErr = cp.loadRawConfig(ctx, c, configKey)
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
	configBody := make(map[string]any)
	configBody["sampleConfig"] = "value"
	configKey := "testConfigKey"
	_, marshalErr := JSONMarshal(configBody)
	require.NoError(t, marshalErr)

	_, _, insertErr := cp.insertConfig(c, configKey, configBody)
	require.NoError(t, insertErr)

	// modify the document body directly in the bucket
	updatedBody := make(map[string]any)
	updatedBody["unexpected"] = "value"
	err := dataStore.Set(ctx, configKey, 0, nil, updatedBody)
	require.NoError(t, err)

	// attempt to re-insert, must return ErrAlreadyExists
	_, _, reinsertErr := cp.insertConfig(c, configKey, configBody)
	require.Equal[error](t, ErrAlreadyExists, reinsertErr)

	// Retrieve the config
	var loadedConfig map[string]any
	_, kvOps, loadErr := cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, 1, kvOps)
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// set the document to an empty body, shouldn't be treated as delete
	err = dataStore.Set(ctx, configKey, 0, nil, nil)
	require.NoError(t, err)

	// Retrieve the config
	_, kvOps, loadErr = cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, 1, kvOps, "an empty body is not a deleted doc, so it is not restored")
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// Fetch the document directly from the bucket to verify resurrect handling didn't occur
	var docBody map[string]any
	_, err = dataStore.Get(ctx, configKey, &docBody)
	assert.NoError(t, err)
	assert.True(t, docBody == nil)

	// delete the document directly in the bucket (system xattr will be preserved)
	deleteErr := dataStore.Delete(ctx, configKey)
	assert.NoError(t, deleteErr)

	// Retrieve the config
	_, kvOps, loadErr = cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, 2, kvOps, "the deleted doc is read and then restored")
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// Fetch the document directly from the bucket to verify resurrect handling DID occur
	_, err = dataStore.Get(ctx, configKey, &docBody)
	assert.NoError(t, err)
	assert.True(t, docBody != nil)

	// Retrieve the config
	_, kvOps, loadErr = cp.loadConfig(ctx, c, configKey, &loadedConfig)
	require.NoError(t, loadErr)
	assert.Equal(t, 1, kvOps, "the doc was restored by the last read")
	assert.Equal(t, configBody["sampleConfig"], loadedConfig["sampleConfig"])

	// A remove of a doc with no config xattr and one of a missing doc both return ErrNotFound, but kv_ops counts only
	// the first, which fails on a path rather than on the whole doc.
	bodyOnlyKey := "testBodyOnlyKey"
	require.NoError(t, dataStore.Set(ctx, bodyOnlyKey, 0, nil, map[string]any{"v": 1}))
	_, kvOps, removeErr := cp.removeRawConfig(c, bodyOnlyKey, 0)
	require.Equal[error](t, ErrNotFound, removeErr)
	assert.Equal(t, 1, kvOps, "no config xattr")
	_, kvOps, removeErr = cp.removeRawConfig(c, "testMissingKey", 0)
	require.Equal[error](t, ErrNotFound, removeErr)
	assert.Equal(t, 0, kvOps, "missing doc")
}

// TestConfigPersistenceXattrFormatMismatches ensures that the XattrFormatMismatches stat is incremented when loading a config of a different format.
func TestConfigPersistenceXattrFormatMismatches(t *testing.T) {

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

	nonXattrConfigPersistence := &DocumentBootstrapPersistence{}
	nonXattrConfigKey := "testNonXattrConfigKey"
	nonXattrConfigBody := map[string]any{
		"sampleConfig": "value",
	}

	xattrConfigKey := "testXattrConfigKey"
	xattrConfigPersistence := &XattrBootstrapPersistence{}
	xattrConfigBody := map[string]any{
		"sampleConfig": "value",
	}

	// create config without xattrs
	_, marshalErr := JSONMarshal(nonXattrConfigBody)
	require.NoError(t, marshalErr)
	_, _, insertErr := nonXattrConfigPersistence.insertConfig(c, nonXattrConfigKey, nonXattrConfigBody)
	require.NoError(t, insertErr)

	existingStat := SyncGatewayStats.GlobalStats.ConfigStat.XattrFormatMismatches.Value()

	// load config in xattr mode
	data, _, _, err := xattrConfigPersistence.loadRawConfig(ctx, c, nonXattrConfigKey)
	require.Error(t, err)
	assert.Nil(t, data)
	assert.Equal(t, existingStat+1, SyncGatewayStats.GlobalStats.ConfigStat.XattrFormatMismatches.Value())

	// we can try the other way around (xattr config loading in non-xattr mode) - but it won't have any effect on the stat. This works in one way only.
	_, marshalErr = JSONMarshal(xattrConfigBody)
	require.NoError(t, marshalErr)
	_, _, insertErr = xattrConfigPersistence.insertConfig(c, xattrConfigKey, xattrConfigBody)
	require.NoError(t, insertErr)

	existingStat = SyncGatewayStats.GlobalStats.ConfigStat.XattrFormatMismatches.Value()

	// load config in non-xattr mode
	data, _, _, err = nonXattrConfigPersistence.loadRawConfig(ctx, c, xattrConfigKey)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"cfgVersion":1}`), data) // whitespace stripped cfgXattrBody

	// this stat isn't effective since it only works when loading a non-xattr config in xattr mode
	assert.Equal(t, existingStat, SyncGatewayStats.GlobalStats.ConfigStat.XattrFormatMismatches.Value())
}
