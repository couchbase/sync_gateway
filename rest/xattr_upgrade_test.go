// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test retrieval of a document stored w/ xattrs when running in non-xattr mode (upgrade handling).
func TestCheckForUpgradeOnRead(t *testing.T) {

	// Only run when xattrs are disabled, but requires couchbase server since we're writing an xattr directly to the bucket
	if base.TestUseXattrs() {
		t.Skip("Check for upgrade test only runs w/ SG_TEST_USE_XATTRS=false")
	}

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: false,
		}},
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// Write doc in SG format (doc + xattr) directly to the bucket
	key := "TestCheckForUpgrade"
	bodyString := `
{
  "value": "2-d"
}`
	xattrString := `
{
    "rev": "2-d",
    "flags": 24,
    "sequence": 5,
    "recent_sequences": [1,2,3,4,5],
    "history": {
      "revs": [
        "1-089c019bbfaba27047008599143bc66f",
        "2-b92296d32600ec90dc05ff18ae61a1e8",
        "2-b",
        "2-c",
        "2-d"
      ],
      "parents": [-1,0,0,0,0],
      "bodymap": {
        "2": "{\"value\":\"%s\"}",
        "3": "{\"value\":\"%s\"}"
      },
      "bodyKeyMap": {
      	"1": "_sync:rb:test"
      },
      "channels": [null,null,null,null,null]
    },
    "cas": "",
    "time_saved": "2017-09-14T23:54:25.975220906-07:00"
}`

	ctx := base.TestCtx(t)
	// Create via the SDK with sync metadata intact
	_, err := dataStore.WriteCasWithXattr(ctx, key, base.SyncXattrName, 0, 0, []byte(bodyString), []byte(xattrString), nil)
	assert.NoError(t, err, "Error writing doc w/ xattr")

	// Attempt to get the documents via Sync Gateway.  Should successfully retrieve doc by triggering
	// checkForUpgrade handling to detect metadata in xattr.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+key, "")
	assert.Equal(t, 200, response.Code)
	log.Printf("response:%s", response.Body.Bytes())

	// Validate non-xattr write doesn't get upgraded
	nonMobileKey := "TestUpgradeNoXattr"
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["channels"] = "ABC"
	_, err = dataStore.Add(nonMobileKey, 0, nonMobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the non-mobile via Sync Gateway.  Should return 404.
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+nonMobileKey, "")
	assert.Equal(t, 404, response.Code)
}

// Test retrieval of a document stored w/ xattrs when running in non-xattr mode (upgrade handling).
func TestCheckForUpgradeOnWrite(t *testing.T) {

	// Only run when xattrs are disabled, but requires couchbase server since we're writing an xattr directly to the bucket
	if base.TestUseXattrs() {
		t.Skip("Check for upgrade test only runs w/ SG_TEST_USE_XATTRS=false")
	}

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: false,
		}},
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD, base.KeyCache)

	// Write doc in SG format (doc + xattr) directly to the bucket
	key := "TestCheckForUpgrade"
	bodyString := `
{
  "value": "2-d"
}`
	xattrString := `
{
    "rev": "2-d",
    "flags": 24,
    "sequence": 5,
    "recent_sequences": [1,2,3,4,5],
    "history": {
      "revs": [
        "1-089c019bbfaba27047008599143bc66f",
        "2-b92296d32600ec90dc05ff18ae61a1e8",
        "2-b",
        "2-c",
        "2-d"
      ],
      "parents": [-1,0,0,0,0],
      "bodymap": {
        "2": "{\"value\":\"%s\"}",
        "3": "{\"value\":\"%s\"}"
      },
      "bodyKeyMap": {
      	"1": "_sync:rb:test"
      },
      "channels": [null,null,null,null,null]
    },
    "cas": "",
    "time_saved": "2017-09-14T23:54:25.975220906-07:00"
}`

	ctx := base.TestCtx(t)
	// Create via the SDK with sync metadata intact
	_, err := dataStore.WriteCasWithXattr(ctx, key, base.SyncXattrName, 0, 0, []byte(bodyString), []byte(xattrString), nil)
	assert.NoError(t, err, "Error writing doc w/ xattr")
	require.NoError(t, rt.WaitForSequence(5))

	// Attempt to update the documents via Sync Gateway.  Should trigger checkForUpgrade handling to detect metadata in xattr, and update normally.
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=2-d", key), `{"updated":true}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("response:%s", response.Body.Bytes())

	rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, rawResponse.Code)
	log.Printf("raw response:%s", rawResponse.Body.Bytes())
	require.NoError(t, rt.WaitForSequence(6))

	// Validate non-xattr document doesn't get upgraded on attempted write
	nonMobileKey := "TestUpgradeNoXattr"
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["channels"] = "ABC"
	_, err = dataStore.Add(nonMobileKey, 0, nonMobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to update the non-mobile document via Sync Gateway.  Should return
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+nonMobileKey, `{"updated":true}`)
	assert.Equal(t, 409, response.Code)
	log.Printf("response:%s", response.Body.Bytes())
}

// Test feed processing of a document stored w/ xattrs when running in non-xattr mode (upgrade handling).
func TestCheckForUpgradeFeed(t *testing.T) {

	// Only run when xattrs are disabled, but requires couchbase server since we're writing an xattr directly to the bucket
	if base.TestUseXattrs() {
		t.Skip("Check for upgrade test only runs w/ SG_TEST_USE_XATTRS=false")
	}

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD, base.KeyCache)

	// Write doc in SG format (doc + xattr) directly to the bucket
	key := "TestCheckForUpgrade"
	bodyString := `
{
  "value": "2-d"
}`
	xattrString := `
{
    "rev": "1-089c019bbfaba27047008599143bc66f",
    "sequence": 1,
    "recent_sequences": [1],
    "history": {
      "revs": [
        "1-089c019bbfaba27047008599143bc66f"
      ],
      "parents": [-1],
      "channels": [null]
    },
    "cas": "",
    "time_saved": "2017-09-14T23:54:25.975220906-07:00"
}`

	ctx := base.TestCtx(t)
	// Create via the SDK with sync metadata intact
	_, err := dataStore.WriteCasWithXattr(ctx, key, base.SyncXattrName, 0, 0, []byte(bodyString), []byte(xattrString), nil)
	assert.NoError(t, err, "Error writing doc w/ xattr")
	require.NoError(t, rt.WaitForSequence(1))

	// Attempt to update the documents via Sync Gateway.  Should trigger checkForUpgrade handling to detect metadata in xattr, and update normally.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_changes", "")
	assert.Equal(t, 200, response.Code)
	log.Printf("response:%s", response.Body.Bytes())

	// Validate non-xattr document doesn't get processed on attempted feed read
	nonMobileKey := "TestUpgradeNoXattr"
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["channels"] = "ABC"

	_, err = dataStore.Add(nonMobileKey, 0, nonMobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// We don't have a way to wait for a upgrade that doesn't happen, but we can look for the warning that happens.
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.Cache().NonMobileIgnoredCount.Value()
	}, 1)
}
