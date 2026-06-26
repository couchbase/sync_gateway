// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package upgradetest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestGetUserChannelHistoryAfterCollectionUpgrade verifies channel history is reported for both the
// default collection and a named collection when a database originally configured with only
// _default._default is upgraded to add a named collection (in the default scope), and the user
// gains then loses channel access in that new collection.
//
// This test lives in the upgradetest package because it requires named collections to be created in
// the _default scope (UseDefaultScope), so that _default._default and a named collection can coexist
// in a single database - Sync Gateway only supports one named scope per database.
func TestGetUserChannelHistoryAfterCollectionUpgrade(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 1)

	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	const dbName = "db"

	// 1. Create the database with only the default collection.
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{Collections: rest.CollectionsConfig{base.DefaultCollection: {}}},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// 2. Create a user with chan1, chan2 in the default collection, then revoke them to populate
	//    the default collection's channel history.
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1",
		`{"password":"letmein","admin_channels":["chan1","chan2"]}`), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1",
		`{"admin_channels":[]}`), http.StatusOK)

	// 3. Upgrade the database to add a named collection alongside the default collection (both in
	//    the default scope).
	namedStore := rt.TestBucket.GetNonDefaultDatastoreNames()[0]
	scope := namedStore.ScopeName()
	collection := namedStore.CollectionName()
	upgradedConfig := rt.NewDbConfig()
	upgradedConfig.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{Collections: rest.CollectionsConfig{
			base.DefaultCollection: {},
			collection:             {},
		}},
	}
	rest.RequireStatus(t, rt.ReplaceDbConfig(dbName, upgradedConfig), http.StatusCreated)

	// 4. Grant the user channels in the new named collection, then revoke them to populate that
	//    collection's channel history.
	grant := fmt.Sprintf(`{"collection_access":{%q:{%q:{"admin_channels":["chan3","chan4"]}}}}`, scope, collection)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1", grant), http.StatusOK)
	revoke := fmt.Sprintf(`{"collection_access":{%q:{%q:{"admin_channels":[]}}}}`, scope, collection)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1", revoke), http.StatusOK)

	// 5. GET channel history and confirm both collections are reported.
	response := rt.SendAdminRequest(http.MethodGet, "/db/_user/user1/_access_history", "")
	rest.RequireStatus(t, response, http.StatusOK)

	var result rest.GetUserAccessHistoryResponse
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	assert.ElementsMatch(t, []string{"chan1", "chan2"}, result.Channels[base.DefaultScope][base.DefaultCollection])
	assert.ElementsMatch(t, []string{"chan3", "chan4"}, result.Channels[scope][collection])
}

// TestCompactUserChannelHistoryAfterCollectionUpgrade verifies the compact endpoint can remove
// channel history from both the default collection and a named collection when a database originally
// configured with only _default._default is upgraded to add a named collection (in the default
// scope), and the user has accrued channel history in both.
func TestCompactUserChannelHistoryAfterCollectionUpgrade(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 1)

	rt := rest.NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	const dbName = "db"

	// 1. Create the database with only the default collection.
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{Collections: rest.CollectionsConfig{base.DefaultCollection: {}}},
	}
	rest.RequireStatus(t, rt.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	// 2. Create a user with chan1, chan2 in the default collection, then revoke them to populate
	//    the default collection's channel history.
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1",
		`{"password":"letmein","admin_channels":["chan1","chan2"]}`), http.StatusCreated)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1",
		`{"admin_channels":[]}`), http.StatusOK)

	// 3. Upgrade the database to add a named collection alongside the default collection (both in
	//    the default scope).
	namedStore := rt.TestBucket.GetNonDefaultDatastoreNames()[0]
	scope := namedStore.ScopeName()
	collection := namedStore.CollectionName()
	upgradedConfig := rt.NewDbConfig()
	upgradedConfig.Scopes = rest.ScopesConfig{
		base.DefaultScope: rest.ScopeConfig{Collections: rest.CollectionsConfig{
			base.DefaultCollection: {},
			collection:             {},
		}},
	}
	rest.RequireStatus(t, rt.ReplaceDbConfig(dbName, upgradedConfig), http.StatusCreated)

	// 4. Grant the user channels in the new named collection, then revoke them to populate that
	//    collection's channel history.
	grant := fmt.Sprintf(`{"collection_access":{%q:{%q:{"admin_channels":["chan3","chan4"]}}}}`, scope, collection)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1", grant), http.StatusOK)
	revoke := fmt.Sprintf(`{"collection_access":{%q:{%q:{"admin_channels":[]}}}}`, scope, collection)
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/db/_user/user1", revoke), http.StatusOK)

	// 5. Compact channel history in both the default and named collections.
	compactBody := fmt.Sprintf(`{"channels":{%q:{%q:["chan1","chan2"],%q:["chan3","chan4"]}}}`,
		base.DefaultScope, base.DefaultCollection, collection)
	response := rt.SendAdminRequest(http.MethodPost, "/db/_user/user1/_access_history/compact", compactBody)
	rest.RequireStatus(t, response, http.StatusOK)

	var result rest.CompactUserAccessHistoryResponse
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &result))
	assert.ElementsMatch(t, []string{"chan1", "chan2"}, result.CompactedChannels[base.DefaultScope][base.DefaultCollection])
	assert.ElementsMatch(t, []string{"chan3", "chan4"}, result.CompactedChannels[scope][collection])

	// 6. GET channel history and confirm both collections are now empty.
	getResp := rt.SendAdminRequest(http.MethodGet, "/db/_user/user1/_access_history", "")
	rest.RequireStatus(t, getResp, http.StatusOK)
	var afterHistory rest.GetUserAccessHistoryResponse
	require.NoError(t, base.JSONUnmarshal(getResp.Body.Bytes(), &afterHistory))
	assert.Empty(t, afterHistory.Channels[base.DefaultScope][base.DefaultCollection])
	assert.Empty(t, afterHistory.Channels[scope][collection])
}
