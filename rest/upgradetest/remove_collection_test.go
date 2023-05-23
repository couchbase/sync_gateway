// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package upgradetest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveCollection(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("test relies on boostrap connection and needs CBS")
	}
	base.TestRequiresCollections(t)
	numCollections := 2
	bucket := base.GetPersistentTestBucket(t)
	defer bucket.Close()
	base.RequireNumTestDataStores(t, numCollections)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: bucket.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          base.StringPtr(t.Name()),
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.GetCollectionsConfig(t, rt.TestBucket, numCollections)

	dbName := "removecollectiondb"
	resp := rt.CreateDatabase(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	dataStores := rt.TestBucket.GetNonDefaultDatastoreNames()
	deletedDataStore := dataStores[1]

	defer func() {
		assert.NoError(t, bucket.CreateDataStore(deletedDataStore))

	}()
	// drop a data store
	require.NoError(t, rt.TestBucket.DropDataStore(deletedDataStore))
	require.Len(t, rt.TestBucket.GetNonDefaultDatastoreNames(), len(dataStores)-1)

	/*resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_offline", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_online", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	delete(dbConfig.Scopes[deletedDataStore.ScopeName()].Collections, deletedDataStore.CollectionName())
	resp = rt.UpsertDbConfig(dbName, dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)
	*/

	rt.Close()
	rtConfig = &rest.RestTesterConfig{
		CustomTestBucket: bucket.NoCloseClone(),
		PersistentConfig: true,
		GroupID:          base.StringPtr(t.Name()),
	}

	rt = rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	resp = rt.SendAdminRequest(http.MethodDelete, "/"+dbName+"/", "")
	rest.RequireStatus(t, resp, http.StatusOK)

}
