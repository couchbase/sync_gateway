// Copyright 2023-Present Couchbase, Inc.
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRemoveCollection tests when a collection has been removed from CBS, and the server is restarted. We should be able to modify or delete the database.
func TestRemoveCollection(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("test relies on bootstrap connection and needs CBS")
	}
	base.TestRequiresCollections(t)
	base.RequireNumTestBuckets(t, 2)
	numCollections := 2
	bucket := base.GetPersistentTestBucket(t)
	defer bucket.Close(base.TestCtx(t))
	base.RequireNumTestDataStores(t, numCollections)
	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket:             bucket.NoCloseClone(),
		PersistentConfig:             true,
		GroupID:                      base.StringPtr(t.Name()),
		AdminInterfaceAuthentication: true,
	}
	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = rest.GetCollectionsConfig(t, rt.TestBucket, numCollections)

	dbName := "removecollectiondb"

	dbcJSON, err := base.JSONMarshal(dbConfig)
	require.NoError(t, err)
	resp := rt.SendAdminRequestWithAuth(http.MethodPut, "/"+dbName+"/", string(dbcJSON), base.TestClusterUsername(), base.TestClusterPassword())
	rest.RequireStatus(t, resp, http.StatusCreated)

	dataStores := rt.TestBucket.GetNonDefaultDatastoreNames()
	deletedDataStore := dataStores[1]

	defer func() {
		assert.NoError(t, bucket.CreateDataStore(rt.Context(), deletedDataStore))

	}()
	// drop a data store
	require.NoError(t, rt.TestBucket.DropDataStore(deletedDataStore))
	require.Len(t, rt.TestBucket.GetNonDefaultDatastoreNames(), len(dataStores)-1)

	rt.Close()
	rtConfig = &rest.RestTesterConfig{
		CustomTestBucket:             bucket.NoCloseClone(),
		PersistentConfig:             true,
		GroupID:                      base.StringPtr(t.Name()),
		AdminInterfaceAuthentication: true,
	}

	rt = rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	bucket2Role := rest.RouteRole{
		RoleName:       rest.MobileSyncGatewayRole.RoleName,
		DatabaseScoped: true,
	}
	if base.TestsUseServerCE() {
		bucket2Role = rest.RouteRole{
			RoleName:       rest.BucketFullAccessRole.RoleName,
			DatabaseScoped: true,
		}
	}

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	altBucket := base.GetTestBucket(t)
	defer altBucket.Close(base.TestCtx(t))
	const password = "password2"
	rest.MakeUser(t, httpClient, eps[0], bucket2Role.RoleName, password, []string{fmt.Sprintf("%s[%s]", bucket2Role.RoleName, altBucket.GetName())})
	defer rest.DeleteUser(t, httpClient, eps[0], bucket2Role.RoleName)

	delete(dbConfig.Scopes[deletedDataStore.ScopeName()].Collections, deletedDataStore.CollectionName())

	dbcJSON, err = base.JSONMarshal(dbConfig)
	require.NoError(t, err)

	resp = rt.SendAdminRequestWithAuth(http.MethodPost, "/"+dbName+"/", string(dbcJSON), base.TestClusterUsername(), base.TestClusterPassword())
	rest.RequireStatus(t, resp, http.StatusForbidden)

	// wrong RBAC user
	resp = rt.SendAdminRequestWithAuth(http.MethodDelete, "/"+dbName+"/", "", bucket2Role.RoleName, password)
	rest.RequireStatus(t, resp, http.StatusForbidden)

	// bad credentials
	resp = rt.SendAdminRequestWithAuth(http.MethodDelete, "/"+dbName+"/", "", "baduser", "badpassword")
	rest.RequireStatus(t, resp, http.StatusUnauthorized)

	resp = rt.SendAdminRequestWithAuth(http.MethodDelete, "/"+dbName+"/", "", base.TestClusterUsername(), base.TestClusterPassword())
	rest.RequireStatus(t, resp, http.StatusOK)

}
