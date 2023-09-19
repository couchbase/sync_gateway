//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectionsPutDocInKeyspace creates a collection and starts up a RestTester instance on it.
// Ensures that various keyspaces can or can't be used to insert a doc in the collection.
func TestCollectionsPutDocInKeyspace(t *testing.T) {
	const (
		username = "alice"
		password = "pass"
		// dbName is the default name from RestTester
		dbName = "db"
	)
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					username: {Password: base.StringPtr(password)},
				},
			},
		},
	})
	defer rt.Close()

	ds := rt.GetSingleDataStore()
	dataStoreName, ok := base.AsDataStoreName(ds)
	require.True(t, ok)
	tests := []struct {
		name               string
		keyspace           string
		expectedStatus     int
		requireCollections bool
	}{
		{
			name:           "fully qualified",
			keyspace:       rt.GetSingleKeyspace(),
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "collection only",
			keyspace:       strings.Join([]string{dbName, dataStoreName.CollectionName()}, base.ScopeCollectionSeparator),
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "invalid collection",
			keyspace:       strings.Join([]string{dbName, dataStoreName.ScopeName(), "buzz"}, base.ScopeCollectionSeparator),
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "invalid scope",
			keyspace:       strings.Join([]string{dbName, "buzz", dataStoreName.CollectionName()}, base.ScopeCollectionSeparator),
			expectedStatus: http.StatusNotFound,
		},
		{
			name:               "no default db",
			keyspace:           dbName,
			expectedStatus:     http.StatusNotFound,
			requireCollections: true,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.requireCollections {
				base.TestRequiresCollections(t)
			}
			docID := fmt.Sprintf("doc%d", i)
			path := fmt.Sprintf("/%s/%s", test.keyspace, docID)
			resp := rt.SendUserRequestWithHeaders(http.MethodPut, path, `{"test":true}`, nil, username, password)
			RequireStatus(t, resp, test.expectedStatus)
			if test.expectedStatus == http.StatusNotFound {
				require.Contains(t, resp.Body.String(), test.keyspace)
				// assert special case where /db/docID returns db._default._default
				if test.keyspace == dbName {
					require.Contains(t, resp.Body.String(), "keyspace db not found")
				}
			}
			if test.expectedStatus == http.StatusCreated {
				// go and check that the doc didn't just end up in the default collection of the test bucket
				docBody, _, err := ds.GetRaw(docID)
				assert.NoError(t, err)
				assert.NotNil(t, docBody)

				defaultDataStore := rt.Bucket().DefaultDataStore()
				_, err = defaultDataStore.Get(docID, &gocb.GetOptions{})
				if rt.GetDatabase().OnlyDefaultCollection() {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			}
		})
	}
}

// TestCollectionsPublicChannel ensures that a doc routed to the public channel is accessible by a user with no other access.
func TestCollectionsPublicChannel(t *testing.T) {
	const (
		username = "alice"
		password = "pass"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					username: {Password: base.StringPtr(password)},
				},
			},
		},
	})
	defer rt.Close()

	pathPublic := "/{{.keyspace}}/docpublic"
	resp := rt.SendAdminRequest(http.MethodPut, pathPublic, `{"channels":["!"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, pathPublic, "", nil, username, password)
	RequireStatus(t, resp, http.StatusOK)

	pathPrivate := "/{{.keyspace}}/docprivate"
	resp = rt.SendAdminRequest(http.MethodPut, pathPrivate, `{"channels":["a"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, pathPrivate, "", nil, username, password)
	RequireStatus(t, resp, http.StatusForbidden)

	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/_all_docs?include_docs=true", "", nil, username, password)
	RequireStatus(t, resp, http.StatusOK)
	t.Logf("all docs resp: %s", resp.BodyBytes())
	var alldocsresp struct {
		Rows      []interface{} `json:"rows"`
		TotalRows int           `json:"total_rows"`
	}
	err := json.Unmarshal(resp.BodyBytes(), &alldocsresp)
	require.NoError(t, err)
	assert.Equal(t, 1, alldocsresp.TotalRows)
	assert.Len(t, alldocsresp.Rows, 1)
}

// TestNoCollectionsPutDocWithKeyspace ensures that a keyspace can't be used to insert a doc on a database not configured for collections.
func TestNoCollectionsPutDocWithKeyspace(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// Force use of no scopes intentionally
	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{
		CustomTestBucket: tb,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
			},
		},
	})
	defer rt.Close()

	// can't put doc into invalid keyspaces
	response := rt.SendAdminRequest("PUT", "/db.invalidScope.invalidCollection/doc1", "{}")
	RequireStatus(t, response, http.StatusNotFound)

	response = rt.SendAdminRequest("PUT", "/db.invalidCollection/doc1", "{}")
	RequireStatus(t, response, http.StatusNotFound)

	// can put doc into _default scope/collection explicitly ... or implicitly (tested elsewhere e.g: TestPutEmptyDoc)
	response = rt.SendAdminRequest("PUT", "/db._default._default/doc1", "{}")
	RequireStatus(t, response, http.StatusCreated)

	// retrieve doc in both ways (_default._default and no fully-qualified keyspace)
	response = rt.SendAdminRequest("GET", "/db._default._default/doc1", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc1","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`, string(response.BodyBytes()))

	response = rt.SendAdminRequest("GET", "/db/doc1", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc1","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`, string(response.BodyBytes()))
}

func TestSingleCollectionDCP(t *testing.T) {
	base.TestRequiresCollections(t)
	if !base.TestUseXattrs() {
		t.Skip("Test relies on import - needs xattrs")
	}

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
			},
		},
	})
	defer rt.Close()

	tc := rt.GetSingleDataStore()

	const docID = "doc1"

	ok, err := tc.AddRaw(docID, 0, []byte(`{"test":true}`))
	require.NoError(t, err)
	require.True(t, ok)

	// ensure the doc is picked up by the import DCP feed and actually gets imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	require.NoError(t, err)

	require.NoError(t, rt.WaitForDoc(docID))
}

func TestMultiCollectionDCP(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SkipImportTestsIfNotEnabled(t)

	const numCollections = 2

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{AutoImport: true}},
	}, numCollections)
	defer rt.Close()

	colls := rt.GetDbCollections()
	require.Len(t, colls, numCollections)

	for _, c := range colls {
		_, err := c.GetCollectionDatastore().Add(t.Name(), 0, map[string]any{"test": true})
		require.NoError(t, err)
	}

	// ensure the docs are picked up by the import DCP feed and actually gets imported
	err := rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == numCollections
	})
	require.NoError(t, err)

	require.NoError(t, rt.WaitForPendingChanges())

	for _, ks := range rt.GetKeyspaces() {
		_, err = rt.WaitForChanges(1, fmt.Sprintf("/%s/_changes", ks), "", true)
		require.NoError(t, err)
	}
}

func TestMultiCollectionChannelAccess(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	tb := base.GetPersistentTestBucket(t)
	defer tb.Close(ctx)

	scopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	c1SyncFunction := `function(doc) {channel(doc.chan);}`

	scope := dataStoreNames[0].ScopeName()
	collection1 := dataStoreNames[0].CollectionName()
	collection2 := dataStoreNames[1].CollectionName()

	scopesConfig[scope].Collections[collection1] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[scope].Collections[collection2] = CollectionConfig{SyncFn: &c1SyncFunction}

	fmt.Println(scopesConfig)
	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Scopes:           scopesConfig,
			NumIndexReplicas: base.UintPtr(0),
			EnableXattrs:     base.BoolPtr(base.TestUseXattrs()),
		},
		},
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 3)
	defer rt.Close()

	userPayload := `{
		"password":"letmein",
		"collection_access": {
			"%s": {
				"%s": {
					"admin_channels":%s
				}
			}
		}
	}`

	// Create a few users with access to various channels via admin grants
	resp := rt.SendAdminRequest("PUT", "/db/_user/userA", fmt.Sprintf(userPayload, scope, collection1, `["A"]`))
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/db/_user/userB", fmt.Sprintf(userPayload, scope, collection1, `["B"]`))
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/db/_user/userAB", fmt.Sprintf(userPayload, scope, collection1, `["A","B"]`))
	RequireStatus(t, resp, http.StatusCreated)

	// Write docs to both collections in various channels
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/testDocBarA", `{"chan":["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/testDocBarB", `{"chan":["B"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/testDocBarAB", `{"chan":["A","B"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/testDocBazA", `{"chan":["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/testDocBazB", `{"chan":["B"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	// Ensure users can only see documents in the appropriate collection/channels they should be able to have access to
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace1}}/testDocBarA", "", nil, "userA", "letmein")
	RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace1}}/testDocBarB", "", nil, "userA", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace2}}/testDocBazB", "", nil, "userB", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace1}}/testDocBarAB", "", nil, "userA", "letmein")
	RequireStatus(t, resp, http.StatusOK)

	// Add a new collection and update the db config
	scopesConfig = GetCollectionsConfig(t, tb, 3)
	dataStoreNames = GetDataStoreNamesFromScopesConfig(scopesConfig)

	collection3 := dataStoreNames[2].CollectionName()
	scopesConfig[scope].Collections[collection1] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[scope].Collections[collection2] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[scope].Collections[collection3] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfigString, err := json.Marshal(scopesConfig)
	require.NoError(t, err)

	resp = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		tb.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	RequireStatus(t, resp, http.StatusCreated)

	// Put a doc in new collection and make sure it cant be accessed
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace3}}/testDocBazA", `{"chan":["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace3}}/testDocBazA", "", nil, "userA", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)

	// Update user to set some channels on new collection
	resp = rt.SendAdminRequest("PUT", "/db/_user/userB", fmt.Sprintf(userPayload, scope, collection3, `["B"]`))
	RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendAdminRequest("PUT", "/db/_user/userAB", fmt.Sprintf(userPayload, scope, collection3, `["A","B"]`))
	RequireStatus(t, resp, http.StatusOK)

	// Ensure users can access the given channels in new collection, can't access docs in other channels on the new collection
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace3}}/testDocBazA", "", nil, "userB", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace3}}/testDocBazA", "", nil, "userAB", "letmein")
	RequireStatus(t, resp, http.StatusOK)

	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace3}}/testDocBazB", `{"chan":["B"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace3}}/testDocBazB", "", nil, "userB", "letmein")
	RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace3}}/testDocBazB", "", nil, "userAB", "letmein")
	RequireStatus(t, resp, http.StatusOK)

	// Remove collection and update the db config
	scopesConfig = GetCollectionsConfig(t, tb, 2)

	scopesConfig[scope].Collections[collection1] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[scope].Collections[collection2] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfigString, err = json.Marshal(scopesConfig)
	require.NoError(t, err)

	resp = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		tb.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	RequireStatus(t, resp, http.StatusCreated)

	// Ensure users can't access docs in a removed collection
	//
	// we can't use the {{.keyspace3}} URI template variable here as the collection no longer exists on the RestTester,
	// but we still want to try issuing the request to the old keyspace name.
	keyspace3 := "db." + scope + "." + collection3
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/"+keyspace3+"/testDocBazA", "", nil, "userB", "letmein")
	RequireStatus(t, resp, http.StatusNotFound)
}

func TestMultiCollectionDynamicChannelAccess(t *testing.T) {
	base.TestRequiresCollections(t)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	scopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	c1SyncFunction := `function(doc) {access(doc.username, [doc.grant1,doc.grant2]);
                  channel(doc.chan);
            }`

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = CollectionConfig{SyncFn: &c1SyncFunction}

	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		//PersistentConfig: true,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Scopes:           scopesConfig,
			NumIndexReplicas: base.UintPtr(0),
		},
		},
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 3)
	defer rt.Close()

	userPayload := `{
      "password":"letmein",
      "collection_access": {
         "%s": {
            "%s": {
               "admin_channels":%s
            }
         }
      }
   }`
	// Create a few users without any channel access
	resp := rt.SendAdminRequest("PUT", "/db/_user/alice", fmt.Sprintf(userPayload, dataStoreNames[0].ScopeName(), dataStoreNames[0].CollectionName(), `[]`))
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/db/_user/bob", fmt.Sprintf(userPayload, dataStoreNames[0].ScopeName(), dataStoreNames[0].CollectionName(), `[]`))
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/db/_user/abby", fmt.Sprintf(userPayload, dataStoreNames[0].ScopeName(), dataStoreNames[0].CollectionName(), `[]`))
	RequireStatus(t, resp, http.StatusCreated)

	// Write docs in each collection that runs the per-collection sync functions that grant users access to various channels
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/testDocBarA", `{"username": "alice", "grant1": "A", "chan":["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/testDocBarB", `{"username": "bob", "grant1": "B", "chan":["B"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/testDocBazAB", `{"username": "abby", "grant1": "A", "grant2": "B","chan":["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/testDocBazB", `{"chan":["B"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	// Ensure users get given access to the channel in the appropriate collection, and is not accidentally granting access for a channel of the same name in another collection
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace1}}/testDocBarA", "", nil, "bob", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace1}}/testDocBarA", "", nil, "abby", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace1}}/testDocBarA", "", nil, "alice", "letmein")
	RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace2}}/testDocBazAB", "", nil, "alice", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace2}}/testDocBazB", "", nil, "bob", "letmein")
	RequireStatus(t, resp, http.StatusForbidden)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace2}}/testDocBazB", "", nil, "abby", "letmein")
	RequireStatus(t, resp, http.StatusOK)

}

// TestCollectionsBasicIndexQuery ensures that the bucket API is able to create an index on a collection
// and query documents written to the collection.
func TestCollectionsBasicIndexQuery(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test requires N1QL")
	}

	base.TestRequiresCollections(t)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	const docID = "doc1"

	collection := rt.GetSingleTestDatabaseCollection()

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"test":true}`)
	RequireStatus(t, resp, http.StatusCreated)

	// use the rt.Bucket which has got the foo.bar scope/collection set up
	ds := rt.GetSingleDataStore()
	n1qlStore, ok := base.AsN1QLStore(ds)
	require.True(t, ok)

	idxName := t.Name() + "_primary"
	ctx := base.TestCtx(t)
	require.NoError(t, n1qlStore.CreatePrimaryIndex(ctx, idxName, nil))
	require.NoError(t, n1qlStore.WaitForIndexesOnline(ctx, []string{idxName}, false))

	res, err := n1qlStore.Query(ctx, "SELECT keyspace_id, bucket_id, scope_id from system:indexes WHERE name = $idxName",
		map[string]interface{}{"idxName": idxName}, base.RequestPlus, true)
	require.NoError(t, err)

	var indexMetaResult struct {
		BucketID   *string `json:"bucket_id"`
		ScopeID    *string `json:"scope_id"`
		KeyspaceID *string `json:"keyspace_id"`
	}
	require.NoError(t, res.One(ctx, &indexMetaResult))
	require.NotNil(t, indexMetaResult)

	// if the index was created on the _default collection in the bucket, keyspace_id is the bucket name, and the other fields are not present.
	assert.NotNilf(t, indexMetaResult.BucketID, "bucket_id was not present - index was created on the _default collection!")
	assert.NotNilf(t, indexMetaResult.ScopeID, "scope_id was not present - index was created on the _default collection!")
	require.NotNilf(t, indexMetaResult.KeyspaceID, "keyspace_id should be present")
	assert.NotEqualf(t, rt.Bucket().GetName(), *indexMetaResult.KeyspaceID, "keyspace_id was the bucket name - index was created on the _default collection!")

	// if the index was created on a collection, the keyspace_id becomes the collection, along with additional fields for bucket and scope.
	assert.Equal(t, rt.Bucket().GetName(), *indexMetaResult.BucketID)
	assert.Equal(t, collection.ScopeName, *indexMetaResult.ScopeID)
	assert.Equal(t, collection.Name, *indexMetaResult.KeyspaceID)

	// try and query the document that we wrote via SG
	res, err = n1qlStore.Query(ctx, "SELECT test FROM "+base.KeyspaceQueryToken+" WHERE test = true", nil, base.RequestPlus, true)
	require.NoError(t, err)

	var primaryQueryResult struct {
		Test *bool `json:"test"`
	}
	require.NoError(t, res.One(ctx, &primaryQueryResult))
	require.NotNil(t, primaryQueryResult)

	assert.True(t, *primaryQueryResult.Test)
}

// TestCollectionsSGIndexQuery is more of an end-to-end test to ensure SG indexes are built correctly,
// and the channel access query is able to run when pulling a document as a user, and backfill the channel cache.
func TestCollectionsSGIndexQuery(t *testing.T) {
	t.Skip("Requires config-based collection channel assignment (pending CBG-2551)")
	base.TestRequiresCollections(t)

	// force GSI for this one test
	useViews := base.BoolPtr(false)

	const (
		username       = "alice"
		password       = "letmein"
		validChannel   = "valid"
		invalidChannel = "invalid"

		validDocID   = "doc1"
		invalidDocID = "doc2"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				UseViews: useViews,
				Users: map[string]*auth.PrincipalConfig{
					username: {
						ExplicitChannels: base.SetOf(validChannel),
						Password:         base.StringPtr(password),
					},
				},
			},
		},
	})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+validDocID, `{"test": true, "channels": ["`+validChannel+`"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+invalidDocID, `{"test": true, "channels": ["`+invalidChannel+`"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/_all_docs", ``, nil, username, password)
	RequireStatus(t, resp, http.StatusOK)
	var allDocsResponse struct {
		TotalRows int `json:"total_rows"`
		Rows      []struct {
			ID string `json:"id"`
		} `json:"rows"`
	}
	require.NoError(t, base.JSONDecoder(resp.Body).Decode(&allDocsResponse))
	assert.Equal(t, 1, allDocsResponse.TotalRows)
	require.Len(t, allDocsResponse.Rows, 1)
	assert.Equal(t, validDocID, allDocsResponse.Rows[0].ID)

	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/"+validDocID, ``, nil, username, password)
	RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/"+invalidDocID, ``, nil, username, password)
	RequireStatus(t, resp, http.StatusForbidden)

	_, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes", username, false)
	require.NoError(t, err)
}

func TestCollectionsPutDBInexistentCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 1)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db2/", fmt.Sprintf(`{"bucket": "%s", "num_index_replicas":0, "scopes": {"_default": {"collections": {"new_collection": {}}}}}`, tb.GetName()))
	RequireStatus(t, resp, http.StatusForbidden)
}

func TestCollectionsPutDocInDefaultCollectionWithNamedCollections(t *testing.T) {
	base.TestRequiresCollections(t)

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// create named collection in the default scope
	const customCollectionName = "new_collection"
	dBucket := tb.GetUnderlyingBucket().(sgbucket.DynamicDataStoreBucket)
	require.NoError(t, dBucket.CreateDataStore(base.TestCtx(t), base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: customCollectionName}))
	defer func() {
		assert.NoError(t, dBucket.DropDataStore(base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: customCollectionName}))
	}()

	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	}

	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db1/", fmt.Sprintf(`{"bucket": "%s", "num_index_replicas":0, "scopes": {"_default": {"collections": {"_default": {}, "%s": {}}}}}`, tb.GetName(), customCollectionName))
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db1/doc1", `{"test": true}`)
	AssertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db1._default._default/doc2", `{"test": true}`)
	AssertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", fmt.Sprintf("/db1._default.%s/doc3", customCollectionName), `{"test": true}`)
	AssertStatus(t, resp, http.StatusCreated)
}

func TestCollectionsChangeConfigScope(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("can not create new buckets and scopes in walrus")
	}

	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	scopesAndCollections := map[string][]string{
		"fooScope": {
			"bar",
		},
		"quxScope": {
			"quux",
		},
	}
	err := base.CreateBucketScopesAndCollections(ctx, tb.BucketSpec, scopesAndCollections)
	require.NoError(t, err)
	defer func() {
		collection, err := base.AsCollection(tb.DefaultDataStore())
		require.NoError(t, err)
		cm := collection.Collection.Bucket().Collections()
		for scope := range scopesAndCollections {
			assert.NoError(t, cm.DropScope(scope, nil))
		}

	}()

	serverErr := make(chan error)
	config := BootstrapStartupConfigForTest(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs(ctx))

	// Create a DB configured with one scope
	res := BootstrapAdminRequest(t, http.MethodPut, "/db/", string(mustMarshalJSON(t, map[string]any{
		"bucket":                      tb.GetName(),
		"num_index_replicas":          0,
		"enable_shared_bucket_access": base.TestUseXattrs(),
		"use_views":                   base.TestsDisableGSI(),
		"scopes": ScopesConfig{
			"fooScope": {
				Collections: CollectionsConfig{
					"bar": {},
				},
			},
		},
	})))
	require.Equal(t, http.StatusCreated, res.StatusCode, "failed to create DB")

	// Try updating its scopes
	res = BootstrapAdminRequest(t, http.MethodPut, "/db/_config", string(mustMarshalJSON(t, map[string]any{
		"bucket":                      tb.GetName(),
		"num_index_replicas":          0,
		"enable_shared_bucket_access": base.TestUseXattrs(),
		"use_views":                   base.TestsDisableGSI(),
		"scopes": ScopesConfig{
			"quxScope": {
				Collections: CollectionsConfig{
					"quux": {},
				},
			},
		},
	})))
	base.RequireAllAssertions(t,
		assert.Equal(t, http.StatusBadRequest, res.StatusCode, "should not be able to change scope"),
		assert.Contains(t, res.Body, "cannot change scopes after database creation"),
	)
}

// TestCollecitonStats ensures that stats are specific to each collection.
func TestCollectionStats(t *testing.T) {
	base.TestRequiresCollections(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	scopesConfig := GetCollectionsConfig(t, tb, 2)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	syncFn := `
		function(doc) {
			if (doc.throwException) {
				channel(undefinedvariable);
			}
			if (doc.require) {
				requireAdmin();
			}
		}`

	scope1Name, collection1Name := dataStoreNames[0].ScopeName(), dataStoreNames[0].CollectionName()
	scope2Name, collection2Name := dataStoreNames[1].ScopeName(), dataStoreNames[1].CollectionName()
	scopesConfig[scope1Name].Collections[collection1Name] = CollectionConfig{SyncFn: &syncFn}
	scopesConfig[scope2Name].Collections[collection2Name] = CollectionConfig{SyncFn: &syncFn}

	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
		GuestEnabled:     true,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Scopes:           scopesConfig,
				NumIndexReplicas: base.UintPtr(0),
				AutoImport:       base.TestUseXattrs(),
				EnableXattrs:     base.BoolPtr(base.TestUseXattrs()),
			},
		},
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	// Wait for the DB to be ready before attempting to get initial error count
	require.NoError(t, rt.WaitForDBOnline())

	collection1Stats, err := rt.GetDatabase().DbStats.CollectionStat(scope1Name, collection1Name)
	require.NoError(t, err)
	assert.Equal(t, int64(0), collection1Stats.SyncFunctionCount.Value())
	assert.Equal(t, int64(0), collection1Stats.SyncFunctionTime.Value())
	assert.Equal(t, int64(0), collection1Stats.SyncFunctionRejectCount.Value())
	assert.Equal(t, int64(0), collection1Stats.SyncFunctionRejectAccessCount.Value())
	assert.Equal(t, int64(0), collection1Stats.SyncFunctionExceptionCount.Value())
	assert.Equal(t, int64(0), collection1Stats.ImportCount.Value())
	assert.Equal(t, int64(0), collection1Stats.NumDocReads.Value())
	assert.Equal(t, int64(0), collection1Stats.DocReadsBytes.Value())
	assert.Equal(t, int64(0), collection1Stats.NumDocWrites.Value())
	assert.Equal(t, int64(0), collection1Stats.DocWritesBytes.Value())

	collection2Stats, err := rt.GetDatabase().DbStats.CollectionStat(scope2Name, collection2Name)
	require.NoError(t, err)
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionCount.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionTime.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionRejectCount.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionRejectAccessCount.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionExceptionCount.Value())
	assert.Equal(t, int64(0), collection2Stats.ImportCount.Value())
	assert.Equal(t, int64(0), collection2Stats.NumDocReads.Value())
	assert.Equal(t, int64(0), collection2Stats.DocReadsBytes.Value())
	assert.Equal(t, int64(0), collection2Stats.NumDocWrites.Value())
	assert.Equal(t, int64(0), collection2Stats.DocWritesBytes.Value())

	doc1Contents := `{"foobar":true}`
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/doc1", doc1Contents)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Equal(t, int64(1), collection1Stats.NumDocWrites.Value())
	if base.TestUseXattrs() {
		assert.Equal(t, int64(len(doc1Contents)), collection1Stats.DocWritesBytes.Value()) // xattr writes size should exactly match doc contents
	} else {
		assert.Greater(t, collection1Stats.DocWritesBytes.Value(), int64(len(doc1Contents))) // non-xattr writes have sync data size included
	}
	assert.Equal(t, int64(1), collection1Stats.SyncFunctionCount.Value())
	assert.GreaterOrEqual(t, collection1Stats.SyncFunctionTime.Value(), int64(0))
	assert.Equal(t, int64(0), collection1Stats.NumDocReads.Value())
	assert.Equal(t, int64(0), collection1Stats.DocReadsBytes.Value())

	response = rt.SendAdminRequest("GET", "/{{.keyspace1}}/doc1", ``)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, int64(1), collection1Stats.NumDocReads.Value())
	assert.Equal(t, int64(len(doc1Contents)), collection1Stats.DocReadsBytes.Value())
	assert.Equal(t, int64(1), collection1Stats.SyncFunctionCount.Value())

	// runtime error
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/doc2", `{"throwException":true}`)
	assert.Equal(t, http.StatusInternalServerError, response.Code)
	assert.Contains(t, response.Body.String(), "Exception in JS sync function")
	assert.Equal(t, int64(2), collection1Stats.SyncFunctionCount.Value())
	assert.Equal(t, int64(1), collection1Stats.SyncFunctionExceptionCount.Value())

	// require methods shouldn't cause a true exception
	response = rt.SendRequest("PUT", "/{{.keyspace1}}/doc3", `{"require":true}`)
	assert.Equal(t, http.StatusForbidden, response.Code)
	assert.Contains(t, response.Body.String(), "sg admin required")
	assert.Equal(t, int64(3), collection1Stats.SyncFunctionCount.Value())
	assert.Equal(t, int64(1), collection1Stats.SyncFunctionExceptionCount.Value())
	assert.Equal(t, int64(1), collection1Stats.SyncFunctionRejectCount.Value())

	// we've not done anything to collection 2 yet, so still expect zero everything
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionCount.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionTime.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionRejectCount.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionRejectAccessCount.Value())
	assert.Equal(t, int64(0), collection2Stats.SyncFunctionExceptionCount.Value())
	assert.Equal(t, int64(0), collection2Stats.ImportCount.Value())
	assert.Equal(t, int64(0), collection2Stats.NumDocReads.Value())
	assert.Equal(t, int64(0), collection2Stats.DocReadsBytes.Value())
	assert.Equal(t, int64(0), collection2Stats.NumDocWrites.Value())
	assert.Equal(t, int64(0), collection2Stats.DocWritesBytes.Value())

	// but make sure the 2nd collection stats are indeed wired up correctly... we don't need to be too comprehensive here given above coverage.
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/doc1", doc1Contents)
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Equal(t, int64(1), collection2Stats.NumDocWrites.Value())

	// write a doc to the bucket and have it imported and check stat
	if base.TestUseXattrs() {
		dbc, err := rt.GetDatabase().GetDatabaseCollection(scope2Name, collection2Name)
		require.NoError(t, err)
		ok, err := dbc.GetCollectionDatastore().AddRaw("importeddoc", 0, []byte(`{"imported":true}`))
		require.NoError(t, err)
		assert.True(t, ok)
		base.RequireWaitForStat(t, collection2Stats.ImportCount.Value, 1)
		assert.Equal(t, int64(2), collection2Stats.NumDocWrites.Value())
	}
}
