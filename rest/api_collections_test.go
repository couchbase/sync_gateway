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
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectionsPutDocInKeyspace creates a collection and starts up a RestTester instance on it.
// Ensures that various keyspaces can or can't be used to insert a doc in the collection.
func TestCollectionsPutDocInKeyspace(t *testing.T) {
	base.TestRequiresCollections(t)
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
		name           string
		keyspace       string
		expectedStatus int
	}{
		// if a single scope and collection is defined, use that implicitly
		/*{
			name:           "implicit scope and collection",
			keyspace:       "db",
			expectedStatus: http.StatusNotFound,
		},
		*/
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
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			docID := fmt.Sprintf("doc%d", i)
			path := fmt.Sprintf("/%s/%s", test.keyspace, docID)
			resp := rt.SendUserRequestWithHeaders(http.MethodPut, path, `{"test":true}`, nil, username, password)
			RequireStatus(t, resp, test.expectedStatus)

			if test.expectedStatus == http.StatusCreated {
				// go and check that the doc didn't just end up in the default collection of the test bucket
				docBody, _, err := ds.GetRaw(docID)
				assert.NoError(t, err)
				assert.NotNil(t, docBody)

				defaultDataStore := rt.Bucket().DefaultDataStore()
				_, err = defaultDataStore.Get(docID, &gocb.GetOptions{})
				assert.Error(t, err)
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
	tb := base.GetTestBucket(t)
	defer tb.Close()

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

	if !base.TestUseXattrs() {
		t.Skip("Test relies on import - needs xattrs")
	}

	t.Skip("Skip until CBG-2266 is implemented")
	tb := base.GetTestBucket(t)
	defer tb.Close()

	ctx := base.TestCtx(t)
	err := base.CreateBucketScopesAndCollections(ctx, tb.BucketSpec, map[string][]string{
		"foo": {
			"bar",
			"baz",
		},
	})
	require.NoError(t, err)
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
				Scopes: ScopesConfig{
					"foo": ScopeConfig{
						Collections: map[string]CollectionConfig{
							"bar": {},
							"baz": {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	underlying, ok := rt.Bucket().DefaultDataStore().(*base.Collection)
	require.True(t, ok, "rt bucket was not a Collection")

	_, err = underlying.Collection.Bucket().Scope("foo").Collection("bar").Insert("testDocBar", map[string]any{"test": true}, nil)
	require.NoError(t, err)
	_, err = underlying.Collection.Bucket().Scope("foo").Collection("baz").Insert("testDocBaz", map[string]any{"test": true}, nil)
	require.NoError(t, err)

	// ensure the doc is picked up by the import DCP feed and actually gets imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	require.NoError(t, err)

	// TODO(CBG-2329): collection-aware caching
	// require.NoError(t, rt.WaitForDoc(docID))
}

func TestMultiCollectionChannelAccess(t *testing.T) {
	base.TestRequiresCollections(t)
	//if base.UnitTestUrlIsWalrus() {
	//	t.Skip("This test only works against Couchbase Server")
	//}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	scopesConfig := GetCollectionsConfig(t, tb, 3)
	dataStoreNames := GetDataStoreNamesFromScopesConfig(scopesConfig)
	c1SyncFunction := `function(doc) {channel(doc.chan);}`

	scope := dataStoreNames[0].ScopeName()
	collection1 := dataStoreNames[0].CollectionName()
	collection2 := dataStoreNames[1].CollectionName()
	collection3 := dataStoreNames[2].CollectionName()

	scopesConfig[scope].Collections[collection1] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[scope].Collections[collection2] = CollectionConfig{SyncFn: &c1SyncFunction}

	rtConfig := &RestTesterConfig{
		CustomTestBucket: tb,
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
	resp = rt.SendAdminRequest("GET", "/db/_user/userA", ``)
	fmt.Println(resp.Body)
	RequireStatus(t, resp, http.StatusOK)

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
	scopesConfig[scope].Collections[collection3] = CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfigString, err := json.Marshal(scopesConfig)
	require.NoError(t, err)
	resp = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		tb.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	RequireStatus(t, resp, http.StatusCreated)

	// Put a doc in new collection and make sure it cant be accessed
	resp = rt.SendAdminRequest("GET", "/{{.keyspace2}}/testDocBazB", ``)
	fmt.Println(resp.Body)
	RequireStatus(t, resp, http.StatusOK)
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
	require.NoError(t, n1qlStore.CreatePrimaryIndex(idxName, nil))
	require.NoError(t, n1qlStore.WaitForIndexesOnline([]string{idxName}, false))

	res, err := n1qlStore.Query("SELECT keyspace_id, bucket_id, scope_id from system:indexes WHERE name = $idxName",
		map[string]interface{}{"idxName": idxName}, base.RequestPlus, true)
	require.NoError(t, err)

	var indexMetaResult struct {
		BucketID   *string `json:"bucket_id"`
		ScopeID    *string `json:"scope_id"`
		KeyspaceID *string `json:"keyspace_id"`
	}
	require.NoError(t, res.One(&indexMetaResult))
	require.NotNil(t, indexMetaResult)

	// if the index was created on the _default collection in the bucket, keyspace_id is the bucket name, and the other fields are not present.
	assert.NotNilf(t, indexMetaResult.BucketID, "bucket_id was not present - index was created on the _default collection!")
	assert.NotNilf(t, indexMetaResult.ScopeID, "scope_id was not present - index was created on the _default collection!")
	require.NotNilf(t, indexMetaResult.KeyspaceID, "keyspace_id should be present")
	assert.NotEqualf(t, rt.Bucket().GetName(), *indexMetaResult.KeyspaceID, "keyspace_id was the bucket name - index was created on the _default collection!")

	// if the index was created on a collection, the keyspace_id becomes the collection, along with additional fields for bucket and scope.
	assert.Equal(t, rt.Bucket().GetName(), *indexMetaResult.BucketID)
	assert.Equal(t, collection.ScopeName(), *indexMetaResult.ScopeID)
	assert.Equal(t, collection.Name(), *indexMetaResult.KeyspaceID)

	// try and query the document that we wrote via SG
	res, err = n1qlStore.Query("SELECT test FROM "+base.KeyspaceQueryToken+" WHERE test = true", nil, base.RequestPlus, true)
	require.NoError(t, err)

	var primaryQueryResult struct {
		Test *bool `json:"test"`
	}
	require.NoError(t, res.One(&primaryQueryResult))
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

func TestCollectionsChangeConfigScope(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("can not create new buckets and scopes in walrus")
	}

	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close()
	ctx := base.TestCtx(t)

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
	require.NoError(t, sc.WaitForRESTAPIs())

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
