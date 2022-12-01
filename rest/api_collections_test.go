//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
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
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rest errors are slightly different with walrus than CBS")
	}
	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	// dbName is the default name from RestTester
	dbName := "db"

	ds := tb.GetNamedDataStore()
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
			keyspace:       strings.Join([]string{dbName, dataStoreName.ScopeName(), dataStoreName.CollectionName()}, base.ScopeCollectionSeparator),
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

	const (
		username = "alice"
		password = "pass"
	)
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			rt := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: tb.NoCloseClone(),
				DatabaseConfig: &DatabaseConfig{
					DbConfig: DbConfig{
						Users: map[string]*auth.PrincipalConfig{
							username: {Password: base.StringPtr(password)},
						},
						Scopes: ScopesConfig{
							dataStoreName.ScopeName(): ScopeConfig{
								Collections: map[string]CollectionConfig{
									dataStoreName.CollectionName(): {},
								},
							},
						},
					},
				},
			})
			defer rt.Close()

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

// TestNoCollectionsPutDocWithKeyspace ensures that a keyspace can't be used to insert a doc on a database not configured for collections.
func TestNoCollectionsPutDocWithKeyspace(t *testing.T) {
	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb,
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

	tb := base.GetTestBucket(t)
	defer tb.Close()

	tc, err := base.AsCollection(tb.GetSingleDataStore())
	require.NoError(t, err)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport: true,
				Scopes: ScopesConfig{
					tc.ScopeName(): ScopeConfig{
						Collections: map[string]CollectionConfig{
							tc.CollectionName(): {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	const docID = "doc1"

	ok, err := rt.Bucket().DefaultDataStore().AddRaw(docID, 0, []byte(`{"test":true}`))
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

// TestCollectionsBasicIndexQuery ensures that the bucket API is able to create an index on a collection
// and query documents written to the collection.
func TestCollectionsBasicIndexQuery(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test requires N1QL")
	}

	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	ds := tb.GetNamedDataStore()
	dataStoreName, ok := base.AsDataStoreName(ds)
	require.True(t, ok)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Scopes: ScopesConfig{
					dataStoreName.ScopeName(): ScopeConfig{
						Collections: map[string]CollectionConfig{
							dataStoreName.CollectionName(): {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	const docID = "doc1"

	keyspace := strings.Join([]string{"db", dataStoreName.ScopeName(), dataStoreName.CollectionName()}, base.ScopeCollectionSeparator)

	resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, docID), `{"test":true}`)
	RequireStatus(t, resp, http.StatusCreated)

	// use the rt.Bucket which has got the foo.bar scope/collection set up
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
	assert.Equal(t, dataStoreName.ScopeName(), *indexMetaResult.ScopeID)
	assert.Equal(t, dataStoreName.CollectionName(), *indexMetaResult.KeyspaceID)

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

	tb := base.GetTestBucket(t)
	defer tb.Close()

	ds := tb.GetNamedDataStore()

	dataStoreName, ok := base.AsDataStoreName(ds)
	require.True(t, ok)

	keyspace := strings.Join([]string{"db", dataStoreName.ScopeName(), dataStoreName.CollectionName()}, base.ScopeCollectionSeparator)

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
				Scopes: ScopesConfig{
					dataStoreName.ScopeName(): ScopeConfig{
						Collections: map[string]CollectionConfig{
							dataStoreName.CollectionName(): {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, validDocID), `{"test": true, "channels": ["`+validChannel+`"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, invalidDocID), `{"test": true, "channels": ["`+invalidChannel+`"]}`)
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

	resp = rt.SendUserRequestWithHeaders(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, validDocID), ``, nil, username, password)
	RequireStatus(t, resp, http.StatusOK)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, invalidDocID), ``, nil, username, password)
	RequireStatus(t, resp, http.StatusForbidden)

	_, err := rt.WaitForChanges(1, "/db/_changes", username, false)
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
	err := base.CreateBucketScopesAndCollections(ctx, tb.BucketSpec, map[string][]string{
		"fooScope": {
			"bar",
		},
		"quxScope": {
			"quux",
		},
	})
	require.NoError(t, err)

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
