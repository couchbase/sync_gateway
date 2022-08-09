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
	"testing"

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
		scopeName      = "foo"
		collectionName = "bar"
	)

	tests := []struct {
		name           string
		keyspace       string
		expectedStatus int
	}{
		{
			name:           "implicit scope and collection",
			keyspace:       "db",
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "fully qualified",
			keyspace:       fmt.Sprintf("%s.%s.%s", "db", scopeName, collectionName),
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "collection only",
			keyspace:       fmt.Sprintf("%s.%s", "db", collectionName),
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "invalid collection",
			keyspace:       fmt.Sprintf("%s.%s.%s", "db", scopeName, "buzz"),
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "invalid scope",
			keyspace:       fmt.Sprintf("%s.%s.%s", "db", "buzz", collectionName),
			expectedStatus: http.StatusNotFound,
		},
	}

	tb := base.GetTestBucket(t)
	defer tb.Close()

	const (
		username = "alice"
		password = "pass"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		createScopesAndCollections: true,
		TestBucket:                 tb.NoCloseClone(), // Clone so scope/collection isn't set on tb from rt
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					username: {Password: base.StringPtr(password)},
				},
				Scopes: ScopesConfig{
					scopeName: ScopeConfig{
						Collections: map[string]CollectionConfig{
							collectionName: {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			docID := fmt.Sprintf("doc%d", i)
			path := fmt.Sprintf("/%s/%s", test.keyspace, docID)
			resp := rt.SendUserRequestWithHeaders(http.MethodPut, path, `{"test":true}`, nil, username, password)
			requireStatus(t, resp, test.expectedStatus)

			if test.expectedStatus == http.StatusCreated {
				// go and check that the doc didn't just end up in the default collection of the test bucket
				docBody, _, err := tb.GetRaw(docID)
				assert.Truef(t, base.IsDocNotFoundError(err), "didn't expect doc %q to be in the default collection but got body:%s err:%v", docID, docBody, err)
			}
		})
	}
}

func TestCollectionsDCP(t *testing.T) {
	base.TestRequiresCollections(t)
	if base.TestUseXattrs() {
		t.Skip("This test does not work with XATTRs due to using DCP import feed")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyDCP, base.KeyImport)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt := NewRestTester(t, &RestTesterConfig{
		createScopesAndCollections: true,
		TestBucket:                 tb.NoCloseClone(), // Clone so scope/collection isn't set on tb from rt
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Scopes: ScopesConfig{
					"foo": ScopeConfig{
						Collections: map[string]CollectionConfig{
							"bar": {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	const docID = "doc1"

	ok, err := rt.Bucket().AddRaw(docID, 0, []byte(`{"test":true}`))
	require.NoError(t, err)
	require.True(t, ok)

	// ensure the doc is picked up by the import DCP feed and actually gets imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	require.NoError(t, err)

	// ensure the doc comes back over the caching feed after import
	t.Log("Not performing caching feed check - CBG-1143")
	//assert.NoError(t, rt.WaitForDoc(docID))
}

// TestCollectionsBasicIndexQuery ensures that the bucket API is able to create an index on a collection
// and query documents written to the collection.
func TestCollectionsBasicIndexQuery(t *testing.T) {
	base.TestRequiresCollections(t)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt := NewRestTester(t, &RestTesterConfig{
		createScopesAndCollections: true,
		TestBucket:                 tb.NoCloseClone(), // Clone so scope/collection isn't set on tb from rt
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Scopes: ScopesConfig{
					"foo": ScopeConfig{
						Collections: map[string]CollectionConfig{
							"bar": {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	const (
		scope      = "foo"
		collection = "bar"
		keyspace   = "db." + scope + "." + collection
		docID      = "doc1"
	)

	resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, docID), `{"test":true}`)
	requireStatus(t, resp, http.StatusCreated)

	// use the rt.Bucket which has got the foo.bar scope/collection set up
	n1qlStore, ok := base.AsN1QLStore(rt.Bucket())
	require.True(t, ok)

	idxName := t.Name() + "_primary"
	require.NoError(t, n1qlStore.CreatePrimaryIndex(idxName, nil))
	require.NoError(t, n1qlStore.WaitForIndexOnline(idxName))

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
	assert.NotEqualf(t, tb.Bucket.GetName(), *indexMetaResult.KeyspaceID, "keyspace_id was the bucket name - index was created on the _default collection!")

	// if the index was created on a collection, the keyspace_id becomes the collection, along with additional fields for bucket and scope.
	assert.Equal(t, tb.Bucket.GetName(), *indexMetaResult.BucketID)
	assert.Equal(t, scope, *indexMetaResult.ScopeID)
	assert.Equal(t, collection, *indexMetaResult.KeyspaceID)

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

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeyQuery, base.KeyCRUD)

	// force GSI for this one test
	useViews := base.BoolPtr(false)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	const (
		username       = "alice"
		password       = "letmein"
		validChannel   = "valid"
		invalidChannel = "invalid"

		scope      = "foo"
		collection = "bar"
		keyspace   = "db." + scope + "." + collection

		validDocID   = "doc1"
		invalidDocID = "doc2"
	)

	rt := NewRestTester(t, &RestTesterConfig{
		createScopesAndCollections: true,
		TestBucket:                 tb.NoCloseClone(), // Clone so scope/collection isn't set on tb from rt
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
					scope: ScopeConfig{
						Collections: map[string]CollectionConfig{
							collection: {},
						},
					},
				},
			},
		},
	})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, validDocID), `{"test": true, "channels": ["`+validChannel+`"]}`)
	requireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, invalidDocID), `{"test": true, "channels": ["`+invalidChannel+`"]}`)
	requireStatus(t, resp, http.StatusCreated)

	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/_all_docs", ``, nil, username, password)
	requireStatus(t, resp, http.StatusOK)
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
	requireStatus(t, resp, http.StatusOK)
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, invalidDocID), ``, nil, username, password)
	requireStatus(t, resp, http.StatusForbidden)

	_, err := rt.waitForChanges(1, "/db/_changes", username, false)
	assert.NoError(t, err)
}
