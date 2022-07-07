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
)

// TestCollectionsPutDocInKeyspace creates a collection and starts up a RestTester instance on it.
// Ensures that various keyspaces can be used to insert a doc in the collection.
func TestCollectionsPutDocInKeyspace(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support scopes and collections")
	}

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
