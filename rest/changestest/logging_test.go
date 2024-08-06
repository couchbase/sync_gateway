// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package changestest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

// TestDocChangedLogging exercises some of the logging in DocChanged
func TestDocChangedLogging(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyCache, base.KeyChanges)

	rt := rest.NewRestTesterMultipleCollections(t, nil, 2)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/doc1", `{"foo":"bar"}`)
	rest.RequireStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/doc1", `{"foo":"bar"}`)
	rest.RequireStatus(t, response, http.StatusCreated)
	rt.WaitForPendingChanges()

	base.AssertLogContains(t, "Ignoring non-metadata mutation for doc", func() {
		err := rt.GetDatabase().MetadataStore.Set("doc1", 0, nil, db.Body{"foo": "bar"})
		require.NoError(t, err)
		// write another doc to ensure the previous non-metadata doc has been seen...
		// no other way of synchronising this no-op as no stats to wait on
		response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/doc2", `{"foo":"bar"}`)
		rest.RequireStatus(t, response, http.StatusCreated)
		rt.WaitForPendingChanges()
	})
}
