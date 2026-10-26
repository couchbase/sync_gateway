// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importtest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

func TestImportFilterLogging(t *testing.T) {
	ctx := base.TestCtx(t)
	const errorMessage = `ImportFilterError`
	importFilter := `function (doc) { console.error("` + errorMessage + `"); return doc.type == "mobile"; }`
	dbName := sgtest.SafeDatabaseName(t.Name())
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			Name:         dbName,
			ImportFilter: &importFilter,
			AutoImport:   false,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // use default collection since we are using default sync function
	defer rt.Close()

	// Add document to bucket
	key := "ValidImport"
	body := make(map[string]any)
	body["type"] = "mobile"
	body["channels"] = "A"
	ok, err := rt.GetSingleDataStore().Add(ctx, key, 0, body)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Attempt to get doc will trigger import
	base.AssertErrorCount(t, "db:"+dbName, 1, func() {
		base.AssertLogContains(t, errorMessage, func() {
			response := rt.SendAdminRequest("GET", "/"+dbName+"/"+key, "")
			assert.Equal(t, http.StatusOK, response.Code)
		})
	})
}
