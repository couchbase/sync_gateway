// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !race

package importtest

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

// AssertLogContains can hit the race detector due to swapping the global loggers

func TestImportFilterLogging(t *testing.T) {
	const errorMessage = `ImportFilterError`
	importFilter := `function (doc) { console.error("` + errorMessage + `"); return doc.type == "mobile"; }`
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportFilter: &importFilter,
			AutoImport:   false,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // use default collection since we are using default sync function
	defer rt.Close()

	// Add document to bucket
	key := "ValidImport"
	body := make(map[string]interface{})
	body["type"] = "mobile"
	body["channels"] = "A"
	ok, err := rt.GetSingleDataStore().Add(key, 0, body)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Get number of errors before
	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	// Attempt to get doc will trigger import
	base.AssertLogContains(t, errorMessage, func() {
		response := rt.SendAdminRequest("GET", "/db/"+key, "")
		assert.Equal(t, http.StatusOK, response.Code)
	})

	// Get number of errors after
	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	// Make sure at least one error was logged
	assert.GreaterOrEqual(t, numErrors+1, numErrorsAfter)

}
