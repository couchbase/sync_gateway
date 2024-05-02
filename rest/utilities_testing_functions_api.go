// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// NewRestTesterForUserQueries creates a new RestTester using persistent config, and a database "db".
// Only the user-query-related fields are copied from `queryConfig`; the rest are ignored.
func NewRestTesterForUserQueries(t *testing.T, queryConfig DbConfig) *RestTester {
	if base.TestsDisableGSI() {
		t.Skip("These tests use N1QL functions which requires GSI")
	}
	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{
		EnableUserQueries: true,
		PersistentConfig:  true,
	})

	dbConfig := rt.NewDbConfig()

	dbConfig.UserFunctions = queryConfig.UserFunctions

	resp := rt.CreateDatabase("db", dbConfig)
	if !AssertStatus(t, resp, 201) {
		rt.Close()
		t.FailNow()
		return nil // (never reached)
	}
	return rt
}
