// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importuserxattrtest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

func TestUserXattrSetUnsetDBConfig(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UserXattrKey = base.StringPtr("myXattr")
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.AssertStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), `"user_xattr_key":"myXattr"`)

	// upsert an empty string to ensure we can remove the config option
	resp = rt.UpsertDbConfig(rt.GetDatabase().Name, rest.DbConfig{UserXattrKey: base.StringPtr("")})
	rest.AssertStatus(t, resp, http.StatusCreated)

	// empty string for the config option will be treated like nil in use
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.AssertStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), `"user_xattr_key":""`)

	// PUT to fully remove the config option (nil)
	dbConfig.UserXattrKey = nil
	resp = rt.ReplaceDbConfig(rt.GetDatabase().Name, dbConfig)
	rest.AssertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.AssertStatus(t, resp, http.StatusOK)
	assert.NotContains(t, string(resp.BodyBytes()), `"user_xattr_key"`)
}
