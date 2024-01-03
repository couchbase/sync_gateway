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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			UserXattrKey: base.StringPtr("myXattr"),
		}},
	})
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.AssertStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), `"user_xattr_key":"myXattr"`)

	// upsert an explicit empty string (instead of nil) to ensure we can remove the config option
	resp = rt.UpsertDbConfig(rt.GetDatabase().Name, rest.DbConfig{UserXattrKey: base.StringPtr("")})
	rest.AssertStatus(t, resp, http.StatusCreated)

	// empty string for the config option will be treated like nil in use
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.AssertStatus(t, resp, http.StatusOK)
	assert.Contains(t, string(resp.BodyBytes()), `"user_xattr_key":""`)

	// PUT to fully remove the config option (nil)
	dbConfig := rt.DatabaseConfig
	dbConfig.UserXattrKey = nil
	resp = rt.ReplaceDbConfig(rt.GetDatabase().Name, dbConfig.DbConfig)
	rest.AssertStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	rest.AssertStatus(t, resp, http.StatusOK)
	assert.NotContains(t, string(resp.BodyBytes()), `"user_xattr_key"`)
}
