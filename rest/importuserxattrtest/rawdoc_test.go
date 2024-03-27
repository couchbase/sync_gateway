// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importuserxattrtest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserXattrsRawGet(t *testing.T) {
	docKey := t.Name()
	xattrKey := "xattrKey"

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				AutoImport:   true,
				UserXattrKey: &xattrKey,
			},
		},
	})
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())

	cas, err := rt.GetSingleDataStore().Get(docKey, nil)
	require.NoError(t, err)

	ctx := rt.Context()
	_, err = rt.GetSingleDataStore().UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, "val")}, nil)
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value() == 1
	})
	require.NoError(t, err)

	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var RawReturn struct {
		Meta struct {
			Xattrs map[string]interface{} `json:"xattrs"`
		} `json:"_meta"`
	}

	err = json.Unmarshal(resp.BodyBytes(), &RawReturn)
	require.NoError(t, err)

	assert.Equal(t, "val", RawReturn.Meta.Xattrs[xattrKey])
}
