// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

const (
	revocationTestRole     = "foo"
	revocationTestUser     = "user"
	revocationTestPassword = "test"
)

func InitScenario(t *testing.T, rtConfig *RestTesterConfig) (ChannelRevocationTester, *RestTester) {

	defaultSyncFn := `
			function (doc, oldDoc){
				if (doc._id === 'userRoles'){
					for (var key in doc.roles){
						role(key, doc.roles[key]);
					}
				}
				if (doc._id === 'roleChannels'){
					for (var key in doc.channels){
						access(key, doc.channels[key]);
					}
				}
				if (doc._id === 'userChannels'){
					for (var key in doc.channels){
						access(key, doc.channels[key]);
					}
				}
				if (doc._id.indexOf("doc") >= 0){
					channel(doc.channels);
				}
			}`

	if rtConfig == nil {
		rtConfig = &RestTesterConfig{
			SyncFn: defaultSyncFn,
		}
	} else if rtConfig.SyncFn == "" {
		rtConfig.SyncFn = defaultSyncFn
	}

	rt := NewRestTester(t, rtConfig)

	revocationTester := ChannelRevocationTester{
		test:       t,
		restTester: rt,
	}

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", fmt.Sprintf(`{"name": "%s", "password": "%s"}`, revocationTestUser, revocationTestPassword))
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/_role/foo", `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	return revocationTester, rt
}

func TestRevocationsWithQueryLimit2Channels(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:           false,
			QueryPaginationLimit: base.IntPtr(2),
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.addUserChannel("user", "ch2")

	revocationTester.fillToSeq(9)
	_ = rt.createDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1", "ch2"}})
	_ = rt.createDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch1", "ch2"}})
	_ = rt.createDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"ch1", "ch2"}})

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	_ = changes.Last_Seq
	// This changes feed would loop if CBG-3273 was still an issue
	changes = revocationTester.getChanges(0, 4)

	// Get one of the 3 docs which the user should still have access to
	resp := rt.SendUserRequestWithHeaders("GET", "/db/doc1", "", nil, "user", "test")
	RequireStatus(t, resp, 200)

	// Revoke access to ch2
	revocationTester.removeUserChannel("user", "ch2")
	changes = revocationTester.getChanges(0, 7)
	// Get a doc to ensure no access
	resp = rt.SendUserRequestWithHeaders("GET", "/db/doc1", "", nil, "user", "test")
	RequireStatus(t, resp, 403)
}
