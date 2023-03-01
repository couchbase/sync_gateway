// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRolePurge(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	// Create role
	resp := rt.SendAdminRequest("PUT", "/db/_role/role", GetRolePayload(t, "", "", collection, []string{"channel"}))
	RequireStatus(t, resp, http.StatusCreated)

	// Delete role
	resp = rt.SendAdminRequest("DELETE", "/db/_role/role", ``)
	RequireStatus(t, resp, http.StatusOK)

	// Ensure role is gone
	resp = rt.SendAdminRequest("GET", "/db/_role/role", ``)
	RequireStatus(t, resp, http.StatusNotFound)

	// Ensure role is 'soft-deleted' and we can still get the doc
	role, err := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetRoleIncDeleted("role")
	assert.NoError(t, err)
	assert.NotNil(t, role)

	// Re-create role
	resp = rt.SendAdminRequest("PUT", "/db/_role/role", GetRolePayload(t, "", "", collection, []string{"channel"}))
	RequireStatus(t, resp, http.StatusCreated)

	// Delete role again but with purge flag
	resp = rt.SendAdminRequest("DELETE", "/db/_role/role?purge=true", ``)
	RequireStatus(t, resp, http.StatusOK)

	// Ensure role is purged, can't access at all
	role, err = rt.GetDatabase().Authenticator(base.TestCtx(t)).GetRoleIncDeleted("role")
	assert.Nil(t, err)
	assert.Nil(t, role)

	// Ensure role returns 404 via REST call
	resp = rt.SendAdminRequest("GET", "/db/_role/role", ``)
	RequireStatus(t, resp, http.StatusNotFound)
}
func TestRoleAPI(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	// PUT a role
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_role/hipster", GetRolePayload(t, "", "", collection, []string{"fedoras", "fixies"}))
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_role/testdeleted", GetRolePayload(t, "", "", collection, []string{"fedoras", "fixies"}))
	RequireStatus(t, response, 201)
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/testdeleted", ""), 200)

	// GET the role and make sure the result is OK
	response = rt.SendAdminRequest("GET", "/db/_role/hipster", "")
	RequireStatus(t, response, 200)
	role := auth.PrincipalConfig{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &role))
	assert.Equal(t, "hipster", *role.Name)
	var expPassword *string
	assert.Equal(t, expPassword, role.Password)

	response = rt.SendAdminRequest("GET", "/db/_role/", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, `["hipster"]`, response.Body.String())

	// DELETE the role
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)

	// POST a role
	response = rt.SendAdminRequest("POST", "/db/_role", GetRolePayload(t, "hipster", "", collection, []string{"fedoras", "fixies"}))
	RequireStatus(t, response, 301)
	response = rt.SendAdminRequest("POST", "/db/_role/", GetRolePayload(t, "hipster", "", collection, []string{"fedoras", "fixies"}))
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/hipster", "")
	RequireStatus(t, response, 200)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &role))
	assert.Equal(t, "hipster", *role.Name)
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_role/hipster", ""), 200)

	// GET including deleted
	response = rt.SendAdminRequest("GET", "/db/_role/?deleted=true", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, `["hipster","testdeleted"]`, response.Body.String())
}
func TestFunkyRoleNames(t *testing.T) {
	cases := []struct {
		Name     string
		RoleName string
	}{
		{
			Name:     "hashes",
			RoleName: "foo#bar",
		},
		{
			Name:     "question mark",
			RoleName: "foo?bar",
		},
		{
			Name:     "dollars",
			RoleName: "$foo$bar",
		},
		{
			Name:     "underscore prefix",
			RoleName: "_sync-foobar",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			require.Truef(t, auth.IsValidPrincipalName(tc.RoleName), "expected '%s' to be accepted", tc.RoleName)
			roleNameJSON, err := json.Marshal("role:" + tc.RoleName)
			require.NoError(t, err)
			const username = "user1"
			syncFn := fmt.Sprintf(`function(doc) {channel(doc.channels); role("%s", %s);}`, username, string(roleNameJSON))
			rt := NewRestTester(t,
				&RestTesterConfig{
					SyncFn: syncFn,
				})
			defer rt.Close()
			collection := rt.GetSingleTestDatabaseCollection()

			ctx := rt.Context()
			a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

			// Create a test user
			user, err := a.NewUser(username, "letmein", channels.BaseSetOf(t))
			require.NoError(t, err)
			require.NoError(t, a.Save(user))

			// Create role
			response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_role/%s", url.PathEscape(tc.RoleName)), GetRolePayload(t, "", "", collection, []string{"testchannel"}))
			RequireStatus(t, response, 201)

			// Create test document
			response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc", `{"channels":["testchannel"]}`)
			RequireStatus(t, response, 201)

			// Assert user can access it
			response = rt.SendUserRequest("GET", "/{{.keyspace}}/testdoc", "", username)
			RequireStatus(t, response, 200)
		})
	}
}
func TestBulkDocsChangeToRoleAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `
		function(doc) {
			if(doc.type == "roleaccess") {
				channel(doc.channel);
				access(doc.grantTo, doc.grantChannel);
			} else {
				requireAccess(doc.mustHaveAccess)
			}
		}`,
		DatabaseConfig: &DatabaseConfig{}, // revocation requires collection specific channel access
	}
	rt := NewRestTester(t,
		&rtConfig)
	defer rt.Close()
	ctx := rt.Context()

	// Create a role with no channels assigned to it
	authenticator := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	role, err := authenticator.NewRole("role1", nil)
	require.NoError(t, err)
	assert.NoError(t, authenticator.Save(role))

	// Create a user with an explicit role grant for role1
	user, err := authenticator.NewUser("user1", "letmein", nil)
	require.NoError(t, err)
	user.SetExplicitRoles(channels.TimedSet{"role1": channels.NewVbSimpleSequence(1)}, 1)
	err = authenticator.Save(user)
	assert.NoError(t, err)

	// Bulk docs with 2 docs.  First doc grants role1 access to chan1.  Second requires chan1 for write.
	input := `{"docs": [
				{
					"_id": "bulk1",
				 	"type" : "roleaccess",
				 	"grantTo":"role:role1",
				 	"grantChannel":"chan1"
				 },
				{
					"_id": "bulk2",
					"mustHaveAccess":"chan1"
				}
				]}`

	response := rt.SendUserRequest("POST", "/{{.keyspace}}/_bulk_docs", input, "user1")
	RequireStatus(t, response, 201)

	var docs []interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 2, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "1-17424d2a21bf113768dfdbcd344741ac", "id": "bulk1"}, docs[0])

	assert.Equal(t, map[string]interface{}{"rev": "1-f120ccb33c0a6ef43ef202ade28f98ef", "id": "bulk2"}, docs[1])

}
func TestRoleAssignmentBeforeUserExists(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`,
		DatabaseConfig: &DatabaseConfig{}, // revocation requires collection specific channel access
	}
	rt := NewRestTester(t,
		&rtConfig)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()
	c := collection.Name
	s := collection.ScopeName

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// POST a role
	response := rt.SendAdminRequest("POST", "/db/_role/", GetRolePayload(t, "role1", "", collection, []string{"chan1"}))
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_role/role1", "")
	RequireStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "role1", body["name"])

	// Put document to trigger sync function
	response = rt.SendRequest("PUT", "/{{.keyspace}}/doc1", `{"user":"user1", "role":"role:role1", "channel":"chan1"}`) // seq=1
	RequireStatus(t, response, 201)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])

	// POST the new user the GET and verify that it shows the assigned role
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"letmein"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/user1", "")
	RequireStatus(t, response, 200)
	user := auth.PrincipalConfig{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &user))
	assert.Equal(t, "user1", *user.Name)
	assert.Equal(t, []string{"role1"}, user.RoleNames)
	allChans := user.GetChannels(s, c).ToArray()
	sort.Strings(allChans)
	assert.Equal(t, []string{"!", "chan1"}, allChans)

	// goassert.DeepEquals(t, body["admin_roles"], []interface{}{"hipster"})
	// goassert.DeepEquals(t, body["all_channels"], []interface{}{"bar", "fedoras", "fixies", "foo"})
}
func TestRoleAccessChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {role(doc.user, doc.role);channel(doc.channel)}`}
	rt := NewRestTester(t,
		&rtConfig)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()
	c := collection.Name
	s := collection.ScopeName

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	require.NoError(t, err)

	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create users:
	response := rt.SendAdminRequest("PUT", "/db/_user/alice", GetUserPayload(t, "alice", "letmein", "", collection, []string{"alpha"}, nil))
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_user/zegpold", GetUserPayload(t, "zegpold", "letmein", "", collection, []string{"beta"}, nil))
	RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_role/hipster", GetRolePayload(t, "hipster", "", collection, []string{"gamma"}))
	RequireStatus(t, response, 201)
	/*
		alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "alpha"))
		assert.NoError(t, a.Save(alice))
		zegpold, err := a.NewUser("zegpold", "letmein", channels.BaseSetOf(t, "beta"))
		assert.NoError(t, a.Save(zegpold))

		hipster, err := a.NewRole("hipster", channels.BaseSetOf(t, "gamma"))
		assert.NoError(t, a.Save(hipster))
	*/

	// Create some docs in the channels:
	cacheWaiter := rt.ServerContext().Database(ctx, "db").NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(1)
	response = rt.SendRequest("PUT", "/{{.keyspace}}/fashion", `{"user":"alice","role":["role:hipster","role:bogus"]}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	fashionRevID := body["rev"].(string)

	roleGrantSequence := rt.GetDocumentSequence("fashion")

	cacheWaiter.Add(4)
	RequireStatus(t, rt.SendRequest("PUT", "/{{.keyspace}}/g1", `{"channel":"gamma"}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/{{.keyspace}}/a1", `{"channel":"alpha"}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/{{.keyspace}}/b1", `{"channel":"beta"}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/{{.keyspace}}/d1", `{"channel":"delta"}`), 201)

	// Check user access:
	alice, _ := a.GetUser("alice")
	assert.Equal(t,
		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(1),
			"alpha": channels.NewVbSimpleSequence(alice.Sequence()),
			"gamma": channels.NewVbSimpleSequence(roleGrantSequence),
		}, alice.InheritedCollectionChannels(s, c))

	assert.Equal(t,
		channels.TimedSet{
			"bogus":   channels.NewVbSimpleSequence(roleGrantSequence),
			"hipster": channels.NewVbSimpleSequence(roleGrantSequence),
		}, alice.RoleNames())

	zegpold, _ := a.GetUser("zegpold")
	assert.Equal(t,

		channels.TimedSet{
			"!":    channels.NewVbSimpleSequence(1),
			"beta": channels.NewVbSimpleSequence(zegpold.Sequence()),
		}, zegpold.InheritedCollectionChannels(s, c))

	assert.Equal(t, channels.TimedSet{}, zegpold.RoleNames())

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	cacheWaiter.Wait()
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes", "", "alice")
	log.Printf("1st _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 3, len(changes.Results))
	assert.Equal(t, "_user/alice", changes.Results[0].ID)
	assert.Equal(t, "g1", changes.Results[1].ID)
	assert.Equal(t, "a1", changes.Results[2].ID)

	response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes", "", "zegpold")
	log.Printf("2nd _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "_user/zegpold", changes.Results[0].ID)
	assert.Equal(t, "b1", changes.Results[1].ID)
	lastSeqPreGrant := changes.Last_Seq

	// Update "fashion" doc to grant zegpold the role "hipster" and take it away from alice:
	cacheWaiter.Add(1)
	str := fmt.Sprintf(`{"user":"zegpold", "role":"role:hipster", "_rev":%q}`, fashionRevID)
	response = rt.SendRequest("PUT", "/{{.keyspace}}/fashion", str)
	RequireStatus(t, response, 201)

	updatedRoleGrantSequence := rt.GetDocumentSequence("fashion")

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.Equal(t,
		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(0x1),
			"alpha": channels.NewVbSimpleSequence(alice.Sequence()),
		}, alice.InheritedCollectionChannels(s, c))

	zegpold, _ = a.GetUser("zegpold")
	assert.Equal(t,
		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(0x1),
			"beta":  channels.NewVbSimpleSequence(zegpold.Sequence()),
			"gamma": channels.NewVbSimpleSequence(updatedRoleGrantSequence),
		}, zegpold.InheritedCollectionChannels(s, c))

	// The complete _changes feed for zegpold contains docs g1 and b1:
	cacheWaiter.Wait()
	changes.Results = nil
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/_changes", "", "zegpold")
	log.Printf("3rd _changes looks like: %s", response.Body.Bytes())
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	log.Printf("changes: %+v", changes.Results)
	require.Equal(t, len(changes.Results), 3)
	assert.Equal(t, "_user/zegpold", changes.Results[0].ID)
	assert.Equal(t, "b1", changes.Results[1].ID)
	assert.Equal(t, "g1", changes.Results[2].ID)

	// Changes feed with since=lastSeqPreGrant would ordinarily be empty, but zegpold got access to channel
	// gamma after lastSeqPreGrant, so the pre-existing docs in that channel are included:
	response = rt.SendUserRequest("GET", fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", lastSeqPreGrant), "", "zegpold")
	log.Printf("4th _changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "g1", changes.Results[0].ID)
}

type UserRolesTemp struct {
	Roles map[string][]string `json:"roles"`
}
