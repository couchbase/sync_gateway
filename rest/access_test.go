//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublicChanGuestAccess(t *testing.T) {
	rt := NewRestTesterDefaultCollection(t, // CBG-2618: fix collection channel access
		&RestTesterConfig{
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				Guest: &auth.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			}},
		})
	defer rt.Close()

	// Create a document on the public channel
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc", `{"channels": ["!"], "foo": "bar"}`)
	RequireStatus(t, resp, http.StatusCreated)

	// Check guest user has access to public channel
	resp = rt.SendRequest(http.MethodGet, "/db/doc", "")
	RequireStatus(t, resp, http.StatusOK)
	assert.EqualValues(t, "bar", resp.GetRestDocument()["foo"])

	resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/GUEST", ``)
	RequireStatus(t, resp, http.StatusOK)
	fmt.Println("GUEST user:", resp.Body.String())
	assert.EqualValues(t, []interface{}{"!"}, resp.GetRestDocument()["all_channels"])

	// Confirm guest user cannot access other channels it has no access too
	resp = rt.SendAdminRequest(http.MethodPut, "/db/docNoAccess", `{"channels": ["cookie"], "foo": "bar"}`)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendRequest(http.MethodGet, "/db/docNoAccess", "")
	RequireStatus(t, resp, http.StatusForbidden)
}

func TestStarAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges)

	type allDocsRow struct {
		ID    string `json:"id"`
		Key   string `json:"key"`
		Value struct {
			Rev      string              `json:"rev"`
			Channels []string            `json:"channels,omitempty"`
			Access   map[string]base.Set `json:"access,omitempty"` // for admins only
		} `json:"value"`
		Doc   db.Body `json:"doc,omitempty"`
		Error string  `json:"error"`
	}
	var allDocsResult struct {
		TotalRows int          `json:"total_rows"`
		Offset    int          `json:"offset"`
		Rows      []allDocsRow `json:"rows"`
	}

	// Create some docs:
	rt := NewRestTesterDefaultCollection(t, nil) // CBG-2618: fix collection channel access
	defer rt.Close()

	database := rt.ServerContext().Database(rt.Context(), "db")
	if !database.Options.EnableStarChannel {
		t.Skip("This test requires StarChannel to be enabled")
	}

	a := auth.NewAuthenticator(rt.MetadataStore(), nil, auth.DefaultAuthenticatorOptions())
	var changes struct {
		Results []db.ChangeEntry
	}
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	RequireStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":["books"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["gifts"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["!"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["gifts"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":["!"]}`), 201)
	// document added to no channel should only end up available to users with * access
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc6", `{"channels":[]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.NoError(t, err)
	//
	// Part 1 - Tests for user with single channel access:
	//
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "books"))
	assert.NoError(t, a.Save(bernard))

	// GET /db/docid - basic test for channel user has
	response := rt.Send(RequestByUser("GET", "/db/doc1", "", "bernard"))
	RequireStatus(t, response, 200)

	// GET /db/docid - negative test for channel user doesn't have
	response = rt.Send(RequestByUser("GET", "/db/doc2", "", "bernard"))
	RequireStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(RequestByUser("GET", "/db/doc3", "", "bernard"))
	RequireStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns the docs the user has access to:
	response = rt.Send(RequestByUser("GET", "/db/_all_docs?channels=true", "", "bernard"))
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"books"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc3", allDocsResult.Rows[1].ID)
	assert.Equal(t, []string{"!"}, allDocsResult.Rows[1].Value.Channels)

	// Ensure docs have been processed before issuing changes requests
	expectedSeq := uint64(6)
	_ = rt.WaitForSequence(expectedSeq)

	// GET /db/_changes
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(changes.Results))
	since := changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for single channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=books", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	// GET /db/_changes for unauthorized channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=gifts", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(changes.Results))

	//
	// Part 2 - Tests for user with * channel access
	//

	// Create a user:
	fran, err := a.NewUser("fran", "letmein", channels.BaseSetOf(t, "*"))
	assert.NoError(t, a.Save(fran))

	// GET /db/docid - basic test for doc that has channel
	response = rt.Send(RequestByUser("GET", "/db/doc1", "", "fran"))
	RequireStatus(t, response, 200)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(RequestByUser("GET", "/db/doc3", "", "fran"))
	RequireStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs returns all docs (based on user * channel)
	response = rt.Send(RequestByUser("GET", "/db/_all_docs?channels=true", "", "fran"))
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"books"}, allDocsResult.Rows[0].Value.Channels)

	// GET /db/_changes
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, uint64(1), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "fran"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	//
	// Part 3 - Tests for user with no user channel access
	//
	// Create a user:
	manny, err := a.NewUser("manny", "letmein", nil)
	assert.NoError(t, a.Save(manny))

	// GET /db/docid - basic test for doc that has channel
	response = rt.Send(RequestByUser("GET", "/db/doc1", "", "manny"))
	RequireStatus(t, response, 403)

	// GET /db/docid - test for doc with ! channel
	response = rt.Send(RequestByUser("GET", "/db/doc3", "", "manny"))
	RequireStatus(t, response, 200)

	// GET /db/_all_docs?channels=true
	// Check that _all_docs only returns ! docs (based on doc ! channel)
	response = rt.Send(RequestByUser("GET", "/db/_all_docs?channels=true", "", "manny"))
	RequireStatus(t, response, 200)
	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)

	// GET /db/_changes
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)

	// GET /db/_changes for ! channel
	response = rt.Send(RequestByUser("GET", "/db/_changes?filter=sync_gateway/bychannel&channels=!", "", "manny"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))
	since = changes.Results[0].Seq
	assert.Equal(t, "doc3", changes.Results[0].ID)
	assert.Equal(t, uint64(3), since.Seq)
}

func TestNumAccessErrors(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc, oldDoc){if (doc.channels.indexOf("foo") > -1){requireRole("foobar")}}`,
	}

	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

	// Create a test user
	user, err := a.NewUser("user", "letmein", channels.BaseSetOf(t, "A"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	response := rt.Send(RequestByUser("PUT", "/db/doc", `{"prop":true, "channels":["foo"]}`, "user"))
	RequireStatus(t, response, 403)

	base.WaitForStat(func() int64 { return rt.GetDatabase().DbStats.SecurityStats.NumAccessErrors.Value() }, 1)
}
func TestUserHasDocAccessDocNotFound(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.IntPtr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					Size: base.Uint32Ptr(0),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.IntPtr(0),
				},
			},
		}},
	})
	defer rt.Close()
	ctx := rt.Context()

	resp := rt.SendAdminRequest("PUT", "/db/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	database, err := db.CreateDatabase(rt.GetDatabase())
	assert.NoError(t, err)

	collection := database.GetSingleDatabaseCollectionWithUser()
	userHasDocAccess, err := db.UserHasDocAccess(ctx, collection, "doc")
	assert.NoError(t, err)
	assert.True(t, userHasDocAccess)

	// Purge the document from the bucket to force 'not found'
	err = collection.Purge(ctx, "doc")

	userHasDocAccess, err = db.UserHasDocAccess(ctx, collection, "doc")
	assert.NoError(t, err)
	assert.False(t, userHasDocAccess)
}

// CBG-2143: Make sure the REST API is returning forbidden errors if when unsupported config option is set
func TestForceAPIForbiddenErrors(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyHTTP)
	testCases := []struct {
		forceForbiddenErrors bool
	}{
		{
			forceForbiddenErrors: true,
		},
		{
			forceForbiddenErrors: false,
		},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("Forbidden errors %v", test.forceForbiddenErrors), func(t *testing.T) {
			// assertRespStatus changes behaviour depending on if forcing forbidden errors
			assertRespStatus := func(resp *TestResponse, statusIfForbiddenErrorsFalse int) {
				if test.forceForbiddenErrors {
					assertHTTPErrorReason(t, resp, http.StatusForbidden, "forbidden")
					return
				}
				AssertStatus(t, resp, statusIfForbiddenErrorsFalse)
			}

			rt := NewRestTesterDefaultCollection(t, // CBG-2618: fix collection channel access
				&RestTesterConfig{
					SyncFn: `
				function(doc, oldDoc) {
					if (!doc.doNotSync) {
						access("NoPerms", "chan2");
						access("Perms", "chan2");
						requireAccess("chan");
						channel(doc.channels);
					}
				}`,
					DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
						Unsupported: &db.UnsupportedOptions{
							ForceAPIForbiddenErrors: test.forceForbiddenErrors,
						},
						Guest: &auth.PrincipalConfig{
							Disabled: base.BoolPtr(false),
						},
						Users: map[string]*auth.PrincipalConfig{
							"NoPerms": {
								Password: base.StringPtr("password"),
							},
							"Perms": {
								ExplicitChannels: base.SetOf("chan"),
								Password:         base.StringPtr("password"),
							},
						},
					}},
				})
			defer rt.Close()

			// Create the initial document
			resp := rt.SendAdminRequest(http.MethodPut, "/db/doc", `{"doNotSync": true, "foo": "bar", "channels": "chan", "_attachment":{"attach": {"data": "`+base64.StdEncoding.EncodeToString([]byte("attachmentA"))+`"}}}`)
			RequireStatus(t, resp, http.StatusCreated)
			rev := RespRevID(t, resp)

			// GET requests
			// User has no permissions to access document
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// Guest has no permissions to access document
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc", "", nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)

			// User has no permissions to access rev
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc?rev="+rev, "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusOK)

			// Guest has no permissions to access rev
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc?rev="+rev, "", nil, "", "")
			assertRespStatus(resp, http.StatusOK)

			// Attachments should be forbidden as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// Attachment revs should be forbidden as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach?rev="+rev, "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusNotFound)

			// Attachments should be forbidden for guests as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach", "", nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)

			// Attachment revs should be forbidden for guests as well
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/doc/attach?rev="+rev, "", nil, "", "")
			assertRespStatus(resp, http.StatusNotFound)

			// Document does not exist should cause 403
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/notfound", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusNotFound)

			// Document does not exist for guest should cause 403
			resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/notfound", "", nil, "", "")
			assertRespStatus(resp, http.StatusNotFound)

			// PUT requests
			// PUT doc with user with no write perms
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc", `{}`, nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// PUT with rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev="+rev, `{}`, nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// PUT with incorrect rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev=1-abc", `{}`, nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// PUT request as Guest
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc", `{}`, nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// PUT with rev as Guest
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev="+rev, `{}`, nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)

			// PUT with incorrect rev as Guest
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev=1-abc", `{}`, nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// PUT with access but no rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc", `{}`, nil, "Perms", "password")
			assertHTTPErrorReason(t, resp, http.StatusConflict, "Document exists")

			// PUT with access but wrong rev
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev=1-abc", `{}`, nil, "Perms", "password")
			assertHTTPErrorReason(t, resp, http.StatusConflict, "Document revision conflict")

			// Confirm no access grants where granted
			resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/NoPerms", ``)
			RequireStatus(t, resp, http.StatusOK)
			var allChannels struct {
				Channels []string `json:"all_channels"`
			}
			err := json.Unmarshal(resp.BodyBytes(), &allChannels)
			require.NoError(t, err)
			assert.NotContains(t, allChannels.Channels, "chan2")

			resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/Perms", ``)
			RequireStatus(t, resp, http.StatusOK)
			err = json.Unmarshal(resp.BodyBytes(), &allChannels)
			require.NoError(t, err)
			assert.NotContains(t, allChannels.Channels, "chan2")

			// Successful PUT which will grant access grants
			resp = rt.SendUserRequestWithHeaders(http.MethodPut, "/db/doc?rev="+rev, `{"channels": "chan"}`, nil, "Perms", "password")
			AssertStatus(t, resp, http.StatusCreated)

			// Make sure channel access grant was successful
			resp = rt.SendAdminRequest(http.MethodGet, "/db/_user/Perms", ``)
			RequireStatus(t, resp, http.StatusOK)
			err = json.Unmarshal(resp.BodyBytes(), &allChannels)
			require.NoError(t, err)
			assert.Contains(t, allChannels.Channels, "chan2")

			// DELETE requests
			// Attempt to delete document with no permissions
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document rev with no permissions
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev="+rev, "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document with wrong rev
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev=1-abc", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document document that does not exist
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/notfound", "", nil, "NoPerms", "password")
			assertRespStatus(resp, http.StatusForbidden)

			// Attempt to delete document with no permissions as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc", "", nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document rev with no write perms as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev="+rev, "", nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document with wrong rev as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/doc?rev=1-abc", "", nil, "", "")
			assertRespStatus(resp, http.StatusConflict)

			// Attempt to delete document that does not exist as guest
			resp = rt.SendUserRequestWithHeaders(http.MethodDelete, "/db/notfound", "", nil, "", "")
			assertRespStatus(resp, http.StatusForbidden)
		})
	}
}
func TestBulkDocsChangeToAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { requireAccess(doc.channel)}}`}
	rt := NewRestTesterDefaultCollection(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", nil)
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))

	input := `{"docs": [{"_id": "bulk1", "type" : "setaccess", "owner":"user1" , "channel":"chan1"}, {"_id": "bulk2" , "channel":"chan1"}]}`

	response := rt.Send(RequestByUser("POST", "/db/_bulk_docs", input, "user1"))
	RequireStatus(t, response, 201)

	var docs []interface{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docs))
	assert.Equal(t, 2, len(docs))
	assert.Equal(t, map[string]interface{}{"rev": "1-afbcffa8a4641a0f4dd94d3fc9593e74", "id": "bulk1"}, docs[0])

	assert.Equal(t, map[string]interface{}{"rev": "1-4d79588b9fe9c38faae61f0c1b9471c0", "id": "bulk2"}, docs[1])

}

// Test _all_docs API call under different security scenarios
func TestAllDocsAccessControl(t *testing.T) {

	rt := NewRestTesterDefaultCollection(t, nil) // CBG-2618: fix collection channel access
	defer rt.Close()

	database := rt.ServerContext().Database(rt.Context(), "db")
	if !database.Options.EnableStarChannel {
		t.Skip("This test requires StarChannel to be enabled")
	}

	type allDocsRow struct {
		ID    string `json:"id"`
		Key   string `json:"key"`
		Value struct {
			Rev      string              `json:"rev"`
			Channels []string            `json:"channels,omitempty"`
			Access   map[string]base.Set `json:"access,omitempty"` // for admins only
		} `json:"value"`
		Doc   db.Body `json:"doc,omitempty"`
		Error string  `json:"error"`
	}
	type allDocsResponse struct {
		TotalRows int          `json:"total_rows"`
		Offset    int          `json:"offset"`
		Rows      []allDocsRow `json:"rows"`
	}

	// Create some docs:
	a := auth.NewAuthenticator(rt.MetadataStore(), nil, auth.DefaultAuthenticatorOptions())
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	RequireStatus(t, rt.SendRequest("PUT", "/db/doc5", `{"channels":"Cinemax"}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["WB", "Cinemax"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["CBS", "Cinemax"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["CBS"]}`), 201)
	RequireStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":[]}`), 201)

	guest.SetDisabled(true)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create a user:
	alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "Cinemax"))
	assert.NoError(t, a.Save(alice))

	// Get a single doc the user has access to:
	request, _ := http.NewRequest("GET", "/db/doc3", nil)
	request.SetBasicAuth("alice", "letmein")
	response := rt.Send(request)
	RequireStatus(t, response, 200)

	// Get a single doc the user doesn't have access to:
	request, _ = http.NewRequest("GET", "/db/doc2", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 403)

	// Check that _all_docs only returns the docs the user has access to:
	request, _ = http.NewRequest("GET", "/db/_all_docs?channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	allDocsResult := allDocsResponse{}
	log.Printf("Response = %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 3, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc4", allDocsResult.Rows[1].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[1].Value.Channels)
	assert.Equal(t, "doc5", allDocsResult.Rows[2].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[2].Value.Channels)

	// Check all docs limit option
	request, _ = http.NewRequest("GET", "/db/_all_docs?limit=1&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs startkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?startkey=doc5&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc5", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs startkey option with double quote
	request, _ = http.NewRequest("GET", `/db/_all_docs?startkey="doc5"&channels=true`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc5", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs endkey option
	request, _ = http.NewRequest("GET", "/db/_all_docs?endkey=doc3&channels=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check all docs endkey option
	request, _ = http.NewRequest("GET", `/db/_all_docs?endkey="doc3"&channels=true`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check _all_docs with include_docs option:
	request, _ = http.NewRequest("GET", "/db/_all_docs?include_docs=true", nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 3, len(allDocsResult.Rows))
	assert.Equal(t, "doc3", allDocsResult.Rows[0].ID)
	assert.Equal(t, "doc4", allDocsResult.Rows[1].ID)
	assert.Equal(t, "doc5", allDocsResult.Rows[2].ID)

	// Check POST to _all_docs:
	body := `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 4, len(allDocsResult.Rows))
	assert.Equal(t, "doc4", allDocsResult.Rows[0].Key)
	assert.Equal(t, "doc4", allDocsResult.Rows[0].ID)
	assert.Equal(t, "1-e0351a57554e023a77544d33dd21e56c", allDocsResult.Rows[0].Value.Rev)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc1", allDocsResult.Rows[1].Key)
	assert.Equal(t, "forbidden", allDocsResult.Rows[1].Error)
	assert.Equal(t, "", allDocsResult.Rows[1].Value.Rev)
	assert.Equal(t, "doc3", allDocsResult.Rows[2].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[2].Value.Channels)
	assert.Equal(t, "1-20912648f85f2bbabefb0993ddd37b41", allDocsResult.Rows[2].Value.Rev)
	assert.Equal(t, "b0gus", allDocsResult.Rows[3].Key)
	assert.Equal(t, "not_found", allDocsResult.Rows[3].Error)
	assert.Equal(t, "", allDocsResult.Rows[3].Value.Rev)

	// Check GET to _all_docs with keys parameter:
	request, _ = http.NewRequest("GET", `/db/_all_docs?channels=true&keys=%5B%22doc4%22%2C%22doc1%22%2C%22doc3%22%2C%22b0gus%22%5D`, nil)
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response from GET _all_docs = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 4, len(allDocsResult.Rows))
	assert.Equal(t, "doc4", allDocsResult.Rows[0].Key)
	assert.Equal(t, "doc4", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)
	assert.Equal(t, "doc1", allDocsResult.Rows[1].Key)
	assert.Equal(t, "forbidden", allDocsResult.Rows[1].Error)
	assert.Equal(t, "doc3", allDocsResult.Rows[2].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[2].Value.Channels)
	assert.Equal(t, "b0gus", allDocsResult.Rows[3].Key)
	assert.Equal(t, "not_found", allDocsResult.Rows[3].Error)

	// Check POST to _all_docs with limit option:
	body = `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	request, _ = http.NewRequest("POST", "/db/_all_docs?limit=1&channels=true", bytes.NewBufferString(body))
	request.SetBasicAuth("alice", "letmein")
	response = rt.Send(request)
	RequireStatus(t, response, 200)

	log.Printf("Response from POST _all_docs = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(allDocsResult.Rows))
	assert.Equal(t, "doc4", allDocsResult.Rows[0].Key)
	assert.Equal(t, "doc4", allDocsResult.Rows[0].ID)
	assert.Equal(t, []string{"Cinemax"}, allDocsResult.Rows[0].Value.Channels)

	// Check _all_docs as admin:
	response = rt.SendAdminRequest("GET", "/db/_all_docs", "")
	RequireStatus(t, response, 200)

	log.Printf("Admin response = %s", response.Body.Bytes())
	allDocsResult = allDocsResponse{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &allDocsResult)
	require.NoError(t, err)
	require.Equal(t, 5, len(allDocsResult.Rows))
	assert.Equal(t, "doc1", allDocsResult.Rows[0].ID)
	assert.Equal(t, "doc2", allDocsResult.Rows[1].ID)
}
func TestChannelAccessChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {access(doc.owner, doc._id);channel(doc.channel)}`}
	rt := NewRestTesterDefaultCollection(t, &rtConfig) // CBG-2618: fix collection channel access
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create users:
	alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(alice))
	zegpold, err := a.NewUser("zegpold", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(zegpold))

	// Create some docs that give users access:
	response := rt.Send(Request("PUT", "/db/alpha", `{"owner":"alice"}`))
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	alphaRevID := body["rev"].(string)

	RequireStatus(t, rt.Send(Request("PUT", "/db/beta", `{"owner":"boadecia"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/delta", `{"owner":"alice"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/gamma", `{"owner":"zegpold"}`)), 201)

	RequireStatus(t, rt.Send(Request("PUT", "/db/a1", `{"channel":"alpha"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/b1", `{"channel":"beta"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/d1", `{"channel":"delta"}`)), 201)
	RequireStatus(t, rt.Send(Request("PUT", "/db/g1", `{"channel":"gamma"}`)), 201)

	rt.MustWaitForDoc("g1", t)

	changes := ChangesResults{}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "zegpold"))
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)

	assert.NoError(t, err)
	require.Len(t, changes.Results, 1)
	since := changes.Results[0].Seq
	assert.Equal(t, "g1", changes.Results[0].ID)

	// Look up sequences for created docs
	deltaGrantDocSeq, err := rt.SequenceForDoc("delta")
	assert.NoError(t, err, "Error retrieving document sequence")
	gammaGrantDocSeq, err := rt.SequenceForDoc("gamma")
	assert.NoError(t, err, "Error retrieving document sequence")

	alphaDocSeq, err := rt.SequenceForDoc("a1")
	assert.NoError(t, err, "Error retrieving document sequence")
	gammaDocSeq, err := rt.SequenceForDoc("g1")
	assert.NoError(t, err, "Error retrieving document sequence")

	// Check user access:
	alice, _ = a.GetUser("alice")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"alpha": channels.NewVbSimpleSequence(uint64(1)),
			"delta": channels.NewVbSimpleSequence(deltaGrantDocSeq),
		}, alice.Channels())

	zegpold, _ = a.GetUser("zegpold")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"gamma": channels.NewVbSimpleSequence(gammaGrantDocSeq),
		}, zegpold.Channels())

	// Update a document to revoke access to alice and grant it to zegpold:
	str := fmt.Sprintf(`{"owner":"zegpold", "_rev":%q}`, alphaRevID)
	RequireStatus(t, rt.Send(Request("PUT", "/db/alpha", str)), 201)

	alphaGrantDocSeq, err := rt.SequenceForDoc("alpha")
	assert.NoError(t, err, "Error retrieving document sequence")

	// Check user access again:
	alice, _ = a.GetUser("alice")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"delta": channels.NewVbSimpleSequence(deltaGrantDocSeq),
		}, alice.Channels())

	zegpold, _ = a.GetUser("zegpold")
	assert.Equal(
		t,

		channels.TimedSet{
			"!":     channels.NewVbSimpleSequence(uint64(1)),
			"zero":  channels.NewVbSimpleSequence(uint64(1)),
			"alpha": channels.NewVbSimpleSequence(alphaGrantDocSeq),
			"gamma": channels.NewVbSimpleSequence(gammaGrantDocSeq),
		}, zegpold.Channels())

	rt.MustWaitForDoc("alpha", t)

	// Look at alice's _changes feed:
	changes = ChangesResults{}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "alice"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	assert.NoError(t, err)
	assert.Equal(t, "d1", changes.Results[0].ID)

	// The complete _changes feed for zegpold contains docs a1 and g1:
	changes = ChangesResults{}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "zegpold"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	assert.NoError(t, err)
	require.Len(t, changes.Results, 2)
	assert.Equal(t, "g1", changes.Results[0].ID)
	assert.Equal(t, gammaDocSeq, changes.Results[0].Seq.Seq)
	assert.Equal(t, "a1", changes.Results[1].ID)
	assert.Equal(t, alphaDocSeq, changes.Results[1].Seq.Seq)
	assert.Equal(t, alphaGrantDocSeq, changes.Results[1].Seq.TriggeredBy)

	// Changes feed with since=gamma:8 would ordinarily be empty, but zegpold got access to channel
	// alpha after sequence 8, so the pre-existing docs in that channel are included:
	response = rt.Send(RequestByUser("GET", fmt.Sprintf("/db/_changes?since=\"%s\"", since),
		"", "zegpold"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "a1", changes.Results[0].ID)

	// What happens if we call access() with a nonexistent username?
	RequireStatus(t, rt.Send(Request("PUT", "/db/epsilon", `{"owner":"waldo"}`)), 201) // seq 10

	// Must wait for sequence to arrive in cache, since the cache processor will be paused when UpdateSyncFun() is called
	// below, which could lead to a data race if the cache processor is paused while it's processing a change
	rt.MustWaitForDoc("epsilon", t)

	// Finally, throw a wrench in the works by changing the sync fn. Note that normally this wouldn't
	// be changed while the database is in use (only when it's re-opened) but for testing purposes
	// we do it now because we can't close and re-open an ephemeral Walrus database.
	dbc := rt.ServerContext().Database(ctx, "db")
	database, _ := db.GetDatabase(dbc, nil)

	collection := database.GetSingleDatabaseCollectionWithUser()

	changed, err := database.UpdateSyncFun(ctx, `function(doc) {access("alice", "beta");channel("beta");}`)
	assert.NoError(t, err)
	assert.True(t, changed)
	changeCount, err := collection.UpdateAllDocChannels(ctx, false, func(docsProcessed, docsChanged *int) {}, base.NewSafeTerminator())
	assert.NoError(t, err)
	assert.Equal(t, 9, changeCount)

	expectedIDs := []string{"beta", "delta", "gamma", "a1", "b1", "d1", "g1", "alpha", "epsilon"}
	changes, err = rt.WaitForChanges(len(expectedIDs), "/db/_changes", "alice", false)
	assert.NoError(t, err, "Unexpected error")
	log.Printf("_changes looks like: %+v", changes)
	assert.Equal(t, len(expectedIDs), len(changes.Results))

	require.Len(t, changes.Results, len(expectedIDs))
	for i, expectedID := range expectedIDs {
		if changes.Results[i].ID != expectedID {
			log.Printf("changes.Results[i].ID != expectedID.  changes.Results: %+v, expectedIDs: %v", changes.Results, expectedIDs)
		}
		assert.Equal(t, expectedID, changes.Results[i].ID)
	}

}
func TestAccessOnTombstone(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD)

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 if (doc.owner) {
			 	access(doc.owner, doc.channel);
			 }
			 if (doc._deleted && oldDoc.owner) {
			 	access(oldDoc.owner, oldDoc.channel);
			 }
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	err = a.Save(guest)
	assert.NoError(t, err)

	// Create user:
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(bernard))

	// Create doc that gives user access to its channel
	response := rt.SendAdminRequest("PUT", "/db/alpha", `{"owner":"bernard", "channel":"PBS"}`)
	RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	assert.NoError(t, rt.WaitForPendingChanges())

	// Validate the user gets the doc on the _changes feed
	// Check the _changes feed:
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.NoError(t, err)
	require.Len(t, changes.Results, 1)
	if len(changes.Results) > 0 {
		assert.Equal(t, "alpha", changes.Results[0].ID)
	}

	// Delete the document
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/alpha?rev=%s", revId), "")
	RequireStatus(t, response, 200)

	// Make sure it actually was deleted
	response = rt.SendAdminRequest("GET", "/db/alpha", "")
	RequireStatus(t, response, 404)

	// Wait for change caching to complete
	assert.NoError(t, rt.WaitForPendingChanges())

	// Check user access again:
	changes.Results = nil
	response = rt.Send(RequestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &changes))
	require.Len(t, changes.Results, 1)
	if len(changes.Results) > 0 {
		assert.Equal(t, "alpha", changes.Results[0].ID)
		assert.Equal(t, true, changes.Results[0].Deleted)
	}

}

func TestDynamicChannelGrant(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { channel(doc.channel)}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	require.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", nil)
	require.NoError(t, err)
	require.NoError(t, a.Save(user))

	collection := rt.GetDatabase().GetSingleDatabaseCollection()
	keyspace := fmt.Sprintf("%s.%s.%s", "db", collection.ScopeName(), collection.Name())

	// Create a document in channel chan1
	response := rt.Send(RequestByUser("PUT", "/"+keyspace+"/doc1", `{"channel":"chan1", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user cannot access document
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc1", "", "user1"))
	RequireStatus(t, response, 403)

	// Write access granting document
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/grant1", `{"type":"setaccess", "owner":"user1", "channel":"chan1"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user can access document
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc1", "", "user1"))
	RequireStatus(t, response, 200)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "hello", body["greeting"])

	// Create a document in channel chan2
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/doc2", `{"channel":"chan2", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)

	// Write access granting document for chan2 (tests invalidation when channels/inval_seq exists)
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/grant2", `{"type":"setaccess", "owner":"user1", "channel":"chan2"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user can now access both documents
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc1", "", "user1"))
	RequireStatus(t, response, 200)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc2", "", "user1"))
	RequireStatus(t, response, 200)
}

// Verify a dynamic grant of a channel to a role is inherited by a user with that role
func TestRoleChannelGrantInheritance(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {if(doc.type == "setaccess") {channel(doc.channel); access(doc.owner, doc.channel);} else { channel(doc.channel)}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

	collection := rt.GetDatabase().GetSingleDatabaseCollection()
	scopeName := collection.ScopeName()
	collectionName := collection.Name()
	keyspace := fmt.Sprintf("%s.%s.%s", "db", scopeName, collectionName)

	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	require.NoError(t, err)

	// Create a role with admin grant of chan1
	role, err := a.NewRole("role1", nil)
	role.SetCollectionExplicitChannels(scopeName, collectionName, channels.TimedSet{"chan1": channels.NewVbSimpleSequence(1)}, 1)
	require.NoError(t, err)
	require.NoError(t, a.Save(role))

	// Create a test user with access to the role
	user, err = a.NewUser("user1", "letmein", nil)
	require.NoError(t, err)
	user.SetExplicitRoles(channels.TimedSet{"role1": channels.NewVbSimpleSequence(1)}, 1)
	require.NoError(t, a.Save(user))

	// Create documents in channels chan1, chan2, chan3
	response := rt.Send(RequestByUser("PUT", "/"+keyspace+"/doc1", `{"channel":"chan1", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/doc2", `{"channel":"chan2", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/doc3", `{"channel":"chan3", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user can access document in admin role channel (chan1)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc1", "", "user1"))
	RequireStatus(t, response, 200)

	// Verify user cannot access other documents
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc2", "", "user1"))
	RequireStatus(t, response, 403)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc3", "", "user1"))
	RequireStatus(t, response, 403)

	// Write access granting document (grants chan2 to role role1)
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/grant1", `{"type":"setaccess", "owner":"role:role1", "channel":"chan2"}`, "user1"))
	RequireStatus(t, response, 201)
	grant1Rev := RespRevID(t, response)

	// Verify user can access document
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc2", "", "user1"))
	RequireStatus(t, response, 200)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "hello", body["greeting"])

	// Write access granting document for chan2 (tests invalidation when channels/inval_seq exists)
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/grant2", `{"type":"setaccess", "owner":"role:role1", "channel":"chan3"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user can now access all three documents
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc1", "", "user1"))
	RequireStatus(t, response, 200)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc2", "", "user1"))
	RequireStatus(t, response, 200)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc3", "", "user1"))
	RequireStatus(t, response, 200)

	// Revoke access to chan2 (dynamic)
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/grant1?rev="+grant1Rev, `{"type":"setaccess", "owner":"none", "channel":"chan2"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user cannot access doc in revoked channel, but can successfully access remaining documents
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc2", "", "user1"))
	RequireStatus(t, response, 403)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc1", "", "user1"))
	RequireStatus(t, response, 200)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/doc3", "", "user1"))
	RequireStatus(t, response, 200)

}

func TestPublicChannel(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess, base.KeyHTTP, base.KeyHTTPResp)

	rtConfig := RestTesterConfig{SyncFn: `
           function(doc) {
              if(doc.type == "public") {
                 channel("!")
              } else { 
                 channel(doc.channel)
              }
           }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.GetUser("")
	require.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	require.NoError(t, err)

	// Create a test user
	user, err = a.NewUser("user1", "letmein", nil)
	require.NoError(t, err)
	require.NoError(t, a.Save(user))

	collection := rt.GetDatabase().GetSingleDatabaseCollection()
	keyspace := fmt.Sprintf("%s.%s.%s", "db", collection.ScopeName(), collection.Name())

	// Create a document in public channel
	response := rt.Send(RequestByUser("PUT", "/"+keyspace+"/publicDoc", `{"type":"public", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)

	// Create a document in non-public channel
	response = rt.Send(RequestByUser("PUT", "/"+keyspace+"/privateDoc", `{"channel":"restricted", "greeting":"hello"}`, "user1"))
	RequireStatus(t, response, 201)

	// Verify user can access public document, cannot access non-public document
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/publicDoc", "", "user1"))
	RequireStatus(t, response, 200)
	response = rt.Send(RequestByUser("GET", "/"+keyspace+"/privateDoc", "", "user1"))
	RequireStatus(t, response, 403)
}
