//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicChannelGrant(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAccess)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	dbCollection := GetSingleDatabaseCollectionWithUser(t, db)

	db.ChannelMapper = channels.NewChannelMapper(&db.V8VMs, `
	function(doc) {
		if(doc.type == "setaccess") {
			channel(doc.channel);
			access(doc.owner, doc.channel);
		} else {
			channel(doc.channel)
		}
	}`, 0)

	a := dbCollection.Authenticator(ctx)
	user, err := a.NewUser("user1", "letmein", nil)
	assert.NoError(t, err)
	assert.NoError(t, a.Save(user))
	dbCollection.user = user

	// Create a document in channel chan1
	doc1Body := Body{"channel": "chan1", "greeting": "hello"}
	_, _, err = dbCollection.PutExistingRevWithBody(ctx, "doc1", doc1Body, []string{"1-a"}, false)
	require.NoError(t, err)

	// Verify user cannot access document
	existingBody, err := dbCollection.Get1xBody(ctx, "doc1")
	require.Error(t, err)

	// Write access granting document
	grantingBody := Body{"type": "setaccess", "owner": "user1", "channel": "chan1"}
	_, _, err = dbCollection.PutExistingRevWithBody(ctx, "grant1", grantingBody, []string{"1-a"}, false)
	require.NoError(t, err)

	// Verify reloaded user can access document
	require.NoError(t, dbCollection.ReloadUser(ctx))
	existingBody, err = dbCollection.Get1xBody(ctx, "doc1")
	require.NoError(t, err)
	require.NotNil(t, existingBody)
	assert.Equal(t, "hello", existingBody["greeting"])

	// Create a document in channel chan2
	doc2Body := Body{"channel": "chan2", "greeting": "hello"}
	_, _, err = dbCollection.PutExistingRevWithBody(ctx, "doc2", doc2Body, []string{"1-a"}, false)
	require.NoError(t, err)

	// Write access granting document for chan2 (tests invalidation when channels/inval_seq exists)
	grantingBody = Body{"type": "setaccess", "owner": "user1", "channel": "chan2"}
	_, _, err = dbCollection.PutExistingRevWithBody(ctx, "grant2", grantingBody, []string{"1-a"}, false)
	require.NoError(t, err)

	// Verify user can now access both documents
	require.NoError(t, dbCollection.ReloadUser(ctx))
	existingBody, err = dbCollection.Get1xBody(ctx, "doc1")
	require.NoError(t, err)
	existingBody, err = dbCollection.Get1xBody(ctx, "doc2")
	require.NoError(t, err)
}
