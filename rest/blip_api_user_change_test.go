// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/require"
)

// updateUserChannels grants username exactly the given channel set via the admin API.
func updateUserChannels(rt *RestTester, username string, chans []string, roles []string) {
	rt.TB().Helper()
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+username,
		GetUserPayload(rt.TB(), "", RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), chans, roles))
	RequireStatus(rt.TB(), resp, http.StatusOK)
}

// updateRoleChannels sets the channel set on an existing role via the admin API.
func updateRoleChannels(rt *RestTester, roleName string, chans []string) {
	rt.TB().Helper()
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+roleName,
		GetRolePayload(rt.TB(), "", rt.GetSingleDataStore(), chans))
	RequireStatus(rt.TB(), resp, http.StatusOK)
}

// TestBlipContinuousPullUserChannelGrant covers the headline case: a continuous pull parked in
// ChangeWaiter.Wait with nothing to send, woken only by a channel grant on the user document, must
// back-fill the newly granted channel. If the feed wakes but fails to observe the user count moving,
// checkForUserUpdates skips the reload (it only reloads on a count change for continuous feeds) and
// the granted document is never sent.
func TestBlipContinuousPullUserChannelGrant(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const (
			username     = "alice"
			grantedChan  = "chan1"
			deferredChan = "chan2"
		)
		rt.CreateUser(username, []string{grantedChan})

		// deferredDoc first, so its sequence is lower than visibleDoc's
		deferredDoc := rt.PutDoc("deferredDoc", `{"channels":["`+deferredChan+`"]}`)
		visibleDoc := rt.PutDoc("visibleDoc", `{"channels":["`+grantedChan+`"]}`)
		rt.WaitForPendingChanges()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: username})
		defer client.Close()
		btcRunner.StartPull(client.id)

		// Once visibleDoc has arrived the feed is live and caught up, and deferredDoc - written
		// earlier, so ordered first - would already have arrived if it were visible.
		btcRunner.WaitForVersion(client.id, "visibleDoc", visibleDoc)
		_, found := btcRunner.GetVersion(client.id, "deferredDoc", deferredDoc)
		require.False(t, found, "document in an ungranted channel was replicated before the grant")

		// The grant is the only thing that happens from here - no document writes.
		userWaiter := rt.NewUserWaiter(username)
		updateUserChannels(rt, username, []string{grantedChan, deferredChan}, nil)
		db.WaitForUserWaiterChange(t, userWaiter)

		btcRunner.WaitForVersion(client.id, "deferredDoc", deferredDoc)
	})
}

// TestBlipContinuousPullRoleGrant is the same shape, but the grant arrives by adding a role to the
// user rather than a channel. This additionally exercises the RefreshUserKeys path, which rebuilds
// the waiter's principal key set when the user's roles change.
func TestBlipContinuousPullRoleGrant(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const (
			username    = "alice"
			roleName    = "chan2-role"
			userChan    = "chan1"
			roleChan    = "chan2"
			deferredDoc = "deferredDoc"
			visibleDoc  = "visibleDoc"
		)
		rt.CreateRole(roleName, []string{roleChan})
		rt.CreateUser(username, []string{userChan})

		deferredVersion := rt.PutDoc(deferredDoc, `{"channels":["`+roleChan+`"]}`)
		visibleVersion := rt.PutDoc(visibleDoc, `{"channels":["`+userChan+`"]}`)
		rt.WaitForPendingChanges()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: username})
		defer client.Close()
		btcRunner.StartPull(client.id)

		btcRunner.WaitForVersion(client.id, visibleDoc, visibleVersion)
		_, found := btcRunner.GetVersion(client.id, deferredDoc, deferredVersion)
		require.False(t, found, "document in the role's channel was replicated before the role grant")

		userWaiter := rt.NewUserWaiter(username)
		updateUserChannels(rt, username, []string{userChan}, []string{roleName})
		db.WaitForUserWaiterChange(t, userWaiter)

		btcRunner.WaitForVersion(client.id, deferredDoc, deferredVersion)
	})
}

// TestBlipContinuousPullRoleChannelGrant changes the ROLE document while the user document stays
// untouched. The waiter only wakes, and only sees its user count move, if the role's principal key is
// tracked alongside the user's - so this is the case that catches a change which tracks the user key
// but forgets the role keys. Nothing in the existing BLIP suite covers it.
func TestBlipContinuousPullRoleChannelGrant(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const (
			username    = "alice"
			roleName    = "grant-role"
			roleChan    = "chan1"
			deferredCh  = "chan2"
			deferredDoc = "deferredDoc"
			visibleDoc  = "visibleDoc"
		)
		// The user holds the role from the start, so the user document never changes in this test.
		rt.CreateRole(roleName, []string{roleChan})
		rt.CreateUser(username, nil, roleName)

		deferredVersion := rt.PutDoc(deferredDoc, `{"channels":["`+deferredCh+`"]}`)
		visibleVersion := rt.PutDoc(visibleDoc, `{"channels":["`+roleChan+`"]}`)
		rt.WaitForPendingChanges()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: username})
		defer client.Close()
		btcRunner.StartPull(client.id)

		btcRunner.WaitForVersion(client.id, visibleDoc, visibleVersion)
		_, found := btcRunner.GetVersion(client.id, deferredDoc, deferredVersion)
		require.False(t, found, "document in an ungranted channel was replicated before the role update")

		// Only the role document is written - the user document is untouched.
		userWaiter := rt.NewUserWaiter(username)
		updateRoleChannels(rt, roleName, []string{roleChan, deferredCh})
		db.WaitForUserWaiterChange(t, userWaiter)

		btcRunner.WaitForVersion(client.id, deferredDoc, deferredVersion)
	})
}

// TestBlipOneShotPullAfterUserChannelGrant covers the one-shot path, which reaches the user count
// through a different route: SimpleMultiChangesFeed baselines it during feed initialisation, and
// checkForUserUpdates force-reloads for non-continuous feeds regardless of the count. The grant is
// notified before the pull starts, so this asserts the feed initialisation sees the current user.
func TestBlipOneShotPullAfterUserChannelGrant(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const (
			username     = "alice"
			grantedChan  = "chan1"
			deferredChan = "chan2"
		)
		rt.CreateUser(username, []string{grantedChan})

		deferredDoc := rt.PutDoc("deferredDoc", `{"channels":["`+deferredChan+`"]}`)
		visibleDoc := rt.PutDoc("visibleDoc", `{"channels":["`+grantedChan+`"]}`)
		rt.WaitForPendingChanges()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: username})
		defer client.Close()

		userWaiter := rt.NewUserWaiter(username)
		updateUserChannels(rt, username, []string{grantedChan, deferredChan}, nil)
		db.WaitForUserWaiterChange(t, userWaiter)

		// A one-shot pull only sends what the cache has already seen
		rt.WaitForPendingChanges()

		btcRunner.StartOneshotPull(client.id)
		btcRunner.WaitForVersion(client.id, "visibleDoc", visibleDoc)
		btcRunner.WaitForVersion(client.id, "deferredDoc", deferredDoc)
	})
}

// TestBlipOneShotPullGrantMidConnection is the one-shot counterpart to the continuous test: the
// connection is established and used before the grant, so the second pull's feed initialisation runs
// against a BlipSyncContext whose user must have been refreshed by refreshUser on the inbound
// subChanges message. A stale user here means the second pull silently returns nothing new.
func TestBlipOneShotPullGrantMidConnection(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const (
			username     = "alice"
			grantedChan  = "chan1"
			deferredChan = "chan2"
		)
		rt.CreateUser(username, []string{grantedChan})

		deferredDoc := rt.PutDoc("deferredDoc", `{"channels":["`+deferredChan+`"]}`)
		visibleDoc := rt.PutDoc("visibleDoc", `{"channels":["`+grantedChan+`"]}`)
		rt.WaitForPendingChanges()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: username})
		defer client.Close()

		// First pull on the pre-grant user, so the connection is live and its user is cached
		btcRunner.StartOneshotPull(client.id)
		btcRunner.WaitForVersion(client.id, "visibleDoc", visibleDoc)
		_, found := btcRunner.GetVersion(client.id, "deferredDoc", deferredDoc)
		require.False(t, found, "document in an ungranted channel was replicated before the grant")

		userWaiter := rt.NewUserWaiter(username)
		updateUserChannels(rt, username, []string{grantedChan, deferredChan}, nil)
		db.WaitForUserWaiterChange(t, userWaiter)
		rt.WaitForPendingChanges()

		// The subChanges for this pull is an inbound BLIP message, so it runs refreshUser first
		btcRunner.StartOneshotPull(client.id)
		btcRunner.WaitForVersion(client.id, "deferredDoc", deferredDoc)
	})
}
