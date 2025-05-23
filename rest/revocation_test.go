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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ChannelRevocationTester struct {
	restTester *RestTester
	test       testing.TB

	fillerDocVersion   DocVersion
	roleVersion        DocVersion
	roleChannelVersion DocVersion
	userChannelVersion DocVersion

	roles        UserRolesTemp
	roleChannels ChannelsTemp
	userChannels ChannelsTemp
}

const (
	revocationTestRole = "foo"
	revocationTestUser = "user"
)

func (tester *ChannelRevocationTester) addRole(user, role string) {
	if tester.roles.Roles == nil {
		tester.roles.Roles = map[string][]string{}
	}

	tester.roles.Roles[user] = append(tester.roles.Roles[user], fmt.Sprintf("role:%s", role))
	tester.roleVersion = tester.restTester.UpdateDoc("userRoles", tester.roleVersion, string(base.MustJSONMarshal(tester.test, tester.roles)))
	tester.restTester.WaitForPendingChanges()
}

func (tester *ChannelRevocationTester) removeRole(user, role string) {
	delIdx := -1
	roles := tester.roles.Roles[user]
	for idx, val := range roles {
		if val == fmt.Sprintf("role:%s", role) {
			delIdx = idx
			break
		}
	}
	tester.roles.Roles[revocationTestUser] = append(roles[:delIdx], roles[delIdx+1:]...)
	tester.roleVersion = tester.restTester.UpdateDoc("userRoles", tester.roleVersion, string(base.MustJSONMarshal(tester.test, tester.roles)))
	tester.restTester.WaitForPendingChanges()
}

// addRoleChannel grants a channel for the default collection to a role
func (tester *ChannelRevocationTester) addRoleChannel(role, channel string) {
	if tester.roleChannels.Channels == nil {
		tester.roleChannels.Channels = map[string][]string{}
	}

	role = fmt.Sprintf("role:%s", role)

	tester.roleChannels.Channels[role] = append(tester.roleChannels.Channels[role], channel)
	tester.roleChannelVersion = tester.restTester.UpdateDoc("roleChannels", tester.roleChannelVersion, string(base.MustJSONMarshal(tester.test, tester.roleChannels)))
	tester.restTester.WaitForPendingChanges()
}

func (tester *ChannelRevocationTester) removeRoleChannel(role, channel string) {
	delIdx := -1
	role = fmt.Sprintf("role:%s", role)
	channelsSlice := tester.roleChannels.Channels[role]
	for idx, val := range channelsSlice {
		if val == channel {
			delIdx = idx
			break
		}
	}
	tester.roleChannels.Channels[role] = append(channelsSlice[:delIdx], channelsSlice[delIdx+1:]...)
	tester.roleChannelVersion = tester.restTester.UpdateDoc("roleChannels", tester.roleChannelVersion, string(base.MustJSONMarshal(tester.test, tester.roleChannels)))
	tester.restTester.WaitForPendingChanges()
}

func (tester *ChannelRevocationTester) addUserChannel(user, channel string) {
	if tester.userChannels.Channels == nil {
		tester.userChannels.Channels = map[string][]string{}
	}

	tester.userChannels.Channels[user] = append(tester.userChannels.Channels[user], channel)
	tester.userChannelVersion = tester.restTester.UpdateDoc("userChannels", tester.userChannelVersion, string(base.MustJSONMarshal(tester.test, tester.userChannels)))
	tester.restTester.WaitForPendingChanges()
}

func (tester *ChannelRevocationTester) removeUserChannel(user string, channel string) {
	delIdx := -1
	channelsSlice := tester.userChannels.Channels[user]
	for idx, val := range channelsSlice {
		if val == channel {
			delIdx = idx
			break
		}
	}
	tester.userChannels.Channels[revocationTestUser] = append(channelsSlice[:delIdx], channelsSlice[delIdx+1:]...)
	tester.userChannelVersion = tester.restTester.UpdateDoc("userChannels", tester.userChannelVersion, string(base.MustJSONMarshal(tester.test, tester.userChannels)))
	tester.restTester.WaitForPendingChanges()
}

// fillToSeq writes filler documents (with empty bodies) for each sequence between the current db sequence and the requested sequence
func (tester *ChannelRevocationTester) fillToSeq(seq uint64) {
	ctx := base.DatabaseLogCtx(base.TestCtx(tester.test), tester.restTester.GetDatabase().Name, nil)
	currentSeq, err := tester.restTester.GetDatabase().LastSequence(ctx)
	require.NoError(tester.test, err)

	loopCount := seq - currentSeq
	for i := 0; i < int(loopCount); i++ {
		tester.fillerDocVersion = tester.restTester.UpdateDoc("fillerDoc", tester.fillerDocVersion, "{}")
	}
}

func (tester *ChannelRevocationTester) getChanges(sinceSeq interface{}, expectedLength int) ChangesResults {
	var changes ChangesResults

	// Ensure any previous mutations have caught up before issuing changes request
	tester.restTester.WaitForPendingChanges()

	err := tester.restTester.WaitForCondition(func() bool {
		changes = tester.restTester.GetChanges(fmt.Sprintf("/{{.keyspace}}/_changes?since=%v&revocations=true", sinceSeq), "user")
		return len(changes.Results) == expectedLength
	})
	require.NoError(tester.test, err, fmt.Sprintf("Unexpected: %d. Expected %d", len(changes.Results), expectedLength))
	return changes
}

func InitScenario(t testing.TB, rtConfig *RestTesterConfig) (ChannelRevocationTester, *RestTester) {

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
		test:       rt.TB(),
		restTester: rt,
	}

	rt.CreateUser(revocationTestUser, nil)

	resp := rt.SendAdminRequest("PUT", "/{{.db}}/_role/foo", `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	return revocationTester, rt
}

func TestRevocationScenario1(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(40)

	changes = revocationTester.getChanges(25, 0)
	assert.Equal(t, "25", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(40, 1)
	assert.Equal(t, "75:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario2(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(50)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario3(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(60)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario4(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(70)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "55:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario5(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "75:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario6(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(90)

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "55:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(90, 0)
	assert.Equal(t, "90", changes.Last_Seq.String())
}

func TestRevocationScenario7(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(100)

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(100, 0)
	assert.Equal(t, "100", changes.Last_Seq.String())
}

func TestRevocationScenario8(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(50)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(50, 0)
	assert.Equal(t, "50", changes.Last_Seq.String())
}

func TestRevocationScenario9(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(60)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(60, 0)
	assert.Equal(t, "60", changes.Last_Seq.String())
}

func TestRevocationScenario10(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(70)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(70, 0)
	assert.Equal(t, "70", changes.Last_Seq.String())
}

func TestRevocationScenario11(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(80)
	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "75:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationScenario12(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(90)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(90, 0)
	assert.Equal(t, "90", changes.Last_Seq.String())
}

func TestRevocationScenario13(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(100)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(100, 0)
	assert.Equal(t, "100", changes.Last_Seq.String())
}

func TestRevocationScenario14(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "ch1"}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.fillToSeq(4)
	revocationTester.addRoleChannel("foo", "ch1")

	changes := revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq.String())

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(25)
	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq.String())

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestEnsureRevocationAfterDocMutation(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	// Give role access to channel A and give user access to role
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 Create doc channel A
	revocationTester.fillToSeq(4)
	const docID = "doc"
	version := rt.PutDoc(docID, `{"channels": "A"}`)

	// Skip to seq 10 then do pull since 4 to get doc
	revocationTester.fillToSeq(10)
	changes := revocationTester.getChanges(4, 1)
	assert.Equal(t, "5", changes.Last_Seq.String())

	// Skip to seq 14 then revoke role from user
	revocationTester.fillToSeq(14)
	revocationTester.removeRole("user", "foo")

	// Skip to seq 19 and then update doc foo
	revocationTester.fillToSeq(19)
	_ = rt.UpdateDoc(docID, version, `{"channels": "A"}`)
	rt.WaitForPendingChanges()

	// Get changes and ensure doc is revoked through ID-only revocation
	changes = revocationTester.getChanges(10, 1)
	assert.Equal(t, "15:20", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestEnsureRevocationUsingDocHistory(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	// Give role access to channel A and give user access to role
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 Create doc channel A
	revocationTester.fillToSeq(4)
	const docID = "doc"
	version := rt.PutDoc(docID, `{"channels": "A"}`)

	// Do pull to get doc
	changes := revocationTester.getChanges(4, 1)
	assert.Equal(t, "5", changes.Last_Seq.String())

	// Revoke channel from role at seq 8
	revocationTester.fillToSeq(7)
	revocationTester.removeRoleChannel("foo", "A")

	// Remove doc from A and re-add
	version = rt.UpdateDoc(docID, version, "{}")
	_ = rt.UpdateDoc(docID, version, `{"channels": "A"}`)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "8:10", changes.Last_Seq.String())
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithAdminChannels(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	rt.CreateUser("user", []string{"A"})

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", false)

	assert.Equal(t, "doc", changes.Results[1].ID)
	assert.False(t, changes.Results[0].Revoked)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "", "letmein", "", dataStore, []string{}, nil))
	RequireStatus(t, resp, http.StatusOK)

	changes = rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d&revocations=true", 2), "user", false)

	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithAdminRoles(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_role/role", GetRolePayload(t, "", rt.GetSingleDataStore(), []string{"A"}))
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_roles": ["role"], "password": "letmein"}`)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", false)

	assert.Equal(t, "doc", changes.Results[1].ID)
	assert.False(t, changes.Results[1].Revoked)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_roles": []}`)
	RequireStatus(t, resp, http.StatusOK)

	changes = rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d&revocations=true", 3), "user", false)

	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationMutationMovesIntoRevokedChannel(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "A")

	revocationTester.fillToSeq(4)
	const docID = "doc"
	const doc2ID = "doc2"
	docVersion := rt.PutDoc(docID, `{"channels": []}`)
	doc2Version := rt.PutDoc(doc2ID, `{"channels": ["A"]}`)

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)
	assert.Equal(t, doc2ID, changes.Results[1].ID)

	revocationTester.removeRole("user", "foo")
	_ = rt.UpdateDoc(docID, docVersion, `{"channels": ["A"]}`)
	_ = rt.UpdateDoc(doc2ID, doc2Version, `{"channels": ["A"], "val": "mutate"}`)

	changes = revocationTester.getChanges(6, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, doc2ID, changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationResumeAndLowSeqCheck(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_role/foo2", `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.addRole("user", "foo2")
	revocationTester.addRoleChannel("foo2", "ch2")

	revocationTester.fillToSeq(9)
	const doc1ID = "doc1"
	const doc2ID = "doc2"
	doc1Version := rt.PutDoc(doc1ID, `{"channels": ["ch1"]}`)
	doc2Version := rt.PutDoc(doc2ID, `{"channels": ["ch2"]}`)

	changes := revocationTester.getChanges(0, 3)

	revocationTester.fillToSeq(19)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(29)
	revocationTester.removeRoleChannel("foo2", "ch2")

	revocationTester.fillToSeq(39)
	doc1Version = rt.UpdateDoc(doc1ID, doc1Version, `{"channels": ["ch1"], "val": "mutate"}`)

	revocationTester.fillToSeq(49)
	doc2Version = rt.UpdateDoc(doc2ID, doc2Version, `{"channels": ["ch2"], "val": "mutate"}`)

	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Equal(t, doc1ID, changes.Results[0].ID)
	assert.Equal(t, doc1Version.RevID, changes.Results[0].Changes[0]["rev"])
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, doc2ID, changes.Results[1].ID)
	assert.Equal(t, doc2Version.RevID, changes.Results[1].Changes[0]["rev"])
	assert.True(t, changes.Results[1].Revoked)

	changes = revocationTester.getChanges("20:40", 1)
	assert.Equal(t, doc2ID, changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	// Check no results with 60
	changes = revocationTester.getChanges("60", 0)

	// Ensure 11 low sequence means we get revocations from that far back
	changes = revocationTester.getChanges("20:0:60", 1)
	assert.Equal(t, doc2ID, changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

}

func TestRevocationResumeSameRoleAndLowSeqCheck(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.addRoleChannel("foo", "ch2")

	revocationTester.fillToSeq(9)
	const doc1ID = "doc1"
	const doc2ID = "doc2"
	doc1Version := rt.PutDoc(doc1ID, `{"channels": ["ch1"]}`)
	doc2Version := rt.PutDoc(doc2ID, `{"channels": ["ch2"]}`)

	changes := revocationTester.getChanges(0, 3)

	revocationTester.fillToSeq(19)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(29)
	revocationTester.removeRoleChannel("foo", "ch2")

	revocationTester.fillToSeq(39)
	_ = rt.UpdateDoc(doc1ID, doc1Version, `{"channels": ["ch1"], "val": "mutate"}`)

	revocationTester.fillToSeq(49)
	_ = rt.UpdateDoc(doc2ID, doc2Version, `{"channels": ["ch2"], "val": "mutate"}`)

	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Equal(t, doc1ID, changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, doc2ID, changes.Results[1].ID)
	assert.True(t, changes.Results[1].Revoked)

	changes = revocationTester.getChanges("20:40", 1)
	assert.Equal(t, doc2ID, changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	// Check no results with 60
	changes = revocationTester.getChanges("60", 0)

	// Ensure 11 low sequence means we get revocations from that far back
	changes = revocationTester.getChanges("11:0:60", 2)
	assert.Equal(t, doc1ID, changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, doc2ID, changes.Results[1].ID)
	assert.True(t, changes.Results[1].Revoked)
}

func TestRevocationsWithQueryLimit(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.Ptr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					MaxItemCount: base.Ptr(uint32(0)),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.Ptr(0),
				},
			},
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(9)
	_ = rt.PutDoc("doc1", `{"channels": ["ch1"]}`)
	_ = rt.PutDoc("doc2", `{"channels": ["ch1"]}`)
	_ = rt.PutDoc("doc3", `{"channels": ["ch1"]}`)

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	// Run changes once (which has its own wait)
	sinceVal := changes.Last_Seq
	changes = revocationTester.getChanges(sinceVal, 3)

	var queryKey string
	if base.TestsDisableGSI() {
		queryKey = fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncGateway(), db.ViewChannels)
	} else {
		queryKey = db.QueryTypeChannels
	}

	// Once we know the changes will return to right count run again to validate queries ran
	channelQueryCountBefore := rt.GetDatabase().DbStats.Query(queryKey).QueryCount.Value()
	changes = revocationTester.getChanges(sinceVal, 3)
	channelQueryCountAfter := rt.GetDatabase().DbStats.Query(queryKey).QueryCount.Value()

	assert.Equal(t, int64(3), channelQueryCountAfter-channelQueryCountBefore)
}

func TestRevocationsWithQueryLimit2Channels(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	revocationTester, rt := InitScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:           false,
			QueryPaginationLimit: base.Ptr(2),
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.addUserChannel("user", "ch2")

	revocationTester.fillToSeq(9)
	_ = rt.PutDoc("doc1", `{"channels": ["ch1", "ch2"]}`)
	_ = rt.PutDoc("doc2", `{"channels": ["ch1", "ch2"]}`)
	_ = rt.PutDoc("doc3", `{"channels": ["ch1", "ch2"]}`)

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	_ = changes.Last_Seq
	// This changes feed would loop if CBG-3273 was still an issue
	changes = revocationTester.getChanges(0, 4)

	// Get one of the 3 docs which the user should still have access to
	resp := rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", nil, "user", RestTesterDefaultUserPassword)
	RequireStatus(t, resp, 200)

	// Revoke access to ch2
	revocationTester.removeUserChannel("user", "ch2")
	changes = revocationTester.getChanges(0, 7)
	// Get a doc to ensure no access
	resp = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", nil, "user", RestTesterDefaultUserPassword)
	RequireStatus(t, resp, 403)
}

func TestRevocationsWithQueryLimitChangesLimit(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.Ptr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					MaxItemCount: base.Ptr(uint32(0)),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.Ptr(0),
				},
			},
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(9)
	_ = rt.PutDoc("doc1", `{"channels": ["ch1"]}`)
	_ = rt.PutDoc("doc2", `{"channels": ["ch1"]}`)
	_ = rt.PutDoc("doc3", `{"channels": ["ch1"]}`)

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	rt.WaitForPendingChanges()
	waitForUserChangesWithLimit := func(sinceVal interface{}, limit int) ChangesResults {
		var changesRes ChangesResults
		err := rt.WaitForCondition(func() bool {
			changesRes = rt.GetChanges(fmt.Sprintf("/{{.keyspace}}/_changes?since=%v&revocations=true&limit=%d", sinceVal, limit), "user")
			return len(changesRes.Results) == limit
		})
		assert.NoError(t, err)
		return changesRes
	}

	sinceVal := changes.Last_Seq
	changes = waitForUserChangesWithLimit(sinceVal, 2)
	assert.Len(t, changes.Results, 2)

	changes = waitForUserChangesWithLimit(sinceVal, 3)
	assert.Len(t, changes.Results, 3)
}

func TestRevocationUserHasDocAccessDocNotFound(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency when running in integration")
	}

	revocationTester, rt := InitScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			QueryPaginationLimit: base.Ptr(2),
			CacheConfig: &CacheConfig{
				RevCacheConfig: &RevCacheConfig{
					MaxItemCount: base.Ptr(uint32(0)),
				},
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxNumber: base.Ptr(0),
				},
			},
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "A")

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeRoleChannel("foo", "A")
	rt.WaitForPendingChanges()

	leakyDataStore, ok := base.AsLeakyDataStore(rt.GetSingleDataStore())
	require.True(t, ok)
	leakyDataStore.SetGetRawCallback(func(s string) error {
		require.NoError(t, leakyDataStore.Delete("doc"))
		return nil
	})

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	require.Len(t, changes.Results, 1)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, changes.Results[0].ID, "doc")
}

// Test does not directly run wasDocInChannelAtSeq but aims to test this through performing revocation operations
// that will hit the various cases that wasDocInChannelAtSeq will handle
func TestWasDocInChannelAtSeq(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "a")
	revocationTester.addRoleChannel("foo", "c")

	const docID = "doc"
	version := rt.PutDoc(docID, `{"channels": ["a"]}`)

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeRoleChannel("foo", "a")
	revocationTester.removeRoleChannel("foo", "c")

	_ = rt.UpdateDoc(docID, version, `{"channels": []}`)
	_ = rt.PutDoc("doc2", `{"channels": ["b", "a"]}`)

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addRoleChannel("foo", "c")
	const doc3ID = "doc3"
	doc3Version := rt.PutDoc(doc3ID, `{"channels": ["c"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.removeRoleChannel("foo", "c")
	_ = rt.UpdateDoc(doc3ID, doc3Version, `{"channels": ["c"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
}

// Test does not directly run channelGrantedPeriods but aims to test this through performing revocation operations
// that will hit the various cases that channelGrantedPeriods will handle
func TestChannelGrantedPeriods(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	revocationTester.addUserChannel("user", "a")
	const docID = "doc"
	version := rt.PutDoc(docID, `{"channels": ["a"]}`)
	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeUserChannel("user", "a")
	version = rt.UpdateDoc(docID, version, `{"mutate": "mutate", "channels": ["a"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addUserChannel("user", "a")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.removeUserChannel("user", "a")
	version = rt.UpdateDoc(docID, version, `{"mutate": "mutate2", "channels": ["a"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addUserChannel("user", "a")
	version = rt.UpdateDoc(docID, version, `{"mutate": "mutate3", "channels": ["a"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "b")
	version = rt.UpdateDoc(docID, version, `{"channels": ["b"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)

	revocationTester.removeRoleChannel("foo", "b")
	revocationTester.removeRole("user", "foo")
	_ = rt.UpdateDoc(docID, version, `{"mutate": "mutate", "channels": ["b"]}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
}

func TestChannelHistoryPruning(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()
	c := rt.GetSingleDataStore().CollectionName()
	s := rt.GetSingleDataStore().ScopeName()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "a")

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": ["a"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	// Enter a load of history by looping over adding and removing a channel. Needs a get changes in there to trigger
	// the actual rebuild
	var changes ChangesResults
	for i := 0; i < 20; i++ {
		changes = revocationTester.getChanges(0, 2)
		assert.Len(t, changes.Results, 2)
		revocationTester.removeRoleChannel("foo", "a")
		changes = revocationTester.getChanges(0, 2)
		assert.Len(t, changes.Results, 2)
		revocationTester.addRoleChannel("foo", "a")
	}

	// Validate history is pruned properly with first entry merged to span a wider period
	authenticator := rt.GetDatabase().Authenticator(base.TestCtx(t))
	role, err := authenticator.GetRole("foo")
	assert.NoError(t, err)
	require.Contains(t, role.CollectionChannelHistory(s, c), "a")
	require.Len(t, role.CollectionChannelHistory(s, c)["a"].Entries, 10)
	assert.Equal(t, role.CollectionChannelHistory(s, c)["a"].Entries[0], auth.GrantHistorySequencePair{StartSeq: 4, EndSeq: 26})

	// Add an additional channel to ensure only the latter one is pruned
	revocationTester.addRoleChannel("foo", "b")
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels": ["b"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Len(t, changes.Results, 2)
	revocationTester.removeRoleChannel("foo", "b")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	// Override role history with a unix time older than 30 days. Means next time rebuildRoles is ran we will end up
	// pruning those entries.
	role, err = authenticator.GetRole("foo")
	assert.NoError(t, err)
	channelHistory := role.CollectionChannelHistory(s, c)
	aHistory := channelHistory["a"]
	aHistory.UpdatedAt = time.Now().Add(-31 * time.Hour * 24).Unix()
	channelHistory["a"] = aHistory

	role.SetChannelHistory(channelHistory)
	err = authenticator.Save(role)
	assert.NoError(t, err)

	// Add another so we have something to wait on
	revocationTester.addRoleChannel("foo", "random")
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels": ["random"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	role, err = authenticator.GetRole("foo")
	assert.NoError(t, err)

	assert.NotContains(t, role.CollectionChannelHistory(s, c), "a")
	assert.Contains(t, role.CollectionChannelHistory(s, c), "b")
}

func TestChannelRevocationWithContiguousSequences(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	revocationTester.addUserChannel("user", "a")
	const docID = "doc"
	version := rt.PutDoc(docID, `{"channels": "a"}`)
	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeUserChannel("user", "a")
	version = rt.UpdateDoc(docID, version, `{"mutate": "mutate", "channels": "a"}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.addUserChannel("user", "a")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.False(t, changes.Results[0].Revoked)

	revocationTester.removeUserChannel("user", "a")
	_ = rt.UpdateDoc(docID, version, `{"mutate": "mutate2", "channels": "a"}`)
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithUserXattrs(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	xattrKey := "channelInfo"

	revocationTester, rt := InitScenario(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   true,
			UserXattrKey: &xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (doc._id === 'accessDoc' && meta.xattrs.channelInfo !== undefined){
					for (var key in meta.xattrs.channelInfo.userChannels){
						access(key, meta.xattrs.channelInfo.userChannels[key]);
					}
				}
				if (doc._id.indexOf("doc") >= 0){
					channel(doc.channels);
				}
			}`,
	})

	defer rt.Close()
	data := rt.GetSingleDataStore()

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/accessDoc", `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	ctx := rt.Context()

	cas, err := data.Get("accessDoc", nil)
	require.NoError(t, err)

	_, err = data.UpdateXattrs(ctx, "accessDoc", 0, cas, map[string][]byte{xattrKey: []byte(`{"userChannels" : {"user": "a"}}`)}, nil)
	require.NoError(t, err)

	_ = rt.PutDoc("doc", `{"channels": "a"}`)

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	cas, err = data.Get("accessDoc", nil)
	require.NoError(t, err)

	require.NoError(t, data.RemoveXattrs(ctx, "accessDoc", []string{xattrKey}, cas))

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
}

func TestReplicatorRevocations(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	// Passive
	revocationTester, rt2 := InitScenario(t, nil)
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "chanA")

	resp := rt2.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels": "chanA"}`)
	RequireStatus(t, resp, http.StatusCreated)

	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", RestTesterDefaultUserPassword)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          false,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	resp = rt1.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)

	revocationTester.removeRole("user", "foo")

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	resp = rt1.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusNotFound)
}

func TestReplicatorRevocationsNoRev(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	// Passive
	revocationTester, rt2 := InitScenario(t, nil)
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "chanA")

	const doc1ID = "doc1"
	doc1Version := rt2.PutDoc(doc1ID, `{"channels": "chanA"}`)
	rt2.WaitForPendingChanges()

	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", RestTesterDefaultUserPassword)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          false,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)

	revocationTester.removeRole("user", "foo")

	_ = rt2.UpdateDoc(doc1ID, doc1Version, `{"channels": "chanA", "mutate": "val"}`)
	rt2.WaitForPendingChanges()

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	resp = rt1.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusNotFound)
}

func TestReplicatorRevocationsNoRevButAlternateAccess(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	// Passive
	revocationTester, rt2 := InitScenario(t, nil)
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "chanA")

	resp := rt2.SendAdminRequest("PUT", "/db/_role/foo2", `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	revocationTester.addRole("user", "foo2")
	revocationTester.addRoleChannel("foo2", "chanB")

	const doc1ID = "doc1"
	doc1Version := rt2.PutDoc(doc1ID, `{"channels": ["chanA", "chanB"]}`)
	rt2.WaitForPendingChanges()

	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", RestTesterDefaultUserPassword)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          false,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	resp = rt1.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)

	revocationTester.removeRole("user", "foo")

	_ = rt2.UpdateDoc(doc1ID, doc1Version, `{"channels": ["chanA", "chanB"], "mutate": "val"}`)
	rt2.WaitForPendingChanges()

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	resp = rt1.SendAdminRequest("GET", "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)
}

func TestReplicatorRevocationsMultipleAlternateAccess(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll) // CBG-1981

	// Passive
	revocationTester, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2ds := rt2.GetSingleDataStore()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			DatabaseConfig: &DatabaseConfig{
				DbConfig: DbConfig{
					Name: "active",
				},
			},
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/" + rt2.GetDatabase().Name)
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword(revocationTestUser, RestTesterDefaultUserPassword)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	revocationTester.addRole(revocationTestUser, revocationTestRole)
	rt2.WaitForPendingChanges()

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)
	require.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	// perform role grant to allow for all channels
	resp := rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", rt2ds, []string{"A", "B", "C"}))

	_ = rt2.PutDoc("docA", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusOK)
	_ = rt2.PutDoc("docAB", `{"channels": ["A", "B"]}`)
	RequireStatus(t, resp, http.StatusOK)
	_ = rt2.PutDoc("docB", `{"channels": ["B"]}`)
	RequireStatus(t, resp, http.StatusOK)
	_ = rt2.PutDoc("docABC", `{"channels": ["A", "B", "C"]}`)
	RequireStatus(t, resp, http.StatusOK)
	_ = rt2.PutDoc("docC", `{"channels": ["C"]}`)
	RequireStatus(t, resp, http.StatusOK)

	// Wait for docs to turn up on local / rt1
	rt1.WaitForChanges(5, "/{{.keyspace}}/_changes?since=0", "", true)

	// Revoke C and ensure docC gets purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", rt2ds, []string{"A", "B"}))
	RequireStatus(t, resp, http.StatusOK)
	rt2.WaitForPendingChanges()

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docC", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

	// Revoke B and ensure docB gets purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", rt2ds, []string{"A"}))
	RequireStatus(t, resp, http.StatusOK)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docB", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

	// Revoke A and ensure docA, docAB, docABC gets purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", rt2ds, []string{}))
	RequireStatus(t, resp, http.StatusOK)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docAB", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docABC", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

}

func TestReplicatorRevocationsWithTombstoneResurrection(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	base.RequireNumTestBuckets(t, 2)

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2ds := rt2.GetSingleDataStore()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2ds, []string{"A", "B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", "letmein")
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	const docAID = "docA"
	const docA1ID = "docA1"
	docAVersion := rt2.PutDoc(docAID, `{"channels": ["A"]}`)
	docA1Version := rt2.PutDoc(docA1ID, `{"channels": ["A"]}`)
	_ = rt2.PutDoc("docA2", `{"channels": ["A"]}`)

	_ = rt2.PutDoc("docB", `{"channels": ["B"]}`)

	require.NoError(t, ar.Start(ctx1))

	rt1.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)

	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	rt2.DeleteDoc(docAID, docAVersion)
	rt2.DeleteDoc(docA1ID, docA1Version)

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2ds, []string{"B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	require.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA1", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA2", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)
}

func TestReplicatorRevocationsWithChannelFilter(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2ds := rt2.GetSingleDataStore()

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	const (
		username = "user"
		password = RestTesterDefaultUserPassword
	)

	passiveDBURL.User = url.UserPassword(username, password)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	_ = rt2.PutDoc("docA", `{"channels": ["ABC"]}`)
	rt2.WaitForPendingChanges()

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          false,
		FilterChannels:      []string{"ABC"},
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	resp := rt2.SendAdminRequest("PUT", "/{{.db}}/_user/user", GetUserPayload(t, username, password, "", rt2ds, []string{"ABC"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	rt2.WaitForPendingChanges()
	require.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	// Wait for docs to turn up on local / rt1
	rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)

	// Revoke A and ensure ABC chanel access and ensure DocA is purged from local
	resp = rt2.SendAdminRequest("PUT", "/{{.db}}/_user/user", GetUserPayload(t, username, password, "", rt2ds, []string{}, nil))
	RequireStatus(t, resp, http.StatusOK)

	require.NoError(t, ar.Stop())

	require.NoError(t, ar.Start(ctx1))

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)
}

func TestReplicatorRevocationsWithStarChannel(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll) // CBG-1981

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2ds := rt2.GetSingleDataStore()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", RestTesterDefaultUserPassword)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	_ = rt2.PutDoc("docA", `{"channels": ["A"]}`)
	_ = rt2.PutDoc("docAB", `{"channels": ["A","B"]}`)
	_ = rt2.PutDoc("docB", `{"channels": ["B"]}`)
	_ = rt2.PutDoc("docABC", `{"channels": ["A","B", "C"]}`)
	_ = rt2.PutDoc("docC", `{"channels": ["C"]}`)
	rt2.WaitForPendingChanges()

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          false,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", RestTesterDefaultUserPassword, "", rt2ds, []string{"*"}, nil))
	RequireStatus(t, resp, http.StatusOK)
	rt2.WaitForPendingChanges()

	require.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	// Wait for docs to turn up on local / rt1
	rt1.WaitForChanges(5, "/{{.keyspace}}/_changes?since=0", "", true)

	// Revoke A and ensure docA, docAB, docABC get purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", RestTesterDefaultUserPassword, "", rt2ds, []string{}, nil))
	RequireStatus(t, resp, http.StatusOK)

	assert.NoError(t, ar.Stop())

	require.NoError(t, ar.Start(ctx1))

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docAB", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docABC", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)
}

func TestReplicatorRevocationsFromZero(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	base.RequireNumTestBuckets(t, 2)

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2ds := rt2.GetSingleDataStore()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2ds, []string{"A", "B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", "letmein")
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	activeReplCfg := &db.ActiveReplicatorConfig{
		ID:          strings.ReplaceAll(t.Name(), "/", ""),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          false,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	}

	ar, err := db.NewActiveReplicator(ctx1, activeReplCfg)
	require.NoError(t, err)

	_ = rt2.PutDoc("docA", `{"channels": ["A"]}`)
	_ = rt2.PutDoc("docA1", `{"channels": ["A"]}`)
	_ = rt2.PutDoc("docA2", `{"channels": ["A"]}`)
	rt2.WaitForPendingChanges()

	require.NoError(t, ar.Start(ctx1))

	rt1.WaitForChanges(3, "/{{.keyspace}}/_changes?since=0", "", true)

	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	// Be sure docs have arrived
	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA", "")
		return resp.Code == http.StatusOK
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA1", "")
		return resp.Code == http.StatusOK
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA2", "")
		return resp.Code == http.StatusOK
	})
	assert.NoError(t, err)

	// Reset checkpoint (since 0)
	require.NoError(t, ar.Reset())

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2ds, []string{"B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	require.NoError(t, ar.Start(ctx1))

	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA1", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docA2", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, err)
}

func TestRevocationMessage(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		revocationTester, rt := InitScenario(t, nil)
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "user",
			Channels:               []string{"*"},
			ClientDeltas:           false,
			SendRevocations:        true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer btc.Close()

		// Add channel to role and role to user
		revocationTester.addRoleChannel("foo", "A")
		revocationTester.addRole("user", "foo")

		// Skip to seq 4 and then create doc in channel A
		revocationTester.fillToSeq(4)
		version := rt.PutDoc("doc", `{"channels": "A"}`)

		// Start pull
		rt.WaitForPendingChanges()
		btcRunner.StartOneshotPull(btc.id)

		// Wait for doc revision to come over
		_ = btcRunner.WaitForBlipRevMessage(btc.id, "doc", version)

		// Remove role from user
		revocationTester.removeRole("user", "foo")

		const doc1ID = "doc1"
		version = rt.PutDoc(doc1ID, `{"channels": "!"}`)

		revocationTester.fillToSeq(10)
		version = rt.UpdateDoc(doc1ID, version, "{}")

		// Start a pull since 5 to receive revocation and removal
		rt.WaitForPendingChanges()
		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Continuous: false, Since: "5"})

		// Wait for doc1 rev2 - This is the last rev we expect so we can be sure replication is complete here
		_ = btcRunner.WaitForVersion(btc.id, doc1ID, version)

		messages := btc.pullReplication.GetMessages()

		testCases := []struct {
			Name            string
			DocID           string
			ExpectedDeleted int64
		}{
			{
				Name:            "Revocation",
				DocID:           "doc",
				ExpectedDeleted: int64(2),
			},
			{
				Name:            "Removed",
				DocID:           "doc1",
				ExpectedDeleted: int64(4),
			},
		}

		for _, testCase := range testCases {
			rt.Run(testCase.Name, func(t *testing.T) {
				// Verify the deleted property in the changes message is "2" this indicated a revocation
				for _, msg := range messages {
					if msg.Properties[db.BlipProfile] == db.MessageChanges {
						var changesMessages [][]interface{}
						err := msg.ReadJSONBody(&changesMessages)
						if err != nil {
							continue
						}

						if len(changesMessages) != 2 || len(changesMessages[0]) != 4 {
							continue
						}

						criteriaMet := false
						for _, changesMessage := range changesMessages {
							castedNum, ok := changesMessage[3].(json.Number)
							if !ok {
								continue
							}
							intDeleted, err := castedNum.Int64()
							if err != nil {
								continue
							}
							if docName, ok := changesMessage[1].(string); ok && docName == testCase.DocID && intDeleted == testCase.ExpectedDeleted {
								criteriaMet = true
								break
							}
						}

						assert.True(t, criteriaMet)
					}
				}
			})
		}

	})
}

func TestRevocationNoRev(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc"
	const waitMarkerID = "docmarker"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		revocationTester, rt := InitScenario(t, nil)
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "user",
			Channels:               []string{"*"},
			ClientDeltas:           false,
			SendRevocations:        true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer btc.Close()

		// Add channel to role and role to user
		revocationTester.addRoleChannel("foo", "A")
		revocationTester.addRole("user", "foo")

		// Skip to seq 4 and then create doc in channel A
		revocationTester.fillToSeq(4)
		version := rt.PutDoc(docID, `{"channels": "A"}`)
		firstOneShotSinceSeq := rt.GetDocumentSequence("doc")

		// OneShot pull to grab doc
		rt.WaitForPendingChanges()
		btcRunner.StartOneshotPull(btc.id)

		_ = btcRunner.WaitForVersion(btc.id, docID, version)

		// Remove role from user
		revocationTester.removeRole("user", "foo")

		_ = rt.UpdateDoc(docID, version, `{"channels": "A", "val": "mutate"}`)

		waitMarkerVersion := rt.PutDoc(waitMarkerID, `{"channels": "!"}`)
		rt.WaitForPendingChanges()
		lastSeqStr := strconv.FormatUint(firstOneShotSinceSeq, 10)
		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Continuous: false, Since: lastSeqStr})

		_ = btcRunner.WaitForVersion(btc.id, waitMarkerID, waitMarkerVersion)

		changesMsg := btc.getMostRecentChangesMessage()
		var messageBody []interface{}
		require.NoError(t, changesMsg.ReadJSONBody(&messageBody))
		require.Len(t, messageBody, 2)
		require.Len(t, messageBody[0], 4)

		deletedFlag, err := messageBody[0].([]interface{})[3].(json.Number).Int64()
		require.NoError(t, err)

		assert.Equal(t, deletedFlag, int64(2))
	})
}

func TestRevocationGetSyncDataError(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	var throw bool
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc"
	const waitMarkerID = "docmarker"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		// Two callbacks to cover usage with CBS/Xattrs and without
		revocationTester, rt := InitScenario(
			t, &RestTesterConfig{
				LeakyBucketConfig: &base.LeakyBucketConfig{
					GetWithXattrCallback: func(key string) error {
						if throw {
							return fmt.Errorf("Leaky Bucket GetWithXattrCallback Error")
						}
						return nil
					}, GetRawCallback: func(key string) error {
						if throw {
							return fmt.Errorf("Leaky Bucket GetRawCallback Error")
						}
						return nil
					},
				},
			},
		)

		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "user",
			Channels:               []string{"*"},
			ClientDeltas:           false,
			SendRevocations:        true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer btc.Close()

		// Add channel to role and role to user
		revocationTester.addRoleChannel("foo", "A")
		revocationTester.addRole("user", "foo")

		// Skip to seq 4 and then create doc in channel A
		revocationTester.fillToSeq(4)
		version := rt.PutDoc(docID, `{"channels": "A"}}`)

		// OneShot pull to grab doc
		rt.WaitForPendingChanges()
		btcRunner.StartOneshotPull(btc.id)

		firstOneShotSinceSeq := rt.GetDocumentSequence("doc")

		throw = true
		_ = btcRunner.WaitForVersion(btc.id, docID, version)

		// Remove role from user
		revocationTester.removeRole("user", "foo")

		_ = rt.UpdateDoc(docID, version, `{"channels": "A", "val": "mutate"}`)

		waitMarkerVersion := rt.PutDoc(waitMarkerID, `{"channels": "!"}`)

		rt.WaitForPendingChanges()
		lastSeqStr := strconv.FormatUint(firstOneShotSinceSeq, 10)
		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Continuous: false, Since: lastSeqStr})

		_ = btcRunner.WaitForVersion(btc.id, waitMarkerID, waitMarkerVersion)
	})
}

// Regression test for CBG-2183.
func TestBlipRevokeNonExistentRole(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&RestTesterConfig{
				GuestEnabled: false,
			})
		defer rt.Close()

		dataStore := rt.GetSingleDataStore()

		// 1. Create user with admin_roles including two roles not previously defined (a1 and a2, for example)
		res := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_user/bilbo", rt.GetDatabase().Name), GetUserPayload(t, "bilbo", RestTesterDefaultUserPassword, "", dataStore, []string{"c1"}, []string{"a1", "a2"}))
		RequireStatus(t, res, http.StatusCreated)

		// Create a doc so we have something to replicate
		res = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/testdoc", `{"channels": ["c1"]}`)
		RequireStatus(t, res, http.StatusCreated)

		// 3. Update the user to not reference one of the roles (update to ['a1'], for example)
		// [also revoke channel c1 so the doc shows up in the revocation queries]
		res = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_user/bilbo", rt.GetDatabase().Name), GetUserPayload(t, "bilbo", RestTesterDefaultUserPassword, "", dataStore, []string{}, []string{"a1"}))
		RequireStatus(t, res, http.StatusOK)

		// 4. Try to sync
		bt := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:               "bilbo",
			SendRevocations:        true,
			SupportedBLIPProtocols: SupportedBLIPProtocols,
		})
		defer bt.Close()

		btcRunner.StartPull(bt.id)

		// in the failing case we'll panic before hitting this
		base.RequireWaitForStat(t, func() int64 {
			return rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()
		}, 1)
	})
}

func TestReplicatorSwitchPurgeNoReset(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	defer db.SuspendSequenceBatching()()

	base.RequireNumTestBuckets(t, 2)

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2ds := rt2.GetSingleDataStore()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2ds, []string{"A", "B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	// Setup replicator
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", "letmein")
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_ = rt2.PutDoc(fmt.Sprintf("docA%d", i), `{"channels": ["A"]}`)
	}

	for i := 0; i < 7; i++ {
		_ = rt2.PutDoc(fmt.Sprintf("docB%d", i), `{"channels": ["B"]}`)
	}

	rt2.WaitForPendingChanges()

	require.NoError(t, ar.Start(ctx1))

	changesResults := rt1.WaitForChanges(17, "/{{.keyspace}}/_changes?since=0", "", true)

	// Going to stop & start replication between these actions to make out of order seq no's more likely. More likely
	// to hit CBG-1591
	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2ds, []string{"B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	// Add another few docs to 'bump' rt1's seq no. Otherwise it'll end up revoking next time as the above user PUT is
	// not processed by the rt1 receiver.
	for i := 7; i < 15; i++ {
		_ = rt2.PutDoc(fmt.Sprintf("docB%d", i), `{"channels": ["B"]}`)
	}

	rt2.WaitForPendingChanges()

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	changesResults = rt1.WaitForChanges(8, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", changesResults.Last_Seq), "", true)

	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
	sgwStats, err = base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err = sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err = db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  base.TestsUseNamedCollections(),
	})
	require.NoError(t, err)

	// Send a doc to act as a 'marker' so we know when replication has completed
	_ = rt2.PutDoc("docMarker", `{"channels": ["B"]}`)

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	// Validate none of the documents are purged after flipping option
	rt2.WaitForPendingChanges()

	changesResults = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", changesResults.Last_Seq), "", true)

	for i := 0; i < 10; i++ {
		resp = rt1.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/docA%d", i), "")
		RequireStatus(t, resp, http.StatusOK)
	}

	for i := 0; i < 7; i++ {
		resp = rt1.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/docB%d", i), "")
		RequireStatus(t, resp, http.StatusOK)
	}

	// Shutdown replicator to close out
	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
}

func TestRevocationDeletedRole(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	// Add a document in channel a
	const docID = "doc"
	_ = rt.PutDoc(docID, `{"channels": "a"}`)

	// Grant role "foo" channel "a"
	revocationTester.addRoleChannel(revocationTestRole, "a")

	// Grant user "user" role "foo"
	revocationTester.addRole(revocationTestUser, revocationTestRole)

	// Expect two changes - pseudo-user doc and "doc"
	changes := revocationTester.getChanges(0, 2)
	require.Len(t, changes.Results, 2)
	require.Equal(t, "doc", changes.Results[1].ID)

	// Delete the role
	resp := rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.db}}/_role/%s", revocationTestRole), "")
	RequireStatus(t, resp, http.StatusOK)

	// Issue new changes request, confirm revocation for "doc"
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}
