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

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ChannelRevocationTester struct {
	restTester *RestTester
	test       *testing.T

	fillerDocRev   string
	roleRev        string
	roleChannelRev string
	userChannelRev string

	roles        UserRolesTemp
	roleChannels ChannelsTemp
	userChannels ChannelsTemp
}

const (
	revocationTestRole     = "foo"
	revocationTestUser     = "user"
	revocationTestPassword = "test"
)

func (tester *ChannelRevocationTester) addRole(user, role string) {
	if tester.roles.Roles == nil {
		tester.roles.Roles = map[string][]string{}
	}

	tester.roles.Roles[user] = append(tester.roles.Roles[user], fmt.Sprintf("role:%s", role))
	revID := tester.restTester.CreateDocReturnRev(tester.test, "userRoles", tester.roleRev, tester.roles)
	tester.roleRev = revID
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
	tester.roles.Roles[user] = append(roles[:delIdx], roles[delIdx+1:]...)
	tester.roleRev = tester.restTester.CreateDocReturnRev(tester.test, "userRoles", tester.roleRev, tester.roles)
}

func (tester *ChannelRevocationTester) addRoleChannel(role, channel string) {
	if tester.roleChannels.Channels == nil {
		tester.roleChannels.Channels = map[string][]string{}
	}

	role = fmt.Sprintf("role:%s", role)

	tester.roleChannels.Channels[role] = append(tester.roleChannels.Channels[role], channel)
	tester.roleChannelRev = tester.restTester.CreateDocReturnRev(tester.test, "roleChannels", tester.roleChannelRev, tester.roleChannels)
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
	tester.roleChannelRev = tester.restTester.CreateDocReturnRev(tester.test, "roleChannels", tester.roleChannelRev, tester.roleChannels)
}

func (tester *ChannelRevocationTester) addUserChannel(user, channel string) {
	if tester.userChannels.Channels == nil {
		tester.userChannels.Channels = map[string][]string{}
	}

	tester.userChannels.Channels[user] = append(tester.userChannels.Channels[user], channel)
	tester.userChannelRev = tester.restTester.CreateDocReturnRev(tester.test, "userChannels", tester.userChannelRev, tester.userChannels)
}

func (tester *ChannelRevocationTester) removeUserChannel(user, channel string) {
	delIdx := -1
	channelsSlice := tester.userChannels.Channels[user]
	for idx, val := range channelsSlice {
		if val == channel {
			delIdx = idx
			break
		}
	}
	tester.userChannels.Channels[user] = append(channelsSlice[:delIdx], channelsSlice[delIdx+1:]...)
	tester.userChannelRev = tester.restTester.CreateDocReturnRev(tester.test, "userChannels", tester.userChannelRev, tester.userChannels)
}

func (tester *ChannelRevocationTester) fillToSeq(seq uint64) {
	ctx := base.DatabaseLogCtx(base.TestCtx(tester.restTester.TB), tester.restTester.GetDatabase().Name, nil)
	currentSeq, err := tester.restTester.GetDatabase().LastSequence(ctx)
	require.NoError(tester.test, err)

	loopCount := seq - currentSeq
	for i := 0; i < int(loopCount); i++ {
		requestURL := "/{{.keyspace}}/fillerDoc"
		if tester.fillerDocRev != "" {
			requestURL += "?rev=" + tester.fillerDocRev
		}
		resp := tester.restTester.SendAdminRequest("PUT", requestURL, "{}")
		require.Equal(tester.test, http.StatusCreated, resp.Code)

		var body db.Body
		err = json.Unmarshal(resp.BodyBytes(), &body)
		require.NoError(tester.test, err)

		tester.fillerDocRev = body["rev"].(string)
	}
}

func (tester *ChannelRevocationTester) getChanges(sinceSeq interface{}, expectedLength int) ChangesResults {
	var changes ChangesResults

	// Ensure any previous mutations have caught up before issuing changes request
	err := tester.restTester.WaitForPendingChanges()
	assert.NoError(tester.test, err)

	err = tester.restTester.WaitForCondition(func() bool {
		resp := tester.restTester.SendUserRequestWithHeaders("GET", fmt.Sprintf("/{{.keyspace}}/_changes?since=%v&revocations=true", sinceSeq), "", nil, "user", "test")
		require.Equal(tester.test, http.StatusOK, resp.Code)
		err := json.Unmarshal(resp.BodyBytes(), &changes)
		require.NoError(tester.test, err)

		return len(changes.Results) == expectedLength
	})
	require.NoError(tester.test, err, fmt.Sprintf("Unexpected: %d. Expected %d", len(changes.Results), expectedLength))

	err = tester.restTester.WaitForPendingChanges()
	assert.NoError(tester.test, err)

	return changes
}

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

	resp := rt.SendAdminRequest("PUT", "/{{.db}}/_user/user", fmt.Sprintf(`{"name": "%s", "password": "%s"}`, revocationTestUser, revocationTestPassword))
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/{{.db}}/_role/foo", `{}`)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(40)

	changes = revocationTester.getChanges(25, 0)
	assert.Equal(t, "25", changes.Last_Seq)

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
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(50)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(60)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(64)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(70)
	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "55:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(80)

	changes = revocationTester.getChanges(50, 1)
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

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
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

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
	assert.Equal(t, "55:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(90, 0)
	assert.Equal(t, "90", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(25)

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

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
	assert.Equal(t, "45:3", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)

	revocationTester.fillToSeq(110)

	changes = revocationTester.getChanges(100, 0)
	assert.Equal(t, "100", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(50)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

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
	assert.Equal(t, "50", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")
	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")
	revocationTester.fillToSeq(54)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(60)
	changes = revocationTester.getChanges(5, 0)
	assert.Equal(t, "5", changes.Last_Seq)

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
	assert.Equal(t, "60", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(74)
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(70, 0)
	assert.Equal(t, "70", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

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
	assert.Equal(t, "75:3", changes.Last_Seq)

	revocationTester.fillToSeq(84)
	revocationTester.removeRoleChannel("foo", "ch1")
	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(80, 1)
	assert.Equal(t, "85:3", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(94)
	revocationTester.removeRole("user", "foo")

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(90, 0)
	assert.Equal(t, "90", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(110)
	changes = revocationTester.getChanges(100, 0)
	assert.Equal(t, "100", changes.Last_Seq)
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
	assert.Equal(t, "5", changes.Last_Seq)

	revocationTester.fillToSeq(19)
	revocationTester.addRole("user", "foo")

	revocationTester.fillToSeq(25)
	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "20:3", changes.Last_Seq)

	revocationTester.fillToSeq(44)
	revocationTester.removeRole("user", "foo")

	changes = revocationTester.getChanges(25, 1)
	assert.Equal(t, "45:3", changes.Last_Seq)
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
	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	// Skip to seq 10 then do pull since 4 to get doc
	revocationTester.fillToSeq(10)
	changes := revocationTester.getChanges(4, 1)
	assert.Equal(t, "5", changes.Last_Seq)

	// Skip to seq 14 then revoke role from user
	revocationTester.fillToSeq(14)
	revocationTester.removeRole("user", "foo")

	// Skip to seq 19 and then update doc foo
	revocationTester.fillToSeq(19)
	_ = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A"})
	err := rt.WaitForPendingChanges()
	require.NoError(t, err)

	// Get changes and ensure doc is revoked through ID-only revocation
	changes = revocationTester.getChanges(10, 1)
	assert.Equal(t, "15:20", changes.Last_Seq)
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
	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	// Do pull to get doc
	changes := revocationTester.getChanges(4, 1)
	assert.Equal(t, "5", changes.Last_Seq)

	// Revoke channel from role at seq 8
	revocationTester.fillToSeq(7)
	revocationTester.removeRoleChannel("foo", "A")

	// Remove doc from A and re-add
	revID = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{})
	_ = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A"})

	changes = revocationTester.getChanges(5, 1)
	assert.Equal(t, "8:10", changes.Last_Seq)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithAdminChannels(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "", "letmein", "", collection, []string{"A"}, nil))
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", false)
	require.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[1].ID)
	assert.False(t, changes.Results[0].Revoked)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "", "letmein", "", collection, []string{}, nil))
	RequireStatus(t, resp, http.StatusOK)

	changes, err = rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d&revocations=true", 2), "user", false)
	require.NoError(t, err)
	require.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
}

func TestRevocationWithAdminRoles(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	resp := rt.SendAdminRequest("PUT", "/db/_role/role", GetRolePayload(t, "", "", collection, []string{"A"}))
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_roles": ["role"], "password": "letmein"}`)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes, err := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", false)
	require.NoError(t, err)
	assert.Equal(t, 2, len(changes.Results))

	assert.Equal(t, "doc", changes.Results[1].ID)
	assert.False(t, changes.Results[1].Revoked)

	resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_roles": []}`)
	RequireStatus(t, resp, http.StatusOK)

	changes, err = rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d&revocations=true", 3), "user", false)
	require.NoError(t, err)
	require.Equal(t, 2, len(changes.Results))

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
	docRevID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{}})
	doc2RevID := rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"A"}})

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)
	assert.Equal(t, "doc2", changes.Results[1].ID)

	revocationTester.removeRole("user", "foo")
	_ = rt.CreateDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{"A"}})
	_ = rt.CreateDocReturnRev(t, "doc2", doc2RevID, map[string]interface{}{"channels": []string{"A"}, "val": "mutate"})

	changes = revocationTester.getChanges(6, 1)
	assert.Len(t, changes.Results, 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
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
	revIDDoc := rt.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	revIDDoc2 := rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch2"}})

	changes := revocationTester.getChanges(0, 3)

	revocationTester.fillToSeq(19)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(29)
	revocationTester.removeRoleChannel("foo2", "ch2")

	revocationTester.fillToSeq(39)
	revIDDoc = rt.CreateDocReturnRev(t, "doc1", revIDDoc, map[string]interface{}{"channels": []string{"ch1"}, "val": "mutate"})

	revocationTester.fillToSeq(49)
	revIDDoc2 = rt.CreateDocReturnRev(t, "doc2", revIDDoc2, map[string]interface{}{"channels": []string{"ch2"}, "val": "mutate"})

	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, revIDDoc, changes.Results[0].Changes[0]["rev"])
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, "doc2", changes.Results[1].ID)
	assert.Equal(t, revIDDoc2, changes.Results[1].Changes[0]["rev"])
	assert.True(t, changes.Results[1].Revoked)

	changes = revocationTester.getChanges("20:40", 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	// Check no results with 60
	changes = revocationTester.getChanges("60", 0)

	// Ensure 11 low sequence means we get revocations from that far back
	changes = revocationTester.getChanges("20:0:60", 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
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
	revIDDoc := rt.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	revIDDoc2 := rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch2"}})

	changes := revocationTester.getChanges(0, 3)

	revocationTester.fillToSeq(19)
	revocationTester.removeRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(29)
	revocationTester.removeRoleChannel("foo", "ch2")

	revocationTester.fillToSeq(39)
	_ = rt.CreateDocReturnRev(t, "doc1", revIDDoc, map[string]interface{}{"channels": []string{"ch1"}, "val": "mutate"})

	revocationTester.fillToSeq(49)
	_ = rt.CreateDocReturnRev(t, "doc2", revIDDoc2, map[string]interface{}{"channels": []string{"ch2"}, "val": "mutate"})

	changes = revocationTester.getChanges(changes.Last_Seq, 2)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, "doc2", changes.Results[1].ID)
	assert.True(t, changes.Results[1].Revoked)

	changes = revocationTester.getChanges("20:40", 1)
	assert.Equal(t, "doc2", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)

	// Check no results with 60
	changes = revocationTester.getChanges("60", 0)

	// Ensure 11 low sequence means we get revocations from that far back
	changes = revocationTester.getChanges("11:0:60", 2)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.True(t, changes.Results[0].Revoked)
	assert.Equal(t, "doc2", changes.Results[1].ID)
	assert.True(t, changes.Results[1].Revoked)
}

func TestRevocationsWithQueryLimit(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, &RestTesterConfig{
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

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(9)
	_ = rt.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.CreateDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"ch1"}})

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
			QueryPaginationLimit: base.IntPtr(2),
		}},
	})
	defer rt.Close()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")
	revocationTester.addUserChannel("user", "ch2")

	revocationTester.fillToSeq(9)
	_ = rt.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1", "ch2"}})
	_ = rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch1", "ch2"}})
	_ = rt.CreateDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"ch1", "ch2"}})

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	_ = changes.Last_Seq
	// This changes feed would loop if CBG-3273 was still an issue
	changes = revocationTester.getChanges(0, 4)

	// Get one of the 3 docs which the user should still have access to
	resp := rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", nil, "user", "test")
	RequireStatus(t, resp, 200)

	// Revoke access to ch2
	revocationTester.removeUserChannel("user", "ch2")
	changes = revocationTester.getChanges(0, 7)
	// Get a doc to ensure no access
	resp = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", nil, "user", "test")
	RequireStatus(t, resp, 403)
}

func TestRevocationsWithQueryLimitChangesLimit(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, &RestTesterConfig{
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

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "ch1")

	revocationTester.fillToSeq(9)
	_ = rt.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"ch1"}})
	_ = rt.CreateDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"ch1"}})

	changes := revocationTester.getChanges("0", 4)

	revocationTester.removeRole("user", "foo")

	waitForUserChangesWithLimit := func(sinceVal interface{}, limit int) ChangesResults {
		var changesRes ChangesResults
		err := rt.WaitForCondition(func() bool {
			resp := rt.SendUserRequestWithHeaders("GET", fmt.Sprintf("/{{.keyspace}}/_changes?since=%v&revocations=true&limit=%d", sinceVal, limit), "", nil, "user", "test")
			require.Equal(t, http.StatusOK, resp.Code)
			err := json.Unmarshal(resp.BodyBytes(), &changesRes)
			require.NoError(t, err)

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
	collection := rt.GetSingleTestDatabaseCollection()

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "A")

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"channels": ["A"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeRoleChannel("foo", "A")
	require.NoError(t, rt.WaitForPendingChanges())

	data := collection.GetCollectionDatastore()
	leakyDataStore, ok := base.AsLeakyDataStore(data)
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

	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeRoleChannel("foo", "a")
	revocationTester.removeRoleChannel("foo", "c")

	_ = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": []string{}})
	_ = rt.CreateDocReturnRev(t, "doc2", "", map[string]interface{}{"channels": []string{"b", "a"}})

	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addRoleChannel("foo", "c")
	doc3Rev := rt.CreateDocReturnRev(t, "doc3", "", map[string]interface{}{"channels": []string{"c"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.removeRoleChannel("foo", "c")
	_ = rt.CreateDocReturnRev(t, "doc3", doc3Rev, map[string]interface{}{"channels": []string{"c"}})
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
	revId := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})
	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeUserChannel("user", "a")
	revId = rt.CreateDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate", "channels": []string{"a"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addUserChannel("user", "a")
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.removeUserChannel("user", "a")
	revId = rt.CreateDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate2", "channels": []string{"a"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addUserChannel("user", "a")
	revId = rt.CreateDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate3", "channels": []string{"a"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
	assert.Len(t, changes.Results, 1)

	revocationTester.addRole("user", "foo")
	revocationTester.addRoleChannel("foo", "b")
	revId = rt.CreateDocReturnRev(t, "doc", revId, map[string]interface{}{"channels": []string{"b"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)

	revocationTester.removeRoleChannel("foo", "b")
	revocationTester.removeRole("user", "foo")
	_ = rt.CreateDocReturnRev(t, "doc", revId, map[string]interface{}{"mutate": "mutate", "channels": []string{"b"}})
	changes = revocationTester.getChanges(changes.Last_Seq, 1)
}

func TestChannelHistoryPruning(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()
	c := collection.Name
	s := collection.ScopeName

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
	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "a"})
	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	revocationTester.removeUserChannel("user", "a")
	revID = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"mutate": "mutate", "channels": "a"})
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
	_ = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"mutate": "mutate2", "channels": "a"})
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
			UserXattrKey: xattrKey,
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
	collection := rt.GetSingleTestDatabaseCollection()

	data := collection.GetCollectionDatastore()

	userXattrStore, ok := base.AsUserXattrStore(data)
	require.True(t, ok)

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/accessDoc", `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	_, err := userXattrStore.WriteUserXattr("accessDoc", xattrKey, map[string]interface{}{"userChannels": map[string]interface{}{"user": "a"}})
	assert.NoError(t, err)

	_ = rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"a"}})

	changes := revocationTester.getChanges(0, 2)
	assert.Len(t, changes.Results, 2)

	_, err = userXattrStore.WriteUserXattr("accessDoc", xattrKey, map[string]interface{}{})
	assert.NoError(t, err)

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

	passiveDBURL.User = url.UserPassword("user", "test")
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

	doc1Rev := rt2.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": "chanA"})

	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", "test")
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

	_ = rt2.CreateDocReturnRev(t, "doc1", doc1Rev, map[string]interface{}{"channels": "chanA", "mutate": "val"})

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

	doc1Rev := rt2.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": []string{"chanA", "chanB"}})

	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	passiveDBURL.User = url.UserPassword("user", "test")
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

	_ = rt2.CreateDocReturnRev(t, "doc1", doc1Rev, map[string]interface{}{"channels": []string{"chanA", "chanB"}, "mutate": "val"})

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
	rt2_collection := rt2.GetSingleTestDatabaseCollection()

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

	passiveDBURL.User = url.UserPassword(revocationTestUser, revocationTestPassword)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	revocationTester.addRole(revocationTestUser, revocationTestRole)
	require.NoError(t, rt2.WaitForPendingChanges())

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
	resp := rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", "", rt2_collection, []string{"A", "B", "C"}))

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
	changesResults, err := rt1.WaitForChanges(5, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 5)

	// Revoke C and ensure docC gets purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", "", rt2_collection, []string{"A", "B"}))
	RequireStatus(t, resp, http.StatusOK)
	require.NoError(t, rt2.WaitForPendingChanges())

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docC", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

	// Revoke B and ensure docB gets purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", "", rt2_collection, []string{"A"}))
	RequireStatus(t, resp, http.StatusOK)

	err = rt1.WaitForCondition(func() bool {
		resp := rt1.SendAdminRequest("GET", "/{{.keyspace}}/docB", "")
		return resp.Code == http.StatusNotFound
	})
	require.NoError(t, err)

	// Revoke A and ensure docA, docAB, docABC gets purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_role/"+revocationTestRole, GetRolePayload(t, "", "", rt2_collection, []string{}))
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
	rt2_collection := rt2.GetSingleTestDatabaseCollection()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2_collection, []string{"A", "B"}, nil))
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

	docARev := rt2.CreateDocReturnRev(t, "docA", "", map[string][]string{"channels": []string{"A"}})
	docA1Rev := rt2.CreateDocReturnRev(t, "docA1", "", map[string][]string{"channels": []string{"A"}})
	_ = rt2.CreateDocReturnRev(t, "docA2", "", map[string][]string{"channels": []string{"A"}})

	_ = rt2.CreateDocReturnRev(t, "docB", "", map[string][]string{"channels": []string{"B"}})

	require.NoError(t, ar.Start(ctx1))

	changesResults, err := rt1.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 4)

	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	resp = rt2.SendAdminRequest("DELETE", "/{{.keyspace}}/docA?rev="+docARev, "")
	RequireStatus(t, resp, http.StatusOK)

	resp = rt2.SendAdminRequest("DELETE", "/{{.keyspace}}/docA1?rev="+docA1Rev, "")
	RequireStatus(t, resp, http.StatusOK)

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2_collection, []string{"B"}, nil))
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
	rt2Collection := rt2.GetSingleTestDatabaseCollection()

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
		password = "test"
	)

	passiveDBURL.User = url.UserPassword(username, password)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	_ = rt2.CreateDocReturnRev(t, "docA", "", map[string][]string{"channels": []string{"ABC"}})
	require.NoError(t, rt2.WaitForPendingChanges())

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

	resp := rt2.SendAdminRequest("PUT", "/{{.db}}/_user/user", GetUserPayload(t, username, password, "", rt2Collection, []string{"ABC"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	require.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	// Wait for docs to turn up on local / rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

	// Revoke A and ensure ABC chanel access and ensure DocA is purged from local
	resp = rt2.SendAdminRequest("PUT", "/{{.db}}/_user/user", GetUserPayload(t, username, password, "", rt2Collection, []string{}, nil))
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
	rt2_collection := rt2.GetSingleTestDatabaseCollection()

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

	passiveDBURL.User = url.UserPassword("user", "test")
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	_ = rt2.CreateDocReturnRev(t, "docA", "", map[string][]string{"channels": []string{"A"}})
	_ = rt2.CreateDocReturnRev(t, "docAB", "", map[string][]string{"channels": []string{"A", "B"}})
	_ = rt2.CreateDocReturnRev(t, "docB", "", map[string][]string{"channels": []string{"B"}})
	_ = rt2.CreateDocReturnRev(t, "docABC", "", map[string][]string{"channels": []string{"A", "B", "C"}})
	_ = rt2.CreateDocReturnRev(t, "docC", "", map[string][]string{"channels": []string{"C"}})
	require.NoError(t, rt2.WaitForPendingChanges())

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

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "test", "", rt2_collection, []string{"*"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	require.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	// Wait for docs to turn up on local / rt1
	changesResults, err := rt1.WaitForChanges(5, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 5)

	// Revoke A and ensure docA, docAB, docABC get purged from local
	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "test", "", rt2_collection, []string{}, nil))
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
	rt2_collection := rt2.GetSingleTestDatabaseCollection()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2_collection, []string{"A", "B"}, nil))
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

	_ = rt2.CreateDocReturnRev(t, "docA", "", map[string][]string{"channels": []string{"A"}})
	_ = rt2.CreateDocReturnRev(t, "docA1", "", map[string][]string{"channels": []string{"A"}})
	_ = rt2.CreateDocReturnRev(t, "docA2", "", map[string][]string{"channels": []string{"A"}})

	require.NoError(t, ar.Start(ctx1))

	changesResults, err := rt1.WaitForChanges(3, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 3)

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

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2_collection, []string{"B"}, nil))
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

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	assert.NoError(t, err)
	defer btc.Close()

	// Add channel to role and role to user
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 and then create doc in channel A
	revocationTester.fillToSeq(4)
	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	require.NoError(t, rt.WaitForPendingChanges())

	// Start pull
	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	// Wait for doc revision to come over
	_, ok := btc.WaitForBlipRevMessage("doc", revID)
	require.True(t, ok)

	// Remove role from user
	revocationTester.removeRole("user", "foo")

	revID = rt.CreateDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": "!"})

	revocationTester.fillToSeq(10)
	revID = rt.CreateDocReturnRev(t, "doc1", revID, map[string]interface{}{})

	require.NoError(t, rt.WaitForPendingChanges())

	// Start a pull since 5 to receive revocation and removal
	err = btc.StartPullSince("false", "5", "false")
	assert.NoError(t, err)

	// Wait for doc1 rev2 - This is the last rev we expect so we can be sure replication is complete here
	_, found := btc.WaitForRev("doc1", revID)
	require.True(t, found)

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
		t.Run(testCase.Name, func(t *testing.T) {
			// Verify the deleted property in the changes message is "2" this indicated a revocation
			for _, msg := range messages {
				if msg.Properties[db.BlipProfile] == db.MessageChanges {
					var changesMessages [][]interface{}
					err = msg.ReadJSONBody(&changesMessages)
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

	assert.NoError(t, err)
}

func TestRevocationNoRev(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := InitScenario(t, nil)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	assert.NoError(t, err)
	defer btc.Close()

	// Add channel to role and role to user
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 and then create doc in channel A
	revocationTester.fillToSeq(4)
	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	require.NoError(t, rt.WaitForPendingChanges())
	firstOneShotSinceSeq := rt.GetDocumentSequence("doc")

	// OneShot pull to grab doc
	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok := btc.WaitForRev("doc", "1-ad48b5c9d9c47b98532a3d8164ec0ae7")
	require.True(t, ok)

	// Remove role from user
	revocationTester.removeRole("user", "foo")

	_ = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A", "val": "mutate"})

	waitRevID := rt.CreateDocReturnRev(t, "docmarker", "", map[string]interface{}{"channels": "!"})
	require.NoError(t, rt.WaitForPendingChanges())

	lastSeqStr := strconv.FormatUint(firstOneShotSinceSeq, 10)
	err = btc.StartPullSince("false", lastSeqStr, "false")
	assert.NoError(t, err)

	_, ok = btc.WaitForRev("docmarker", waitRevID)
	require.True(t, ok)

	messages := btc.pullReplication.GetMessages()

	var highestMsgSeq uint32
	var highestSeqMsg blip.Message
	// Grab most recent changes message
	for _, message := range messages {
		messageBody, err := message.Body()
		require.NoError(t, err)
		if message.Properties["Profile"] == db.MessageChanges && string(messageBody) != "null" {
			if highestMsgSeq < uint32(message.SerialNumber()) {
				highestMsgSeq = uint32(message.SerialNumber())
				highestSeqMsg = message
			}
		}
	}

	var messageBody []interface{}
	err = highestSeqMsg.ReadJSONBody(&messageBody)
	require.NoError(t, err)
	require.Len(t, messageBody, 2)
	require.Len(t, messageBody[0], 4)

	deletedFlag, err := messageBody[0].([]interface{})[3].(json.Number).Int64()
	require.NoError(t, err)

	assert.Equal(t, deletedFlag, int64(2))
}

func TestRevocationGetSyncDataError(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	var throw bool
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	// Two callbacks to cover usage with CBS/Xattrs and without
	revocationTester, rt := InitScenario(
		t, &RestTesterConfig{
			leakyBucketConfig: &base.LeakyBucketConfig{
				GetWithXattrCallback: func(key string) error {
					return fmt.Errorf("Leaky Bucket GetWithXattrCallback Error")
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

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	assert.NoError(t, err)
	defer btc.Close()

	// Add channel to role and role to user
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 and then create doc in channel A
	revocationTester.fillToSeq(4)
	revID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	require.NoError(t, rt.WaitForPendingChanges())
	firstOneShotSinceSeq := rt.GetDocumentSequence("doc")

	// OneShot pull to grab doc
	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	throw = true
	_, ok := btc.WaitForRev("doc", "1-ad48b5c9d9c47b98532a3d8164ec0ae7")
	require.True(t, ok)

	// Remove role from user
	revocationTester.removeRole("user", "foo")

	_ = rt.CreateDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A", "val": "mutate"})

	waitRevID := rt.CreateDocReturnRev(t, "docmarker", "", map[string]interface{}{"channels": "!"})
	require.NoError(t, rt.WaitForPendingChanges())

	lastSeqStr := strconv.FormatUint(firstOneShotSinceSeq, 10)
	err = btc.StartPullSince("false", lastSeqStr, "false")
	assert.NoError(t, err)

	_, ok = btc.WaitForRev("docmarker", waitRevID)
	require.True(t, ok)
}

// Regression test for CBG-2183.
func TestBlipRevokeNonExistentRole(t *testing.T) {
	rt := NewRestTester(t,
		&RestTesterConfig{
			GuestEnabled: false,
		})
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// 1. Create user with admin_roles including two roles not previously defined (a1 and a2, for example)
	res := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_user/bilbo", rt.GetDatabase().Name), GetUserPayload(t, "bilbo", "test", "", collection, []string{"c1"}, []string{"a1", "a2"}))
	RequireStatus(t, res, http.StatusCreated)

	// Create a doc so we have something to replicate
	res = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/testdoc", `{"channels": ["c1"]}`)
	RequireStatus(t, res, http.StatusCreated)

	// 3. Update the user to not reference one of the roles (update to ['a1'], for example)
	// [also revoke channel c1 so the doc shows up in the revocation queries]
	res = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_user/bilbo", rt.GetDatabase().Name), GetUserPayload(t, "bilbo", "test", "", collection, []string{}, []string{"a1"}))
	RequireStatus(t, res, http.StatusOK)

	// 4. Try to sync
	bt, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "bilbo",
		SendRevocations: true,
	})
	require.NoError(t, err)
	defer bt.Close()

	require.NoError(t, bt.StartPull())

	// in the failing case we'll panic before hitting this
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()
	}, 1)
}

func TestReplicatorSwitchPurgeNoReset(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	defer db.SuspendSequenceBatching()()

	base.RequireNumTestBuckets(t, 2)

	// Passive
	_, rt2 := InitScenario(t, nil)
	defer rt2.Close()
	rt2_collection := rt2.GetSingleTestDatabaseCollection()

	// Active
	rt1 := NewRestTester(t,
		&RestTesterConfig{
			CustomTestBucket: base.GetTestBucket(t),
			SyncFn:           channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	resp := rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2_collection, []string{"A", "B"}, nil))
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
		_ = rt2.CreateDocReturnRev(t, fmt.Sprintf("docA%d", i), "", map[string][]string{"channels": []string{"A"}})
	}

	for i := 0; i < 7; i++ {
		_ = rt2.CreateDocReturnRev(t, fmt.Sprintf("docB%d", i), "", map[string][]string{"channels": []string{"B"}})
	}

	err = rt2.WaitForPendingChanges()
	require.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))

	changesResults, err := rt1.WaitForChanges(17, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 17)

	// Going to stop & start replication between these actions to make out of order seq no's more likely. More likely
	// to hit CBG-1591
	require.NoError(t, ar.Stop())
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	resp = rt2.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "letmein", "", rt2_collection, []string{"B"}, nil))
	RequireStatus(t, resp, http.StatusOK)

	// Add another few docs to 'bump' rt1's seq no. Otherwise it'll end up revoking next time as the above user PUT is
	// not processed by the rt1 receiver.
	for i := 7; i < 15; i++ {
		_ = rt2.CreateDocReturnRev(t, fmt.Sprintf("docB%d", i), "", map[string][]string{"channels": []string{"B"}})
	}

	err = rt2.WaitForPendingChanges()
	assert.NoError(t, err)

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	changesResults, err = rt1.WaitForChanges(8, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", changesResults.Last_Seq), "", true)
	require.NoError(t, err)
	assert.Len(t, changesResults.Results, 8)

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
	_ = rt2.CreateDocReturnRev(t, "docMarker", "", map[string][]string{"channels": []string{"B"}})

	require.NoError(t, ar.Start(ctx1))
	rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	// Validate none of the documents are purged after flipping option
	err = rt2.WaitForPendingChanges()
	assert.NoError(t, err)

	changesResults, err = rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", changesResults.Last_Seq), "", true)
	assert.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

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
