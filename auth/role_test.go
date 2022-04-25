/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package auth

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
)

func TestInitRole(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	// Check initializing role with legal role name.
	role := &roleImpl{}
	assert.NoError(t, role.initRole("Music", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Equal(t, "Music", role.Name_)
	assert.Equal(t, channels.TimedSet{
		"Spotify": channels.NewVbSimpleSequence(0x1),
		"Youtube": channels.NewVbSimpleSequence(0x1)}, role.ExplicitChannels_)

	// Check initializing role with illegal role name.
	role = &roleImpl{}
	assert.Error(t, role.initRole("Music/", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Error(t, role.initRole("Music:", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Error(t, role.initRole("Music,", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Error(t, role.initRole(".", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Error(t, role.initRole("\xf7,", channels.SetOf(t, "Spotify", "Youtube")))
}

func TestAuthorizeChannelsRole(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	role, err := auth.NewRole("root", channels.SetOf(t, "superuser"))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	assert.NoError(t, role.AuthorizeAllChannels(channels.SetOf(t, "superuser")))
	assert.Error(t, role.AuthorizeAllChannels(channels.SetOf(t, "unknown")))
	assert.NoError(t, role.AuthorizeAnyChannel(channels.SetOf(t, "superuser", "unknown")))
	assert.Error(t, role.AuthorizeAllChannels(channels.SetOf(t, "unknown1", "unknown2")))
}

func BenchmarkIsValidPrincipalName(b *testing.B) {
	const nameLength = 50
	name := strings.Builder{}
	for i := 0; i < nameLength; i++ {
		name.WriteRune(rune(rand.Intn('z'-'a') + 'a'))
	}
	nameStr := name.String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsValidPrincipalName(nameStr)
	}
}
