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
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitRole(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	// Check initializing role with legal role name.
	role := &roleImpl{}
	assert.NoError(t, role.initRole("Music", channels.BaseSetOf(t, "Spotify", "Youtube"), nil))
	assert.Equal(t, "Music", role.Name_)
	assert.Equal(t, channels.TimedSet{
		"Spotify": channels.NewVbSimpleSequence(0x1),
		"Youtube": channels.NewVbSimpleSequence(0x1)}, role.ExplicitChannels_)

	// Check initializing role with illegal role name.
	role = &roleImpl{}
	assert.Error(t, role.initRole("Music/", channels.BaseSetOf(t, "Spotify", "Youtube"), nil))
	assert.Error(t, role.initRole("Music:", channels.BaseSetOf(t, "Spotify", "Youtube"), nil))
	assert.Error(t, role.initRole("Music,", channels.BaseSetOf(t, "Spotify", "Youtube"), nil))
	assert.Error(t, role.initRole(".", channels.BaseSetOf(t, "Spotify", "Youtube"), nil))
	assert.Error(t, role.initRole("\xf7,", channels.BaseSetOf(t, "Spotify", "Youtube"), nil))
}

func TestAuthorizeChannelsRole(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	dataStore := testBucket.GetSingleDataStore()
	auth := NewAuthenticator(dataStore, nil, DefaultAuthenticatorOptions(base.TestCtx(t)))

	role, err := auth.NewRole("root", channels.BaseSetOf(t, "superuser"))
	assert.NoError(t, err)
	err = auth.Save(role)
	assert.NoError(t, err)

	assert.NoError(t, role.authorizeAllChannels(channels.BaseSetOf(t, "superuser")))
	assert.Error(t, role.authorizeAllChannels(channels.BaseSetOf(t, "unknown")))
	assert.NoError(t, role.authorizeAnyChannel(channels.BaseSetOf(t, "superuser", "unknown")))
	assert.Error(t, role.authorizeAllChannels(channels.BaseSetOf(t, "unknown1", "unknown2")))
}

func TestRoleKeysHash(t *testing.T) {
	for _, metadataDefault := range []bool{false, true} {
		t.Run(fmt.Sprintf("metadataDefault=%t", metadataDefault), func(t *testing.T) {
			testBucket := base.GetTestBucket(t)
			defer testBucket.Close()
			dataStore := testBucket.DefaultDataStore()

			auth := NewAuthenticator(dataStore, nil, DefaultAuthenticatorOptions(base.TestCtx(t)))
			if !metadataDefault {
				namedMetadataOptions := DefaultAuthenticatorOptions(base.TestCtx(t))
				namedMetadataOptions.MetaKeys = base.NewMetadataKeys("foo")

				auth = NewAuthenticator(dataStore, nil, namedMetadataOptions)

			}
			shortRoleName := "shortRole"
			shortRole, err := auth.NewRoleNoChannels(shortRoleName)
			require.NoError(t, err)
			require.NoError(t, auth.Save(shortRole))
			require.Equal(t, shortRoleName, shortRole.Name())
			if metadataDefault {
				require.Equal(t, "_sync:role:shortRole", auth.DocIDForRole(shortRoleName))
				docExists(t, dataStore, "_sync:role:shortRole")
			} else {
				require.Equal(t, "_sync:role:foo:shortRole", auth.DocIDForRole(shortRoleName))
				docExists(t, dataStore, "_sync:role:foo:shortRole")
			}

			longRoleName := "longRoleName" + strings.Repeat("A", 40)
			longRole, err := auth.NewRoleNoChannels(longRoleName)
			require.NoError(t, err)
			require.NoError(t, auth.Save(longRole))
			require.Equal(t, longRoleName, longRole.Name())

			if metadataDefault {
				docExists(t, dataStore, "_sync:role:longRoleNameAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
			} else {
				docExists(t, dataStore, "_sync:role:foo:420fe9ed57be3ed79a275d9598e8e3fb30101b3d")
			}
		})
	}
}

func BenchmarkIsValidPrincipalName(b *testing.B) {
	const nameLength = 25
	name := strings.Builder{}
	for i := 0; i < nameLength; i++ {
		name.WriteRune(rune(rand.Intn('z'-'a') + 'a'))
	}
	nameStr := name.String()

	testcases := []struct {
		desc string
		name string
	}{
		{desc: "valid name", name: nameStr + nameStr},
		{desc: "invalid char", name: nameStr + "/" + nameStr}, // negative case for comparison with validate func
	}

	b.ResetTimer()
	for _, tc := range testcases {
		b.Run(tc.desc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				IsValidPrincipalName(tc.name)
			}
		})

	}
}

func TestValidatePrincipalName(t *testing.T) {
	getName := func(l int) string {
		name := strings.Builder{}
		for i := 0; i < l; i++ {
			name.WriteRune(rune(rand.Intn('z'-'a') + 'a'))
		}
		return name.String()
	}
	name25 := getName(25)
	name50 := getName(50)
	name240 := getName(240)
	nonUTF := "\xc3\x28"
	noAlpha := "!@#$%"

	testcases := []struct {
		desc   string
		name   string
		expect string
	}{
		{desc: "valid name", name: name50, expect: ""},
		{desc: "valid guest", name: "", expect: ""},
		{desc: "invalid char", name: name25 + "/" + name25, expect: "contains '/', ':', ',', or '`'"},
		{desc: "invalid length", name: name240, expect: "length exceeds"},
		{desc: "invalid utf-8", name: nonUTF, expect: "non UTF-8 encoding"},
		{desc: "invalid no alpha", name: noAlpha, expect: "must contain alphanumeric"},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			err := ValidatePrincipalName(tc.name)
			if tc.expect == "" {
				assert.Nil(t, err)
				assert.True(t, IsValidPrincipalName(tc.name)) // assert compatible with older function
			} else if assert.NotNil(t, err) {
				t.Log("msg: ", err.Error())
				assert.Contains(t, err.Error(), tc.expect)
				assert.False(t, IsValidPrincipalName(tc.name))
			}
		})
	}
}

func BenchmarkValidatePrincipalName(b *testing.B) {
	getName := func(l int) string {
		name := strings.Builder{}
		for i := 0; i < l; i++ {
			name.WriteRune(rune(rand.Intn('z'-'a') + 'a'))
		}
		return name.String()
	}
	name25 := getName(25)
	name50 := getName(50)
	name251 := getName(251)

	testcases := []struct {
		desc string
		name string
	}{
		{desc: "valid name", name: name50},
		{desc: "invalid char", name: name25 + "/" + name25},
		{desc: "invalid length", name: name251},
	}

	b.ResetTimer()
	for _, tc := range testcases {
		b.Run(tc.desc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = ValidatePrincipalName(tc.name)
			}
		})

	}
}
