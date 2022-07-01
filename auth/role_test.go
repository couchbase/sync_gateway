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
	// const nameLength = 50
	// name := strings.Builder{}
	// for i := 0; i < nameLength; i++ {
	// 	name.WriteRune(rune(rand.Intn('z'-'a') + 'a'))
	// }
	// nameStr := name.String()
	// b.ResetTimer()
	// for i := 0; i < b.N; i++ {
	// 	IsValidPrincipalName(nameStr)
	// }

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
		{desc: "invalid char", name: nameStr + "/" + nameStr},
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

// NOTE: tests both, IsValidPrincipalName and ValidatePrincipalName, since
// one is just a facade of the other
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
	name251 := getName(251)
	nonUTF := "\xc3\x28"
	noAlpha := "!@#$%"

	testcases := []struct {
		desc   string
		name   string
		fast   bool
		expect string
	}{
		{desc: "valid name", name: name50, expect: ""},
		{desc: "valid guest", name: "", expect: ""},
		{desc: "invalid char", name: name25 + "/" + name25, fast: false, expect: "contains '/', ':', ',', or '`'"},
		{desc: "invalid char fast", name: name25 + "/" + name25, fast: true, expect: "contains '/', ':', ',', or '`'"},
		{desc: "invalid length", name: name251, fast: false, expect: "length exceeds"},
		{desc: "invalid length fast", name: name251, fast: true, expect: "length exceeds"},
		{desc: "invalid utf-8", name: nonUTF, fast: false, expect: "non UTF-8 encoding"},
		{desc: "invalid utf-8 fast", name: nonUTF, fast: true, expect: "non UTF-8 encoding"},
		{desc: "invalid no alpha", name: noAlpha, fast: false, expect: "must contain alphanumeric"},
		{desc: "invalid no alpha fast", name: noAlpha, fast: true, expect: "must contain alphanumeric"},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			err := ValidatePrincipalName(tc.name, tc.fast)
			if tc.expect == "" {
				assert.Nil(t, err)
				assert.True(t, IsValidPrincipalName(tc.name))
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
		fast bool
	}{
		{desc: "valid name", name: name50},
		{desc: "invalid char", name: name25 + "/" + name25, fast: false},
		{desc: "invalid char fast", name: name25 + "/" + name25, fast: true},
		{desc: "invalid length", name: name251, fast: false},
		{desc: "invalid length fast", name: name251, fast: true},
	}

	b.ResetTimer()
	for _, tc := range testcases {
		b.Run(tc.desc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ValidatePrincipalName(tc.name, tc.fast)
			}
		})

	}
}
