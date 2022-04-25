/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package auth

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestUserAuthenticateDisabled(t *testing.T) {
	const (
		username    = "alice"
		oldPassword = "hunter2"
	)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)

	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Create user
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	u, err := auth.NewUser(username, oldPassword, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, u)

	assert.False(t, u.Disabled())
	// Correct password, activated account
	assert.True(t, u.Authenticate(oldPassword))
	// Incorrect password, activated account
	assert.False(t, u.Authenticate("test"))

	// Disable account
	u.SetDisabled(true)

	assert.True(t, u.Disabled())
	// Correct password, disabled account
	assert.False(t, u.Authenticate(oldPassword))
	// Incorrect password, disabled account
	assert.False(t, u.Authenticate("test"))

}

func TestUserAuthenticatePasswordHashUpgrade(t *testing.T) {
	const (
		username      = "alice"
		oldPassword   = "hunter2"
		newBcryptCost = 12
	)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)

	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Create user
	auth := NewAuthenticator(bucket, nil, DefaultAuthenticatorOptions())
	u, err := auth.NewUser(username, oldPassword, base.Set{})
	require.NoError(t, err)
	require.NotNil(t, u)

	user := u.(*userImpl)
	oldHash := user.PasswordHash_

	// Make sure their password was hashed with the desired cost
	cost, err := bcrypt.Cost(user.PasswordHash_)
	require.NoError(t, err)
	assert.Equal(t, DefaultBcryptCost, cost)

	// Try to auth with an incorrect password
	assert.False(t, u.Authenticate("test"))

	// Make sure the hash has not changed
	newHash := user.PasswordHash_
	assert.Equal(t, string(oldHash), string(newHash))

	// Authenticate correctly
	assert.True(t, u.Authenticate(oldPassword))

	// Make sure the hash has still not changed (we've not changed the cost yet)
	newHash = user.PasswordHash_
	assert.Equal(t, string(oldHash), string(newHash))

	// Check the cost is still the old value
	cost, err = bcrypt.Cost(user.PasswordHash_)
	require.NoError(t, err)
	assert.Equal(t, DefaultBcryptCost, cost)

	// Now bump the global bcrypt cost
	err = auth.SetBcryptCost(newBcryptCost)
	require.NoError(t, err)

	// Reset bcrypt cost after test
	defer func() { require.NoError(t, auth.SetBcryptCost(DefaultBcryptCost)) }()

	// Authenticate incorrectly again
	assert.False(t, u.Authenticate("test"))

	// Make sure the hash has still not changed (cost has been bumped, but auth was not successful)
	newHash = user.PasswordHash_
	assert.Equal(t, string(oldHash), string(newHash))

	// Authenticate correctly
	assert.True(t, u.Authenticate(oldPassword))

	// Hash should've changed, as the above authenticate was successful
	newHash = user.PasswordHash_
	assert.NotEqual(t, string(oldHash), string(newHash))

	// Cost should now match newBcryptCost
	cost, err = bcrypt.Cost(user.PasswordHash_)
	require.NoError(t, err)
	assert.Equal(t, newBcryptCost, cost)

}

func TestIsValidEmail(t *testing.T) {
	t.Run("Valid addresses", func(t *testing.T) {
		validEmails := map[string]string{
			`alice@couchbase.com`:       "Valid Email Address Check",
			`a1ice@couchbase.com`:       "Numbers and letters in the address field",
			`alice.bob@couchbase.com`:   "Email contains dot in the address field",
			`alice@couchbase.lab.com`:   "Email contains dot with sub-domain",
			`alice+bob@couchbase.com`:   "Plus sign is considered valid character",
			`alice@127.0.0.1`:           "Domain is valid IP address",
			`1234567890@couchbase.com`:  "Digits in address are valid",
			`alice@couchbase-com`:       "Dash in domain name is valid",
			`_______@couchbase-com`:     "Underscore in the address field is valid",
			`alice@couchbase.name`:      ".name is valid Top Level Domain name",
			`alice@couchbase.co.jp`:     "Dot in Top Level Domain name is considered valid",
			`alice-bob@couchbase.co.jp`: "Dash in address field is valid",
			`alice'bob@couchbase.com`:   "apostrophe in address",
			// Examples from Wikipedia page on Email Addresses
			`simple@example.com`:                             "",
			`very.common@example.com`:                        "",
			`disposable.style.email.with+symbol@example.com`: "",
			`other.email-with-hyphen@example.com`:            "",
			`fully-qualified-domain@example.com`:             "",
			`user.name+tag+sorting@example.com`:              "(may go to user.name@example.com inbox depending on mail server)",
			`x@example.com`:                                  "(one-letter local-part)",
			`example-indeed@strange-example.com`:             "",
			`admin@mailserver1`:                              "(local domain name with no TLD, although ICANN highly discourages dotless email addresses[10])",
			`example@s.example`:                              "(see the List of Internet top-level domains)",
			`" "@example.org`:                                "(space between the quotes)",
			`"john..doe"@example.org`:                        "(quoted double dot)",
			`mailhost!username@example.org`:                  "(bangified host route used for uucp mailers)",
			`user%example.com@example.org`:                   "(% escaped mail route to user@example.com via example.org)",
			`ser-@example.org`:                               "(local part ending with non-alphanumeric character from the list of allowed printable characters)",
		}

		for email, description := range validEmails {
			t.Run(email, func(t *testing.T) {
				assert.True(t, IsValidEmail(email), description)
			})
		}
	})

	t.Run("Invalid addresses", func(t *testing.T) {
		invalidEmails := map[string]string{
			`aliceatcouchbasedotcom`:        "Missing @ sign and domain",
			`#@%^%#$@#$@#.com`:              "Garbage value",
			`@couchbase.com`:                "Missing username",
			`email.couchbase.com`:           "Missing @",
			`alice@couchbase@couchbase.com`: "Two @ sign",
			// Examples from Wikipedia page on Email Addresses
			`Abc.example.com`:   "no @ character",
			`A@b@c@example.com`: "only one @ is allowed outside quotation marks",
		}

		for email, description := range invalidEmails {
			t.Run(email, func(t *testing.T) {
				assert.False(t, IsValidEmail(email), description)
			})
		}
	})

	// Because our validator is permissive rather than strict, these technically invalid emails still pass the validator.
	// We'll assert on current behaviour and log in case this changes in the future.
	t.Run("Allowed Invalid addresses", func(t *testing.T) {
		invalidEmails := map[string]string{
			`Alice Bob <alice@couchbase.com>`: "Encoded html within email is invalid",
			`áĺíćé@couchbase.com`:             "Unicode char as address",
			// Examples from Wikipedia page on Email Addresses
			`a"b(c)d,e:f;g<h>i[j\k]l@example.com`:                                            "none of the special characters in this local-part are allowed outside quotation marks",
			`just"not"right@example.com`:                                                     "quoted strings must be dot separated or the only element making up the local-part",
			`this is"not\allowed@example.com`:                                                "spaces, quotes, and backslashes may only exist when within quoted strings and preceded by a backslash",
			`this\ still\"not\\allowed@example.com`:                                          "even if escaped (preceded by a backslash), spaces, quotes, and backslashes must still be contained by quotes",
			`1234567890123456789012345678901234567890123456789012345678901234+x@example.com`: "local-part is longer than 64 characters",
			`i_like_underscore@but_its_not_allowed_in_this_part.example.com`:                 "Underscore is not allowed in domain part",
		}

		for email, description := range invalidEmails {
			t.Run(email, func(t *testing.T) {
				require.True(t, IsValidEmail(email), "Expected this invalid email to pass with the permissive validator - Have we decided to be more strict about validating? - %s", description)
			})
		}
	})

}

func TestInvalidUsernamesRejected(t *testing.T) {
	cases := []struct {
		Name     string
		Username string
	}{
		{
			Name:     "colons",
			Username: "foo:bar",
		},
		{
			Name:     "commas",
			Username: "foo,bar",
		},
		{
			Name:     "slashes",
			Username: "foo/bar",
		},
		{
			Name:     "no alphanumeric",
			Username: "..",
		},
		{
			Name:     "invalid UTF-8",
			Username: "foo\xf9bar",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			require.False(t, IsValidPrincipalName(tc.Username), "expected '%s' to be rejected", tc.Username)
		})
	}
}

func TestCanSeeChannelSince(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())
	freeChannels := base.SetFromArray([]string{"ESPN", "HBO", "FX", "AMC"})
	user, err := auth.NewUser("user", "password", freeChannels)
	assert.Nil(t, err)

	role, err := auth.NewRole("music", channels.SetOf(t, "Spotify", "Youtube"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	role, err = auth.NewRole("video", channels.SetOf(t, "Netflix", "Hulu"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	user.(*userImpl).setRolesSince(channels.TimedSet{
		"music": channels.NewVbSimpleSequence(1),
		"video": channels.NewVbSimpleSequence(1)})

	for channel := range freeChannels {
		assert.Equal(t, uint64(1), user.CanSeeChannelSince(channel))
	}
	assert.Equal(t, uint64(0), user.CanSeeChannelSince("unknown"))
}

func TestGetAddedChannels(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	role, err := auth.NewRole("music", channels.SetOf(t, "Spotify", "Youtube"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	role, err = auth.NewRole("video", channels.SetOf(t, "Netflix", "Hulu"))
	assert.Nil(t, err)
	assert.Equal(t, nil, auth.Save(role))

	user, err := auth.NewUser("alice", "password", channels.SetOf(t, "ESPN", "HBO", "FX", "AMC"))
	assert.Nil(t, err)
	require.NoError(t, user.SetEmail("alice@couchbase.com"))

	user.(*userImpl).setRolesSince(channels.TimedSet{
		"music": channels.NewVbSimpleSequence(0x5),
		"video": channels.NewVbSimpleSequence(0x6)})

	addedChannels := user.(*userImpl).GetAddedChannels(channels.TimedSet{
		"ESPN": channels.NewVbSimpleSequence(0x5),
		"HBO":  channels.NewVbSimpleSequence(0x6)})

	expectedChannels := channels.SetOf(t, "!", "AMC", "FX", "Hulu", "Netflix", "Spotify", "Youtube")
	log.Printf("Added Channels: %v", addedChannels)
	assert.Equal(t, expectedChannels, addedChannels)
}

// Needless to say; must not authenticate with nil user reference;
func TestUserAuthenticateWithNilUserReference(t *testing.T) {
	var nouser *userImpl
	assert.False(t, nouser.Authenticate("password"))
}

// Must not authenticate if the user account is disabled.
func TestUserAuthenticateWithDisabledUserAccount(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	user.SetDisabled(true)
	assert.True(t, user.Disabled())
	assert.False(t, user.Authenticate(password))
}

// Must not authenticate if old hash is present.
// Password must be reset to use new (bcrypt) password hash.
func TestUserAuthenticateWithOldPasswordHash(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	passwordHash := user.(*userImpl).PasswordHash_
	user.(*userImpl).OldPasswordHash_ = passwordHash
	assert.False(t, user.Authenticate(password))
}

// Must not authenticate with bad password hash
func TestUserAuthenticateWithBadPasswordHash(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	err = user.SetPassword("hunter3")
	require.NoError(t, err)
	assert.False(t, user.Authenticate(password))
}

// Must not authenticate if No hash, but (incorrect) password provided.
func TestUserAuthenticateWithNoHashAndBadPassword(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)
	const (
		username = "alice"
		password = "hunter2"
	)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	auth := NewAuthenticator(testBucket, nil, DefaultAuthenticatorOptions())

	user, err := auth.NewUser(username, password, base.Set{})
	assert.NoError(t, err)
	assert.NotNil(t, user)

	user.(*userImpl).OldPasswordHash_ = nil
	assert.False(t, user.Authenticate("hunter3"))
}
