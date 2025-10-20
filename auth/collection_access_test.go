// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package auth

import (
	"bytes"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireCanSeeCollectionChannels asserts that the principal can see all the specified channels in the given collection
func requireCanSeeCollectionChannels(t *testing.T, scope, collection string, princ Principal, channels ...string) {
	for _, channel := range channels {
		require.True(t, princ.CanSeeCollectionChannel(scope, collection, channel), "Expected %s to be able to see channel %q in %s.%s", princ.Name(), channel, scope, collection)
	}
}

// requireCannotSeeCollectionChannels asserts that the principal cannot see any of the specified channels in the given collection
func requireCannotSeeCollectionChannels(t *testing.T, scope, collection string, princ Principal, channels ...string) {
	for _, channel := range channels {
		require.False(t, princ.CanSeeCollectionChannel(scope, collection, channel), "Expected %s to NOT be able to see channel %q in %s.%s", princ.Name(), channel, scope, collection)
	}
}

func TestUserCollectionAccess(t *testing.T) {

	ctx := base.TestCtx(t)
	// User with no access:
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	options := DefaultAuthenticatorOptions(base.TestCtx(t))
	options.Collections = map[string]map[string]struct{}{
		"scope1": {
			"collection1": struct{}{},
		},
	}
	auth := NewTestAuthenticator(t, bucket.GetSingleDataStore(), nil, options)
	user, _ := auth.NewUser("foo", "password", nil)
	scope := "scope1"
	collection := "collection1"
	otherScope := "scope2"
	otherCollection := "collection2"
	nonMatchingCollections := [][2]string{{base.DefaultScope, base.DefaultCollection}, {scope, otherCollection}, {otherScope, collection}, {otherScope, otherCollection}}
	// Default collection checks - should not have access based on authenticator
	requireExpandWildCardChannel(t, user, nil, []string{"*"})
	requireCannotSeeChannels(t, user, "x", "y", "!", "*")
	require.ErrorIs(t, user.authorizeAllChannels(ch.BaseSetOf(t, "*")), errNotAllowedChannels)
	require.ErrorIs(t, user.authorizeAnyChannel(ch.BaseSetOf(t, "x", "y")), errUnauthorized)
	require.ErrorIs(t, user.authorizeAnyChannel(ch.BaseSetOf(t)), errUnauthorized)
	// Named collection checks
	requireExpandCollectionWildCardChannels(t, user, scope, collection, []string{"!"}, []string{"*"})
	requireCannotSeeCollectionChannels(t, scope, collection, user, "x", "y", "*")
	require.ErrorIs(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")), errNotAllowedChannels)
	require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")), errUnauthorized)
	require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)), errUnauthorized)

	// User with access to one channel in named collection:
	user.setCollectionChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "x"), 1))
	// Matching named collection checks
	requireExpandCollectionWildCardChannels(t, user, scope, collection, []string{"x"}, []string{"*"})
	requireCanSeeCollectionChannels(t, scope, collection, user, "x")
	requireCannotSeeCollectionChannels(t, scope, collection, user, "y", "!", "*")
	require.ErrorIs(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "x", "y")), errNotAllowedChannels)
	require.ErrorIs(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")), errNotAllowedChannels)
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")))
	require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "y")), errUnauthorized)
	require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)), errUnauthorized)

	// Non-matching collection checks
	for _, pair := range nonMatchingCollections {
		s := pair[0]
		c := pair[1]
		requireExpandCollectionWildCardChannels(t, user, s, c, nil, []string{"*"})
		requireCannotSeeCollectionChannels(t, s, c, user, "x", "y", "!", "*")
		if base.IsDefaultCollection(s, c) {
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")), errNotAllowedChannels, "for %s.%s", s, c)
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")), errNotAllowedChannels, "for %s.%s", s, c)
		} else {
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")), errUnauthorizedChannels, "for %s.%s", s, c)
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")), errUnauthorizedChannels, "for %s.%s", s, c)
		}
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "x", "y")), errUnauthorized)
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "y")), errUnauthorized)
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t)), errUnauthorized)
	}

	// User with access to two channels:
	// User with access to one channel in named collection:
	user.setCollectionChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "x", "y"), 1))
	// Matching named collection checks
	requireExpandCollectionWildCardChannels(t, user, scope, collection, []string{"x", "y"}, []string{"*"})
	requireCanSeeCollectionChannels(t, scope, collection, user, "x", "y")
	requireCannotSeeCollectionChannels(t, scope, collection, user, "z")
	require.NoError(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "x", "y")))
	require.ErrorIs(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")), errNotAllowedChannels)
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")))
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "y")))
	require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "z")), errUnauthorized)
	require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)), errUnauthorized)
	// Non-matching collection checks
	for _, pair := range nonMatchingCollections {
		s := pair[0]
		c := pair[1]
		requireExpandCollectionWildCardChannels(t, user, s, c, nil, []string{"*"})
		requireCannotSeeCollectionChannels(t, s, c, user, "x", "y", "z", "!", "*")
		if base.IsDefaultCollection(s, c) {
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")), errNotAllowedChannels, "for %s.%s", s, c)
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")), errNotAllowedChannels, "for %s.%s", s, c)
		} else {
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")), errUnauthorizedChannels, "for %s.%s", s, c)
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")), errUnauthorizedChannels, "for %s.%s", s, c)
		}
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "x", "y")), errUnauthorized, "for %s.%s", s, c)
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "y")), errUnauthorized, "for %s.%s", s, c)
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t)), errUnauthorized, "for %s.%s", s, c)
	}

	// User with wildcard access:
	user.setCollectionChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "*", "q"), 1))
	// Legacy default collection checks
	requireExpandWildCardChannel(t, user, nil, []string{"*"})
	requireCannotSeeChannels(t, user, "x", "y", "q", "!", "*")
	require.ErrorIs(t, user.authorizeAllChannels(ch.BaseSetOf(t, "x", "y")), errNotAllowedChannels)
	require.ErrorIs(t, user.authorizeAllChannels(ch.BaseSetOf(t, "*")), errNotAllowedChannels)
	require.ErrorIs(t, user.authorizeAnyChannel(ch.BaseSetOf(t, "x")), errUnauthorized)
	require.ErrorIs(t, user.authorizeAnyChannel(ch.BaseSetOf(t, "*")), errUnauthorized)
	require.ErrorIs(t, user.authorizeAnyChannel(ch.BaseSetOf(t)), errUnauthorized)
	// Matching named collection checks
	requireExpandCollectionWildCardChannels(t, user, scope, collection, []string{"*", "q"}, []string{"*"})
	requireCanSeeCollectionChannels(t, scope, collection, user, "x", "y", "z", "q", "*")
	require.NoError(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "x", "y")))
	require.NoError(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")))
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")))
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "y")))
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "z")))
	require.NoError(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)))
	// Non-matching collection checks
	for _, pair := range nonMatchingCollections {
		s := pair[0]
		c := pair[1]
		requireExpandCollectionWildCardChannels(t, user, s, c, nil, []string{"*"})
		requireCannotSeeCollectionChannels(t, s, c, user, "x", "y", "z", "q", "!", "*")
		if base.IsDefaultCollection(s, c) {
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")), errNotAllowedChannels)
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")), errNotAllowedChannels)
		} else {
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")), errUnauthorizedChannels)
			require.ErrorIs(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")), errUnauthorizedChannels)
		}
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "x", "y")), errUnauthorized)
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "y")), errUnauthorized)
		require.ErrorIs(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t)), errUnauthorized)
	}
}

func TestSerializeUserWithCollections(t *testing.T) {

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	auth := NewTestAuthenticator(t, bucket.GetSingleDataStore(), nil, DefaultAuthenticatorOptions(base.TestCtx(t)))
	user, _ := auth.NewUser("me", "letmein", ch.BaseSetOf(t, "me", "public"))
	encoded, err := base.JSONMarshal(user)
	require.NoError(t, err)
	assert.True(t, encoded != nil)

	// Ensure empty scopes and collections aren't marshalled
	assert.False(t, bytes.Contains(encoded, []byte("collection_access")))
	scope := "testScope"
	collection := "testCollection"
	user.SetCollectionExplicitChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "x"), 1), 1)
	encoded, err = base.JSONMarshal(user)
	require.NoError(t, err)
	log.Printf("Marshaled User as: %s", encoded)

	resu := &userImpl{auth: auth}
	err = base.JSONUnmarshal(encoded, resu)
	require.NoError(t, err)
	collectionAccess, ok := resu.getCollectionAccess(scope, collection)
	require.True(t, ok)
	ch, exists := collectionAccess.ExplicitChannels_["x"]
	require.True(t, exists)
	require.Equal(t, uint64(1), ch.Sequence)

	// Remove all channels for scope and collection
	user.SetCollectionExplicitChannels(scope, collection, nil, 2)
	encoded, err = base.JSONMarshal(user)
	require.NoError(t, err)
	log.Printf("Marshaled User as: %s", encoded)
	resu = &userImpl{auth: auth}
	err = base.JSONUnmarshal(encoded, resu)
	require.NoError(t, err)
	collectionAccess, ok = resu.getCollectionAccess(scope, collection)
	assert.True(t, ok)
	assert.Len(t, collectionAccess.ExplicitChannels_, 0)
	assert.Equal(t, uint64(2), user.getCollectionChannelInvalSeq(scope, collection))
}

func TestPrincipalConfigSetExplicitChannels(t *testing.T) {

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	userName := "bernard"
	config := &PrincipalConfig{
		Name: &userName,
	}

	config.SetExplicitChannels("scope1", "collection1", "ABC", "DEF")
	assert.Equal(t, config.CollectionAccess, map[string]map[string]*CollectionAccessConfig{
		"scope1": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("ABC", "DEF"),
			},
		},
	})

	config.SetExplicitChannels("scope2", "collection1", "GHI")
	assert.Equal(t, config.CollectionAccess, map[string]map[string]*CollectionAccessConfig{
		"scope1": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("ABC", "DEF"),
			},
		},
		"scope2": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("GHI"),
			},
		},
	})
	config.SetExplicitChannels("scope1", "collection2", "JKL")
	assert.Equal(t, config.CollectionAccess, map[string]map[string]*CollectionAccessConfig{
		"scope1": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("ABC", "DEF"),
			},
			"collection2": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("JKL"),
			},
		},
		"scope2": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("GHI"),
			},
		},
	})
	config.SetExplicitChannels("scope1", "collection1", "MNO")
	assert.Equal(t, config.CollectionAccess, map[string]map[string]*CollectionAccessConfig{
		"scope1": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("MNO"),
			},
			"collection2": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("JKL"),
			},
		},
		"scope2": {
			"collection1": &CollectionAccessConfig{
				ExplicitChannels_: base.SetOf("GHI"),
			},
		},
	})

}

// requireExpandCollectionWildCardChannels asserts that the channels will be expanded to the expected channels for the given collection
func requireExpandCollectionWildCardChannels(t *testing.T, user User, scope, collection string, expectedChannels []string, channelsToExpand []string) {
	require.Equal(t, base.SetFromArray(expectedChannels), user.expandCollectionWildCardChannel(scope, collection, base.SetFromArray(channelsToExpand)), "Expected channels %v for %s.%s from %v on user %s", expectedChannels, scope, collection, channelsToExpand, user.Name())
}
