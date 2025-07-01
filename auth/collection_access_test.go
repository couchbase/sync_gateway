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

func canSeeAllCollectionChannels(t testing.TB, scope, collection string, princ Principal, channels ...string) bool {
	for _, channel := range channels {
		canSee, err := princ.CanSeeCollectionChannel(scope, collection, channel)
		require.NoError(t, err)
		require.True(t, canSee, "expected to be able to see %s.%s channel %q", scope, collection, channel)
	}
	return true
}

func cannotSeeCollectionChannels(t testing.TB, scope, collection string, princ Principal, channels ...string) bool {
	for _, channel := range channels {
		canSee, err := princ.CanSeeCollectionChannel(scope, collection, channel)
		require.NoError(t, err)
		require.False(t, canSee, "expected not to be able to see %s.%s channel %q", scope, collection, channel)
	}
	return true
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
	expandedChannels, err := user.expandWildCardChannel(ch.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.Equal(t, ch.BaseSetOf(t), expandedChannels)
	cannotSeeChannels(t, user, "x", "y", "*", "!")
	assert.False(t, user.authorizeAllChannels(ch.BaseSetOf(t, "*")) == nil)
	assert.False(t, user.authorizeAnyChannel(ch.BaseSetOf(t, "x", "y")) == nil)
	assert.False(t, user.authorizeAnyChannel(ch.BaseSetOf(t)) == nil)
	// Named collection checks
	expandedChannels, err = user.expandCollectionWildCardChannel(scope, collection, ch.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.Equal(t, ch.BaseSetOf(t, "!"), expandedChannels)
	cannotSeeCollectionChannels(t, scope, collection, user, "x", "y", "*")
	assert.False(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")) == nil)
	assert.False(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)) == nil)

	// User with access to one channel in named collection:
	user.setCollectionChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "x"), 1))
	// Matching named collection checks
	expandedChannels, err = user.expandCollectionWildCardChannel(scope, collection, ch.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.Equal(t, ch.BaseSetOf(t, "x"), expandedChannels)
	canSeeAllCollectionChannels(t, scope, collection, user, "x")
	cannotSeeCollectionChannels(t, scope, collection, user, "y")
	assert.False(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.False(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "y")) == nil)
	assert.False(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)) == nil)

	// Non-matching collection checks
	for _, pair := range nonMatchingCollections {
		s := pair[0]
		c := pair[1]
		expandedChannels, err := user.expandCollectionWildCardChannel(s, c, ch.BaseSetOf(t, "*"))
		require.NoError(t, err)
		assert.Equal(t, ch.BaseSetOf(t), expandedChannels)
		cannotSeeCollectionChannels(t, s, c, user, "x", "y")
		assert.False(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")) == nil)
		assert.False(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "x", "y")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "y")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t)) == nil)
	}

	// User with access to two channels:
	// User with access to one channel in named collection:
	user.setCollectionChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "x", "y"), 1))
	// Matching named collection checks
	expandedChannels, err = user.expandCollectionWildCardChannel(scope, collection, ch.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.Equal(t, ch.BaseSetOf(t, "x", "y"), expandedChannels)
	canSeeAllCollectionChannels(t, scope, collection, user, "x", "y")
	cannotSeeCollectionChannels(t, scope, collection, user, "z")
	assert.True(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.False(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "y")) == nil)
	assert.False(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "z")) == nil)
	assert.False(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)) == nil)
	// Non-matching collection checks
	for _, pair := range nonMatchingCollections {
		s := pair[0]
		c := pair[1]
		expandedChannels, err := user.expandCollectionWildCardChannel(s, c, ch.BaseSetOf(t, "*"))
		require.NoError(t, err)
		assert.Equal(t, ch.BaseSetOf(t), expandedChannels)
		cannotSeeCollectionChannels(t, s, c, user, "x", "y")
		assert.False(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")) == nil)
		assert.False(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "x", "y")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "y")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t)) == nil)
	}

	// User with wildcard access:
	user.setCollectionChannels(scope, collection, ch.AtSequence(ch.BaseSetOf(t, "*", "q"), 1))
	// Legacy default collection checks
	expandedChannels, err = user.expandWildCardChannel(ch.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.Equal(t, ch.BaseSetOf(t), expandedChannels)
	cannotSeeChannels(t, user, "*", "x", "y")
	assert.False(t, user.authorizeAllChannels(ch.BaseSetOf(t, "x", "y")) == nil)
	assert.False(t, user.authorizeAllChannels(ch.BaseSetOf(t, "*")) == nil)
	assert.False(t, user.authorizeAnyChannel(ch.BaseSetOf(t, "x")) == nil)
	assert.False(t, user.authorizeAnyChannel(ch.BaseSetOf(t, "*")) == nil)
	assert.False(t, user.authorizeAnyChannel(ch.BaseSetOf(t)) == nil)
	// Matching named collection checks
	expandedChannels, err = user.expandCollectionWildCardChannel(scope, collection, ch.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.Equal(t, ch.BaseSetOf(t, "*", "q"), expandedChannels)
	canSeeAllCollectionChannels(t, scope, collection, user, "x", "y", "z")
	assert.True(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.True(t, user.authorizeAllCollectionChannels(scope, collection, ch.BaseSetOf(t, "*")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "x", "y")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "y")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t, "z")) == nil)
	assert.True(t, user.AuthorizeAnyCollectionChannel(scope, collection, ch.BaseSetOf(t)) == nil)
	// Non-matching collection checks
	for _, pair := range nonMatchingCollections {
		s := pair[0]
		c := pair[1]
		expandedChannels, err := user.expandCollectionWildCardChannel(s, c, ch.BaseSetOf(t, "*"))
		require.NoError(t, err)
		assert.Equal(t, ch.BaseSetOf(t), expandedChannels)
		cannotSeeCollectionChannels(t, s, c, user, "x", "y")
		assert.False(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "x", "y")) == nil)
		assert.False(t, user.authorizeAllCollectionChannels(s, c, ch.BaseSetOf(t, "*")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "x", "y")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t, "y")) == nil)
		assert.False(t, user.AuthorizeAnyCollectionChannel(s, c, ch.BaseSetOf(t)) == nil)
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
	assert.True(t, ch.Sequence == 1)

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
