// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionSyncFunctionKeyWithGroupID(t *testing.T) {
	testCases := []struct {
		scopeName      string
		collectionName string
		groupID        string
		key            string
	}{
		{
			scopeName:      DefaultScope,
			collectionName: DefaultCollection,
			key:            "_sync:syncdata",
		},
		{
			scopeName:      DefaultScope,
			collectionName: DefaultCollection,
			groupID:        "1",
			key:            "_sync:syncdata:1",
		},
		{
			scopeName:      "fooscope",
			collectionName: "barcollection",
			key:            "_sync:syncdata_collection:fooscope.barcollection",
		},
		{
			scopeName:      "fooscope",
			collectionName: "barcollection",
			groupID:        "1",
			key:            "_sync:syncdata_collection:fooscope.barcollection:1",
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s.%s_GroupID:%s", test.scopeName, test.collectionName, test.groupID), func(t *testing.T) {
			require.Equal(t, test.key, CollectionSyncFunctionKeyWithGroupID(test.groupID, test.scopeName, test.collectionName))
		})
	}
}

func TestMetaKeyNames(t *testing.T) {

	// Validates that metadata keys aren't prefixed with MetadataIdPrefix
	for _, metaKeyName := range metadataKeyNames {
		assert.False(t, strings.HasPrefix(metaKeyName, MetadataIdPrefix))
	}
}

func TestMetadataKeyHash(t *testing.T) {
	defaultMetadataKeys := NewMetadataKeys("")
	customMetadataKeys := NewMetadataKeys("foo")

	// normal user name
	bob := "bob"
	require.Equal(t, "_sync:user:bob", defaultMetadataKeys.UserKey(bob))
	require.Equal(t, "_sync:user:foo:bob", customMetadataKeys.UserKey(bob))

	// username one less than hash length
	user39 := strings.Repeat("b", 39)
	require.Equal(t, "_sync:user:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", defaultMetadataKeys.UserKey(user39))
	require.Equal(t, "_sync:user:foo:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", customMetadataKeys.UserKey(user39))

	// username equal to hash length
	user40 := strings.Repeat("b", 40)
	require.Equal(t, "_sync:user:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", defaultMetadataKeys.UserKey(user40))
	require.Equal(t, "_sync:user:foo:56e892b750b146f39a486af8c31d555e9c771b3e", customMetadataKeys.UserKey(user40))

	// make sure hashed user won't get rehashed
	hashedUser := "56e892b750b146f39a486af8c31d555e9c771b3e"
	require.Equal(t, "_sync:user:56e892b750b146f39a486af8c31d555e9c771b3e", defaultMetadataKeys.UserKey(hashedUser))
	require.Equal(t, "_sync:user:foo:afbf3a596bfe3e6687240e011bfccafd51611052", customMetadataKeys.UserKey(hashedUser))

}
