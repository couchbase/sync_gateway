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
	"sync/atomic"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
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

// Verify that upgrading from non-versioned to versioned keys doesn't change the checkpoints when the version hasn't been incremented
func TestDCPMetadataKeyUpgrade(t *testing.T) {

	// Default metadata keys
	defaultMetadataKeys := NewMetadataKeys("")
	// Upgrade check with group ID
	nonVersionedPrefixWithGroup := defaultMetadataKeys.DCPCheckpointPrefix("myGroup")
	versionPrefixWithGroup := defaultMetadataKeys.DCPVersionedCheckpointPrefix("myGroup", 0)
	require.Equal(t, nonVersionedPrefixWithGroup, versionPrefixWithGroup)

	// Upgrade check with empty group ID
	nonVersionedPrefix := defaultMetadataKeys.DCPCheckpointPrefix("")
	versionPrefix := defaultMetadataKeys.DCPVersionedCheckpointPrefix("", 0)
	require.Equal(t, nonVersionedPrefix, versionPrefix)

	// Custom metadata keys
	customMetadataKeys := NewMetadataKeys("foo")

	// Upgrade check with group ID
	nonVersionedPrefixWithGroup = customMetadataKeys.DCPCheckpointPrefix("myGroup")
	versionPrefixWithGroup = customMetadataKeys.DCPVersionedCheckpointPrefix("myGroup", 0)
	require.Equal(t, nonVersionedPrefixWithGroup, versionPrefixWithGroup)

	// Upgrade check with empty group ID
	nonVersionedPrefix = customMetadataKeys.DCPCheckpointPrefix("")
	versionPrefix = customMetadataKeys.DCPVersionedCheckpointPrefix("", 0)
	require.Equal(t, nonVersionedPrefix, versionPrefix)
}

func TestInitSyncInfoErrors(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ds, ok := AsLeakyDataStore(NewLeakyBucket(bucket, LeakyBucketConfig{}).DefaultDataStore())
	require.True(t, ok, "expected leaky bucket to return a leaky data store")

	shouldFailAdd := atomic.Bool{}
	expectedMetadataID := "metadataID"

	missingErrorMsg := "missing"
	if !UnitTestUrlIsWalrus() {
		missingErrorMsg = "not found"
	}
	testCases := []struct {
		name                        string
		expectedError               string
		requiresResync              bool
		requiresAttachmentMigration bool
		addCallback                 func(docID string) (bool, error)
	}{
		{

			name:                        "generic error",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			addCallback: func(docID string) (bool, error) {
				return false, fmt.Errorf("generic error")
			},
			expectedError: "generic error",
		},
		{
			name:                        "single cas error, then empty",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			addCallback: func(docID string) (bool, error) {
				return false, sgbucket.CasMismatchErr{}
			},
			expectedError: missingErrorMsg,
		},
		{
			name:                        "single cas error, get replacement with metadataID=match, no metadataVersion",
			requiresResync:              false,
			requiresAttachmentMigration: true,
			addCallback: func(docID string) (bool, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{MetadataID: Ptr(expectedMetadataID)}
					added, err := ds.Add(docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return false, sgbucket.CasMismatchErr{}
				}
				return false, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=mismatch, no metadataVersion",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			addCallback: func(docID string) (bool, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID: Ptr("another metadataID"),
					}
					added, err := ds.Add(docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return false, sgbucket.CasMismatchErr{}
				}
				return false, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=match, correct metadataVersion",
			requiresResync:              false,
			requiresAttachmentMigration: false,
			addCallback: func(docID string) (bool, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr(expectedMetadataID),
						MetaDataVersion: minimumAttachmentMigrationMetadataVersion,
					}
					added, err := ds.Add(docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return false, sgbucket.CasMismatchErr{}
				}
				return false, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=mismatch, correct metadataVersion",
			requiresResync:              true,
			requiresAttachmentMigration: false,
			addCallback: func(docID string) (bool, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr("another metadataID"),
						MetaDataVersion: minimumAttachmentMigrationMetadataVersion,
					}
					added, err := ds.Add(docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return false, sgbucket.CasMismatchErr{}
				}
				return false, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=match, incorrect metadataVersion",
			requiresResync:              false,
			requiresAttachmentMigration: true,
			addCallback: func(docID string) (bool, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr(expectedMetadataID),
						MetaDataVersion: "3.0.0",
					}
					added, err := ds.Add(docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return false, sgbucket.CasMismatchErr{}
				}
				return false, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=mismatch, incorrect metadataVersion",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			addCallback: func(docID string) (bool, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr("another metadataID"),
						MetaDataVersion: "3.0.0",
					}
					added, err := ds.Add(docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return false, sgbucket.CasMismatchErr{}
				}
				return false, nil
			},
			expectedError: "",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				err := ds.Delete(SGSyncInfo)
				if err != nil {
					RequireDocNotFoundError(t, err)
					return
				}
				assert.NoError(t, err)
			}()
			shouldFailAdd.Store(false)
			ds.config.AddCallback = test.addCallback
			requiresResync, requiresAttachmentMigration, err := InitSyncInfo(ctx, ds, expectedMetadataID)
			if test.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.requiresResync, requiresResync, "Expected requiresResync to be %t", test.requiresResync)
			require.Equal(t, test.requiresAttachmentMigration, requiresAttachmentMigration, "Expected requiresAttachmentMigration to be %t", test.requiresAttachmentMigration)
		})
	}
}
