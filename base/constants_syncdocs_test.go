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

func TestMetadataKeys(t *testing.T) {
	testCases := []struct {
		metadataID                       string
		groupID                          string
		syncSeqKey                       string
		unusedSeqKey                     string // with seq 1
		unusedSeqPrefix                  string
		replicationStatusKey             string
		heartbeaterKey                   string
		sgCfgPrefix                      string
		persistentConfigKey              string
		unusedSeqRangeKey                string // from seq 1 to seq 2
		dcpCheckpointPrefix              string
		dcpVersionedCheckpointPrefix     string
		userKey                          string
		userKeyPrefix                    string
		roleKey                          string
		roleKeyPrefix                    string
		userEmailKey                     string
		sessionKey                       string
		backgroundProcessHeartbeatPrefix string // with backgroundID
		backgroundProcessStatusPrefix    string // with backgroundID
		resyncHeartbeatPrefix            string
		resyncCfgPrefix                  string
		databaseStateKey                 string
	}{
		{
			metadataID:                       "",
			groupID:                          "",
			heartbeaterKey:                   "_sync:",
			syncSeqKey:                       "_sync:seq",
			unusedSeqKey:                     "_sync:unusedSeq:1",
			unusedSeqPrefix:                  "_sync:unusedSeq:",
			replicationStatusKey:             "_sync:sgrStatus:",
			sgCfgPrefix:                      "_sync:cfg",
			persistentConfigKey:              "default",
			unusedSeqRangeKey:                "_sync:unusedSeqs:1:2",
			dcpCheckpointPrefix:              "_sync:dcp_ck:",
			dcpVersionedCheckpointPrefix:     "_sync:dcp_ck:10:",
			userKey:                          "_sync:user:aUser",
			userKeyPrefix:                    "_sync:user:",
			roleKey:                          "_sync:role:aRole",
			roleKeyPrefix:                    "_sync:role:",
			userEmailKey:                     "_sync:useremail:alice@couchbase.com",
			sessionKey:                       "_sync:session:aSessionID",
			backgroundProcessHeartbeatPrefix: "_sync:background_process:heartbeat:backgroundID",
			backgroundProcessStatusPrefix:    "_sync:background_process:status:backgroundID",
			resyncHeartbeatPrefix:            "_sync:resync_hb:",
			resyncCfgPrefix:                  "_sync:resync_cfg:",
			databaseStateKey:                 "_sync:state",
		},
		{
			metadataID:                       "",
			groupID:                          "aGroupID",
			heartbeaterKey:                   "_sync:aGroupID:",
			syncSeqKey:                       "_sync:seq",
			unusedSeqKey:                     "_sync:unusedSeq:1",
			unusedSeqPrefix:                  "_sync:unusedSeq:",
			replicationStatusKey:             "_sync:sgrStatus:aGroupID",
			sgCfgPrefix:                      "_sync:cfgaGroupID:",
			persistentConfigKey:              "aGroupID",
			unusedSeqRangeKey:                "_sync:unusedSeqs:1:2",
			dcpCheckpointPrefix:              "_sync:dcp_ck:aGroupID:",
			dcpVersionedCheckpointPrefix:     "_sync:dcp_ck:aGroupID:10:",
			userKey:                          "_sync:user:aUser",
			userKeyPrefix:                    "_sync:user:",
			roleKey:                          "_sync:role:aRole",
			roleKeyPrefix:                    "_sync:role:",
			userEmailKey:                     "_sync:useremail:alice@couchbase.com",
			sessionKey:                       "_sync:session:aSessionID",
			backgroundProcessHeartbeatPrefix: "_sync:background_process:heartbeat:backgroundID",
			backgroundProcessStatusPrefix:    "_sync:background_process:status:backgroundID",
			resyncHeartbeatPrefix:            "_sync:resync_hb:",
			resyncCfgPrefix:                  "_sync:resync_cfg:",
			databaseStateKey:                 "_sync:state",
		},
		{
			metadataID:                       "aMetadataID",
			groupID:                          "",
			heartbeaterKey:                   "_sync:m_aMetadataID:hb:",
			syncSeqKey:                       "_sync:m_aMetadataID:seq",
			unusedSeqKey:                     "_sync:m_aMetadataID:unusedSeq:1",
			unusedSeqPrefix:                  "_sync:m_aMetadataID:unusedSeq:",
			replicationStatusKey:             "_sync:m_aMetadataID:sgrStatus:",
			sgCfgPrefix:                      "_sync:m_aMetadataID:cfg",
			persistentConfigKey:              "default",
			unusedSeqRangeKey:                "_sync:m_aMetadataID:unusedSeqs:1:2",
			dcpCheckpointPrefix:              "_sync:dcp_ck:aMetadataID:",
			dcpVersionedCheckpointPrefix:     "_sync:dcp_ck:aMetadataID:10:",
			userKey:                          "_sync:user:aMetadataID:aUser",
			userKeyPrefix:                    "_sync:user:aMetadataID:",
			roleKey:                          "_sync:role:aMetadataID:aRole",
			roleKeyPrefix:                    "_sync:role:aMetadataID:",
			userEmailKey:                     "_sync:useremail:aMetadataID:alice@couchbase.com",
			sessionKey:                       "_sync:session:aMetadataID:aSessionID",
			backgroundProcessHeartbeatPrefix: "_sync:m_aMetadataID:background_process:heartbeat:backgroundID",
			backgroundProcessStatusPrefix:    "_sync:m_aMetadataID:background_process:status:backgroundID",
			resyncHeartbeatPrefix:            "_sync:m_aMetadataID:resync_hb:",
			resyncCfgPrefix:                  "_sync:m_aMetadataID:resync_cfg:",
			databaseStateKey:                 "_sync:m_aMetadataID:state",
		},
		{
			metadataID:                       "aMetadataID",
			groupID:                          "aGroupID",
			heartbeaterKey:                   "_sync:m_aMetadataID:hb:aGroupID:",
			syncSeqKey:                       "_sync:m_aMetadataID:seq",
			unusedSeqKey:                     "_sync:m_aMetadataID:unusedSeq:1",
			unusedSeqPrefix:                  "_sync:m_aMetadataID:unusedSeq:",
			replicationStatusKey:             "_sync:m_aMetadataID:sgrStatus:aGroupID",
			sgCfgPrefix:                      "_sync:m_aMetadataID:cfgaGroupID:",
			persistentConfigKey:              "aGroupID",
			unusedSeqRangeKey:                "_sync:m_aMetadataID:unusedSeqs:1:2",
			dcpCheckpointPrefix:              "_sync:dcp_ck:aMetadataID:aGroupID:",
			dcpVersionedCheckpointPrefix:     "_sync:dcp_ck:aMetadataID:aGroupID:10:",
			userKey:                          "_sync:user:aMetadataID:aUser",
			userKeyPrefix:                    "_sync:user:aMetadataID:",
			roleKey:                          "_sync:role:aMetadataID:aRole",
			roleKeyPrefix:                    "_sync:role:aMetadataID:",
			userEmailKey:                     "_sync:useremail:aMetadataID:alice@couchbase.com",
			sessionKey:                       "_sync:session:aMetadataID:aSessionID",
			backgroundProcessHeartbeatPrefix: "_sync:m_aMetadataID:background_process:heartbeat:backgroundID",
			backgroundProcessStatusPrefix:    "_sync:m_aMetadataID:background_process:status:backgroundID",
			resyncHeartbeatPrefix:            "_sync:m_aMetadataID:resync_hb:",
			resyncCfgPrefix:                  "_sync:m_aMetadataID:resync_cfg:",
			databaseStateKey:                 "_sync:m_aMetadataID:state",
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("MetadataID=%s,GroupID=%s", test.metadataID, test.groupID), func(t *testing.T) {
			metadataKeys := NewMetadataKeys(test.metadataID)
			require.Equal(t, test.syncSeqKey, metadataKeys.SyncSeqKey())
			require.Equal(t, test.unusedSeqKey, metadataKeys.UnusedSeqKey(1))
			require.Equal(t, test.unusedSeqPrefix, metadataKeys.UnusedSeqPrefix())
			require.Equal(t, test.replicationStatusKey, metadataKeys.ReplicationStatusKey(test.groupID))
			require.Equal(t, test.heartbeaterKey, metadataKeys.HeartbeaterPrefix(test.groupID))
			require.Equal(t, test.sgCfgPrefix, metadataKeys.SGCfgPrefix(test.groupID))
			if test.groupID == "" {
				// need a groupID for this, specifying default
				persistentConfigKey, err := metadataKeys.PersistentConfigKey("default")
				require.NoError(t, err)
				require.Equal(t, test.persistentConfigKey, persistentConfigKey)
			} else {
				persistentConfigKey, err := metadataKeys.PersistentConfigKey(test.groupID)
				require.NoError(t, err)
				require.Equal(t, test.persistentConfigKey, persistentConfigKey)
			}
			require.Equal(t, test.unusedSeqRangeKey, metadataKeys.UnusedSeqRangeKey(1, 2))
			require.Equal(t, test.dcpCheckpointPrefix, metadataKeys.DCPCheckpointPrefix(test.groupID))
			require.Equal(t, test.dcpVersionedCheckpointPrefix, metadataKeys.DCPVersionedCheckpointPrefix(test.groupID, 10))
			require.Equal(t, test.userKey, metadataKeys.UserKey("aUser"))
			require.Equal(t, test.userKeyPrefix, metadataKeys.UserKeyPrefix())
			require.Equal(t, test.roleKey, metadataKeys.RoleKey("aRole"))
			require.Equal(t, test.roleKeyPrefix, metadataKeys.RoleKeyPrefix())
			require.Equal(t, test.userEmailKey, metadataKeys.UserEmailKey("alice@couchbase.com"))
			require.Equal(t, test.sessionKey, metadataKeys.SessionKey("aSessionID"))
			require.Equal(t, test.backgroundProcessHeartbeatPrefix, metadataKeys.BackgroundProcessHeartbeatPrefix("backgroundID"))
			require.Equal(t, test.backgroundProcessStatusPrefix, metadataKeys.BackgroundProcessStatusPrefix("backgroundID"))
			require.Equal(t, test.resyncHeartbeatPrefix, metadataKeys.ResyncHeartbeaterPrefix())
			require.Equal(t, test.resyncCfgPrefix, metadataKeys.ResyncCfgPrefix())
			require.Equal(t, test.databaseStateKey, metadataKeys.DatabaseStateKey())
		})
	}
}

func TestInitSyncInfoErrors(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	ds, ok := AsLeakyDataStore(NewLeakyBucket(bucket, LeakyBucketConfig{}).DefaultDataStore(ctx))
	require.True(t, ok, "expected leaky bucket to return a leaky data store")

	shouldFailAdd := atomic.Bool{}
	expectedMetadataID := "metadataID"

	testCases := []struct {
		name                        string
		expectedError               string
		requiresResync              bool
		requiresAttachmentMigration bool
		writeCasCallback            func(docID string) (uint64, error)
	}{
		{

			name:                        "generic error",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			writeCasCallback: func(docID string) (uint64, error) {
				return 0, fmt.Errorf("generic error")
			},
			expectedError: "generic error",
		},
		{
			name:                        "single cas error, then empty",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			writeCasCallback: func(docID string) (uint64, error) {
				return 0, sgbucket.CasMismatchErr{}
			},
			expectedError: "syncInfo missing after CAS mismatch on add",
		},
		{
			name:                        "single cas error, get replacement with metadataID=match, no metadataVersion",
			requiresResync:              false,
			requiresAttachmentMigration: true,
			writeCasCallback: func(docID string) (uint64, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{MetadataID: Ptr(expectedMetadataID)}
					added, err := ds.Add(ctx, docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return 0, sgbucket.CasMismatchErr{}
				}
				return 0, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=mismatch, no metadataVersion",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			writeCasCallback: func(docID string) (uint64, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID: Ptr("another metadataID"),
					}
					added, err := ds.Add(ctx, docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return 0, sgbucket.CasMismatchErr{}
				}
				return 0, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=match, correct metadataVersion",
			requiresResync:              false,
			requiresAttachmentMigration: false,
			writeCasCallback: func(docID string) (uint64, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr(expectedMetadataID),
						MetaDataVersion: minimumAttachmentMigrationMetadataVersion,
					}
					added, err := ds.Add(ctx, docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return 0, sgbucket.CasMismatchErr{}
				}
				return 0, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=mismatch, correct metadataVersion",
			requiresResync:              true,
			requiresAttachmentMigration: false,
			writeCasCallback: func(docID string) (uint64, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr("another metadataID"),
						MetaDataVersion: minimumAttachmentMigrationMetadataVersion,
					}
					added, err := ds.Add(ctx, docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return 0, sgbucket.CasMismatchErr{}
				}
				return 0, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=match, incorrect metadataVersion",
			requiresResync:              false,
			requiresAttachmentMigration: true,
			writeCasCallback: func(docID string) (uint64, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr(expectedMetadataID),
						MetaDataVersion: "3.0.0",
					}
					added, err := ds.Add(ctx, docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return 0, sgbucket.CasMismatchErr{}
				}
				return 0, nil
			},
			expectedError: "",
		},
		{
			name:                        "single cas error, get replacement with metadataID=mismatch, incorrect metadataVersion",
			requiresResync:              true,
			requiresAttachmentMigration: true,
			writeCasCallback: func(docID string) (uint64, error) {
				if shouldFailAdd.CompareAndSwap(false, true) {
					newSyncInfo := &SyncInfo{
						MetadataID:      Ptr("another metadataID"),
						MetaDataVersion: "3.0.0",
					}
					added, err := ds.Add(ctx, docID, 0, newSyncInfo)
					require.True(t, added)
					require.NoError(t, err)
					return 0, sgbucket.CasMismatchErr{}
				}
				return 0, nil
			},
			expectedError: "",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				err := ds.Delete(ctx, SGSyncInfo)
				if err != nil {
					RequireDocNotFoundError(t, err)
					return
				}
				assert.NoError(t, err)
			}()
			shouldFailAdd.Store(false)
			ds.config.WriteCasCallback = test.writeCasCallback
			requiresResync, requiresAttachmentMigration, err := InitSyncInfo(ctx, ds, expectedMetadataID, nil)
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

func TestInitSyncInfoBinaryFormat(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	ds := bucket.DefaultDataStore(ctx)

	ccv40 := NewClusterCompatVersion(4, 0)
	ccv41 := NewClusterCompatVersion(4, 1)

	resetDoc := func(t *testing.T) {
		err := ds.Delete(ctx, SGSyncInfo)
		if err != nil && !IsDocNotFoundError(err) {
			require.NoError(t, err)
		}
	}

	t.Run("no doc, ccv 4.1 writes V1 prefix", func(t *testing.T) {
		resetDoc(t)
		_, _, err := InitSyncInfo(ctx, ds, "x", &ccv41)
		require.NoError(t, err)
		raw, _, err := ds.GetRaw(ctx, SGSyncInfo)
		require.NoError(t, err)
		require.NotEmpty(t, raw)
		require.Equal(t, byte(SyncInfoTypeV1), raw[0], "expected V1 prefix byte")
		decoded, err := DecodeSyncInfo(raw)
		require.NoError(t, err)
		require.Equal(t, "x", *decoded.MetadataID)
	})

	t.Run("no doc, ccv 4.0 writes legacy JSON", func(t *testing.T) {
		resetDoc(t)
		_, _, err := InitSyncInfo(ctx, ds, "x", &ccv40)
		require.NoError(t, err)
		raw, _, err := ds.GetRaw(ctx, SGSyncInfo)
		require.NoError(t, err)
		require.NotEmpty(t, raw)
		require.Equal(t, byte('{'), raw[0], "expected legacy JSON")
	})

	t.Run("V1 prefixed doc readable with ccv=nil (forward-compat)", func(t *testing.T) {
		resetDoc(t)
		payload := append([]byte{byte(SyncInfoTypeV1)}, []byte(`{"metadataID":"x"}`)...)
		require.NoError(t, ds.SetRaw(ctx, SGSyncInfo, 0, nil, payload))
		requiresResync, _, err := InitSyncInfo(ctx, ds, "x", nil)
		require.NoError(t, err)
		require.False(t, requiresResync)
	})

	t.Run("V1 prefixed doc round-trip at ccv=4.1", func(t *testing.T) {
		resetDoc(t)
		payload := append([]byte{byte(SyncInfoTypeV1)}, []byte(`{"metadataID":"x","metadata_version":"4.0.0"}`)...)
		require.NoError(t, ds.SetRaw(ctx, SGSyncInfo, 0, nil, payload))
		requiresResync, requiresAttachmentMigration, err := InitSyncInfo(ctx, ds, "x", &ccv41)
		require.NoError(t, err)
		require.False(t, requiresResync)
		require.False(t, requiresAttachmentMigration)
	})

	t.Run("corrupt leading byte propagates decode error", func(t *testing.T) {
		resetDoc(t)
		payload := append([]byte{0xff}, []byte(`{"metadataID":"x"}`)...)
		require.NoError(t, ds.SetRaw(ctx, SGSyncInfo, 0, nil, payload))
		_, _, err := InitSyncInfo(ctx, ds, "x", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unrecognized syncInfo version byte")
	})
}

func TestSetSyncInfoBinaryFormat(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	ds := bucket.DefaultDataStore(ctx)

	ccv41 := NewClusterCompatVersion(4, 1)

	resetDoc := func(t *testing.T) {
		err := ds.Delete(ctx, SGSyncInfo)
		if err != nil && !IsDocNotFoundError(err) {
			require.NoError(t, err)
		}
	}

	t.Run("legacy JSON doc upgraded to V1 on SetSyncInfoMetadataID at ccv 4.1", func(t *testing.T) {
		resetDoc(t)
		require.NoError(t, ds.SetRaw(ctx, SGSyncInfo, 0, nil, []byte(`{"metadataID":"y","metadata_version":"4.0.0"}`)))

		require.NoError(t, SetSyncInfoMetadataID(ctx, ds, "x", &ccv41))

		raw, _, err := ds.GetRaw(ctx, SGSyncInfo)
		require.NoError(t, err)
		require.NotEmpty(t, raw)
		require.Equal(t, byte(SyncInfoTypeV1), raw[0], "expected V1 prefix byte after Set at ccv 4.1")
		decoded, err := DecodeSyncInfo(raw)
		require.NoError(t, err)
		require.Equal(t, "x", *decoded.MetadataID, "metadataID should be updated")
		require.Equal(t, "4.0.0", decoded.MetaDataVersion, "metaVersion should be preserved")
	})

	t.Run("legacy JSON doc upgraded to V1 on SetSyncInfoMetaVersion at ccv 4.1", func(t *testing.T) {
		resetDoc(t)
		require.NoError(t, ds.SetRaw(ctx, SGSyncInfo, 0, nil, []byte(`{"metadataID":"y","metadata_version":"3.0.0"}`)))

		require.NoError(t, SetSyncInfoMetaVersion(ctx, ds, "4.0.0", &ccv41))

		raw, _, err := ds.GetRaw(ctx, SGSyncInfo)
		require.NoError(t, err)
		require.NotEmpty(t, raw)
		require.Equal(t, byte(SyncInfoTypeV1), raw[0], "expected V1 prefix byte after Set at ccv 4.1")
		decoded, err := DecodeSyncInfo(raw)
		require.NoError(t, err)
		require.Equal(t, "y", *decoded.MetadataID, "metadataID should be preserved")
		require.Equal(t, "4.0.0", decoded.MetaDataVersion, "metaVersion should be updated")
	})

	t.Run("corrupt doc surfaces decode error via Set", func(t *testing.T) {
		resetDoc(t)
		require.NoError(t, ds.SetRaw(ctx, SGSyncInfo, 0, nil, append([]byte{0xff}, []byte(`{"metadataID":"y"}`)...)))

		err := SetSyncInfoMetadataID(ctx, ds, "x", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unrecognized syncInfo version byte")
	})

	t.Run("empty arg is a no-op", func(t *testing.T) {
		resetDoc(t)

		require.NoError(t, SetSyncInfoMetadataID(ctx, ds, "", nil))
		require.NoError(t, SetSyncInfoMetaVersion(ctx, ds, "", nil))

		_, _, err := ds.GetRaw(ctx, SGSyncInfo)
		require.True(t, IsDocNotFoundError(err), "expected no syncInfo doc to be written, got err=%v", err)
	})
}

func TestDecodeSyncInfo(t *testing.T) {
	populated := SyncInfo{MetadataID: Ptr("x"), MetaDataVersion: "4.0.0"}
	populatedJSON := `{"metadataID":"x","metadata_version":"4.0.0"}`

	testCases := []struct {
		name        string
		input       []byte
		expected    SyncInfo
		expectedErr string
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: SyncInfo{},
		},
		{
			name:     "legacy JSON",
			input:    []byte(populatedJSON),
			expected: populated,
		},
		{
			name:     "V1 prefix + JSON",
			input:    append([]byte{byte(SyncInfoTypeV1)}, []byte(populatedJSON)...),
			expected: populated,
		},
		{
			name:        "V1 prefix + malformed JSON",
			input:       append([]byte{byte(SyncInfoTypeV1)}, []byte(`{not json`)...),
			expectedErr: "Error unmarshalling syncInfo",
		},
		{
			name:        "unknown discriminator 0xFF",
			input:       append([]byte{0xff}, []byte(populatedJSON)...),
			expectedErr: "unrecognized syncInfo version byte",
		},
		{
			name:        "SyncInfoTypeUnknown (0x00) is not silently accepted",
			input:       append([]byte{byte(SyncInfoTypeUnknown)}, []byte(populatedJSON)...),
			expectedErr: "unrecognized syncInfo version byte",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeSyncInfo(tc.input)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}
