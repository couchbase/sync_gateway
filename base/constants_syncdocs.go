/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"fmt"
)

const SyncDocPrefix = "_sync:" // SyncDocPrefix is a common prefix for all Sync Gateway metadata documents

// Sync Gateway Metadata documents
const (
	SyncSeqKey                       = SyncDocPrefix + "seq"                           // SyncSeqKey is a counter document storing the SG sequence number of a collection
	UnusedSeqPrefix                  = SyncDocPrefix + "unusedSeq:"                    // UnusedSeqPrefix stores a sequence that was reserved by SG but was unused
	UnusedSeqRangePrefix             = SyncDocPrefix + "unusedSeqs:"                   // UnusedSeqRangePrefix stores a sequence range that was reserved by SG but was unused
	RevBodyPrefix                    = SyncDocPrefix + "rb:"                           // RevBodyPrefix stores a conflicting revision's body
	RevPrefix                        = SyncDocPrefix + "rev:"                          // RevPrefix stores an old revision's body for temporary lookup (in-flight requests or delta sync)
	BackgroundProcessHeartbeatPrefix = SyncDocPrefix + "background_process:heartbeat:" // BackgroundProcessHeartbeatPrefix stores a background process heartbeat
	BackgroundProcessStatusPrefix    = SyncDocPrefix + "background_process:status:"    // BackgroundProcessStatusPrefix stores a background process status
	UserPrefix                       = SyncDocPrefix + "user:"                         // UserPrefix stores user documents keyed by user name
	RolePrefix                       = SyncDocPrefix + "role:"                         // RolePrefix stores role documents keyed by role name
	UserEmailPrefix                  = SyncDocPrefix + "useremail:"                    // UserEmailPrefix maps a user's email to a user name for UserPrefix lookups
	SessionPrefix                    = SyncDocPrefix + "session:"                      // SessionPrefix stores user sessions keyed by session ID
	AttPrefix                        = SyncDocPrefix + "att:"                          // AttPrefix SG (v1) attachment data
	Att2Prefix                       = SyncDocPrefix + "att2:"                         // Att2Prefix SG v2 attachment data
	DCPBackfillSeqKey                = SyncDocPrefix + "dcp_backfill"                  // DCPBackfillSeqKey stores a BackfillSequences for a DCP feed
	RepairBackupPrefix               = SyncDocPrefix + "repair:backup:"                // RepairBackupPrefix is the doc prefix used to store a backup of a repaired document
	RepairDryRunPrefix               = SyncDocPrefix + "repair:dryrun:"                // RepairDryRunPrefix is the doc prefix used to store a repaired document in dry-run mode
	SGRStatusPrefix                  = SyncDocPrefix + "sgrStatus:"                    // SGRStatusPrefix is the doc prefix used to store ISGR status documents
)

// Sync Gateway Metadata documents that should be GroupID scoped and accessed via the "WithGroupID" helper methods below
const (
	DCPCheckpointPrefixWithoutGroupID       = SyncDocPrefix + "dcp_ck:"             // DCPCheckpointPrefixWithoutGroupID stores a DCP checkpoint
	SGCfgPrefixWithoutGroupID               = SyncDocPrefix + "cfg"                 // SGCfgPrefixWithoutGroupID stores CfgSG/cbgt data
	HeartbeaterPrefixWithoutGroupID         = SyncDocPrefix                         // HeartbeaterPrefixWithoutGroupID stores a SG node heartbeat document
	PersistentConfigPrefixWithoutGroupID    = SyncDocPrefix + "dbconfig:"           // PersistentConfigPrefixWithoutGroupID stores a database config
	SyncFunctionKeyWithoutGroupID           = SyncDocPrefix + "syncdata"            // SyncFunctionKeyWithoutGroupID stores a copy of the Sync Function
	CollectionSyncFunctionKeyWithoutGroupID = SyncDocPrefix + "syncdata_collection" // CollectionSyncFunctionKeyWithoutGroupID stores a copy of the Sync Function
)

// SyncFunctionKeyWithGroupID returns a doc ID to use when storing the sync function
func SyncFunctionKeyWithGroupID(groupID string) string {
	if groupID != "" {
		return SyncFunctionKeyWithoutGroupID + ":" + groupID
	}
	return SyncFunctionKeyWithoutGroupID
}

// SyncFunctionKeyWithGroupID returns a doc ID to use when storing the sync function
func CollectionSyncFunctionKeyWithGroupID(groupID string, collectionID uint32) string {
	if groupID != "" {
		return fmt.Sprintf("%s:%d:%s", CollectionSyncFunctionKeyWithoutGroupID, collectionID, groupID)
	}
	return fmt.Sprintf("%s:%d", CollectionSyncFunctionKeyWithoutGroupID, collectionID)
}

// DCPCheckpointPrefixWithGroupID returns a doc ID prefix to use for DCP checkpoints
func DCPCheckpointPrefixWithGroupID(groupID string) string {
	if groupID != "" {
		return DCPCheckpointPrefixWithoutGroupID + groupID + ":"
	}
	return DCPCheckpointPrefixWithoutGroupID
}

// SGCfgPrefixWithGroupID returns a doc ID prefix to use for CfgSG/cbgt documents
func SGCfgPrefixWithGroupID(groupID string) string {
	if groupID != "" {
		return SGCfgPrefixWithoutGroupID + ":" + groupID + ":"
	}
	return SGCfgPrefixWithoutGroupID
}

// HeartbeaterPrefixWithGroupID returns a document prefix to use for heartbeat documents
func HeartbeaterPrefixWithGroupID(groupID string) string {
	if groupID != "" {
		return HeartbeaterPrefixWithoutGroupID + groupID + ":"
	}
	return HeartbeaterPrefixWithoutGroupID
}

// PersistentConfigKey returns a document key to use to store database configs
func PersistentConfigKey(groupID string) (string, error) {
	if groupID == "" {
		return "", errors.New("PersistentConfigKey requires a group ID, even if it's just `default`.")
	}
	return PersistentConfigPrefixWithoutGroupID + groupID, nil
}
