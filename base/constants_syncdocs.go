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
	"strconv"
)

const SyncDocPrefix = "_sync:"                                 // Prefix for all legacy (non-namespaced) Sync Gateway metadata documents
const MetadataIdPrefix = "m_"                                  // Prefix for metadataId, to prevent collisions between namespaced and non-namespaced metadata documents
const SyncDocMetadataPrefix = SyncDocPrefix + MetadataIdPrefix // Prefix for all namespaced Sync Gateway metadata documents

// Sync Gateway Metadata document types
type metadataKey int

const (
	MetaKeySeq            metadataKey = iota // "seq"
	MetaKeyUnusedSeq                         // "unusedSeq:"
	MetaKeyUnusedSeqRange                    // "unusedSeqs:"
)

var metadataKeyNames = []string{
	"seq",         // counter document storing the sequence number for a database
	"unusedSeq:",  // stores a sequence that was reserved by SG but was unused
	"unusedSeqs:", // stores a sequence range that was reserved by SG but was unused
}

func (m metadataKey) String() string {
	return metadataKeyNames[m]
}

const (
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

// MetadataKeys defines metadata keys and prefixes for a database, for a given metadataID.  Each Sync Gateway database
// uses a unique metadataID, but may share the same MetadataStore.  MetadataKeys implements key namespacing for that
// MetadataStore.
type MetadataKeys struct {
	metadataID           string
	syncSeq              string
	unusedSeqPrefix      string
	unusedSeqRangePrefix string
}

// DefaultMetadataKeys defines the legacy metadata keys and prefixes.  These are used when the metadata collection is the only
// defined collection, including upgrade scenarios.
var DefaultMetadataKeys = &MetadataKeys{
	metadataID:           "",
	syncSeq:              formatDefaultMetadataKey(MetaKeySeq),
	unusedSeqPrefix:      formatDefaultMetadataKey(MetaKeyUnusedSeq),
	unusedSeqRangePrefix: formatDefaultMetadataKey(MetaKeyUnusedSeqRange),
}

// NewMetadataKeys returns MetadataKeys for the specified MetadataID  If metadataID is empty string, returns the default (legacy) metadata keys.
// Key and prefix formatting is done in this constructor to minimize the work done per retrieval.
func NewMetadataKeys(metadataID string) *MetadataKeys {
	if metadataID == "" {
		return DefaultMetadataKeys
	} else {
		return &MetadataKeys{
			metadataID:           metadataID,
			syncSeq:              formatMetadataKey(metadataID, MetaKeySeq),
			unusedSeqPrefix:      formatMetadataKey(metadataID, MetaKeyUnusedSeq),
			unusedSeqRangePrefix: formatMetadataKey(metadataID, MetaKeyUnusedSeqRange),
		}
	}
}

// SyncSeqKey returns the key for the sequence counter document for a database
//
//	format: _sync:{m_$}:seq
func (mp *MetadataKeys) SyncSeqKey() string {
	return mp.syncSeq
}

// UnusedSeqKey returns the key used to store a unused sequence document for sequence seq.
// These documents are used to release sequences that are allocated but not used, so that they may be
// accounted for by all SG nodes in the cluster.
//
//	format: _sync:{m_$}:unusedSeq:[seq]
func (mp *MetadataKeys) UnusedSeqKey(seq uint64) string {
	return mp.unusedSeqPrefix + strconv.FormatUint(seq, 10)
}

// UnusedSeqPrefix returns just the prefix used for UnusedSeqKey documents (used for DCP filtering)
//
//	format: _sync:{m_$}:unusedSeq:
func (mp *MetadataKeys) UnusedSeqPrefix() string {
	return mp.unusedSeqPrefix
}

// UnusedSeqRangeKey returns the key used to store a unused sequence document for sequence seq.
// These documents are used to release sequences that are allocated but not used, so that they may be
// accounted for by all SG nodes in the cluster.
//
//	format: _sync:{m_$}:unusedSeqs:[fromSeq]:[toSeq]
func (mp *MetadataKeys) UnusedSeqRangeKey(fromSeq, toSeq uint64) string {

	return mp.unusedSeqRangePrefix + strconv.FormatUint(fromSeq, 10) + ":" + strconv.FormatUint(toSeq, 10)
}

// UnusedSeqRangePrefix returns just the prefix used for UnusedSeqRangeKey documents (used for DCP filtering)
//
//	format: _sync:{m_$}:unusedSeqs:
func (mp *MetadataKeys) UnusedSeqRangePrefix() string {
	return mp.unusedSeqRangePrefix
}

// formatMetadataKey formats key into the form _sync:m_[metadataID]:[metaKey]
func formatMetadataKey(metadataPrefix string, metaKey metadataKey) string {
	return SyncDocMetadataPrefix + metadataPrefix + ":" + metaKey.String()
}

// formatDefaultMetadataKey formats key into the form _sync:[metaKey]
func formatDefaultMetadataKey(metaKey metadataKey) string {
	return SyncDocPrefix + metaKey.String()
}

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
