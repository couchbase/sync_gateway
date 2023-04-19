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
const DCPCheckpointPrefix = "dcp_ck:"

// Sync Gateway Metadata document types
type metadataKey int

const (
	MetaKeySeq                              metadataKey = iota // "seq"
	MetaKeyUnusedSeq                                           // "unusedSeq:"
	MetaKeyUnusedSeqRange                                      // "unusedSeqs:"
	MetaKeySGRStatus                                           // "sgrStatus:"
	MetaKeySGCfg                                               // "cfg"
	MetaKeyHeartbeaterPrefix                                   // "hb:"
	MetaKeyDCPBackfill                                         // "dcp_backfill"
	MetaKeyDCPCheckpoint                                       // "dcp_ck:"
	MetaKeyBackgroundProcessHeartbeatPrefix                    // "background_process:heartbeat:"
	MetaKeyBackgroundProcessStatusPrefix                       // "background_process:status:"
	MetaKeyUserPrefix                                          // "user:"
	MetaKeyRolePrefix                                          // "role:"
	MetaKeyUserEmailPrefix                                     // "useremail:"
	MetaKeySessionPrefix                                       // "session:"
)

var metadataKeyNames = []string{
	"seq",                           // counter document storing the sequence number for a database
	"unusedSeq:",                    // stores a sequence that was reserved by SG but was unused
	"unusedSeqs:",                   // stores a sequence range that was reserved by SG but was unused
	"sgrStatus:",                    // document prefix used to store ISGR status documents
	"cfg",                           // document prefix used to store CfgSG/cbgt data
	"hb:",                           // document prefix used to store SG node heartbeat documents
	"dcp_backfill",                  // stores BackfillSequences for a DCP feed,\
	DCPCheckpointPrefix,             // stores a DCP checkpoint
	"background_process:heartbeat:", // stores a background process heartbeat
	"background_process:status:",    // stores a background process status
	"user:",                         // stores a user
	"role:",                         // stores a role
	"useremail:",                    // stores a role
	"session:",                      // stores a session

}

func (m metadataKey) String() string {
	return metadataKeyNames[m]
}

const (
	UserPrefixRoot    = SyncDocPrefix + "user:"    // UserPrefix stores user documents keyed by username
	RolePrefixRoot    = SyncDocPrefix + "role:"    // RolePrefix stores role documents keyed by role name
	SessionPrefixRoot = SyncDocPrefix + "session:" // SessionPrefix stores user sessions keyed by session ID
)

// The following collection-scoped metadata documents are stored with the collection, don't require MetadataKeys handling
const (
	RevBodyPrefix = SyncDocPrefix + "rb:"   // RevBodyPrefix stores a conflicting revision's body
	RevPrefix     = SyncDocPrefix + "rev:"  // RevPrefix stores an old revision's body for temporary lookup (in-flight requests or delta sync)
	AttPrefix     = SyncDocPrefix + "att:"  // AttPrefix SG (v1) attachment data
	Att2Prefix    = SyncDocPrefix + "att2:" // Att2Prefix SG v2 attachment data
)

// The following keys and prefixes don't require MetadataKeys handling as they are cross-database or have
// custom handling for database namespacing
const (
	DCPCheckpointRootPrefix              = SyncDocPrefix + DCPCheckpointPrefix // used to filter checkpoints across groupIDs, databases
	SGRegistryKey                        = SyncDocPrefix + "registry"          // Registry of all SG databases defined for the bucket (for all group IDs)
	SGSyncInfo                           = SyncDocPrefix + "syncInfo"          // SG info for a collection, stored with the collection
	PersistentConfigPrefixWithoutGroupID = SyncDocPrefix + "dbconfig:"         // PersistentConfigPrefixWithoutGroupID stores a database config

	// Sync function naming is collection-scoped, and collections cannot be associated with multiple databases
	SyncFunctionKeyWithoutGroupID           = SyncDocPrefix + "syncdata"            // SyncFunctionKeyWithoutGroupID stores a copy of the Sync Function
	CollectionSyncFunctionKeyWithoutGroupID = SyncDocPrefix + "syncdata_collection" // CollectionSyncFunctionKeyWithoutGroupID stores a copy of the Sync Function
)

// MetadataKeys defines metadata keys and prefixes for a database, for a given metadataID.  Each Sync Gateway database
// uses a unique metadataID, but may share the same MetadataStore.  MetadataKeys implements key namespacing for that
// MetadataStore.
type MetadataKeys struct {
	metadataID                string
	syncSeq                   string
	unusedSeqPrefix           string
	unusedSeqRangePrefix      string
	sgrStatusPrefix           string
	heartbeaterPrefix         string
	persistentConfigPrefix    string
	sgCfgPrefix               string
	dcpBackfill               string
	dcpCheckpoint             string
	backgroundHeartbeatPrefix string
	backgroundStatusPrefix    string
	userPrefix                string
	rolePrefix                string
	userEmailPrefix           string
	sessionPrefix             string
}

// sha1HashLength is the number of characters in a sha1
const sha1HashLength = 40

// DefaultMetadataKeys defines the legacy metadata keys and prefixes.  These are used when the metadata collection is the only
// defined collection, including upgrade scenarios.
var DefaultMetadataKeys = &MetadataKeys{
	metadataID:                "",
	syncSeq:                   formatDefaultMetadataKey(MetaKeySeq),
	unusedSeqPrefix:           formatDefaultMetadataKey(MetaKeyUnusedSeq),
	unusedSeqRangePrefix:      formatDefaultMetadataKey(MetaKeyUnusedSeqRange),
	sgrStatusPrefix:           formatDefaultMetadataKey(MetaKeySGRStatus),
	sgCfgPrefix:               formatDefaultMetadataKey(MetaKeySGCfg),
	heartbeaterPrefix:         SyncDocPrefix, // Default heartbeater prefix does not use MetaKeyHeartbeaterPrefix for backward compatibility with 3.0 and earlier
	dcpBackfill:               formatDefaultMetadataKey(MetaKeyDCPBackfill),
	dcpCheckpoint:             formatDefaultMetadataKey(MetaKeyDCPCheckpoint),
	backgroundHeartbeatPrefix: formatDefaultMetadataKey(MetaKeyBackgroundProcessHeartbeatPrefix),
	backgroundStatusPrefix:    formatDefaultMetadataKey(MetaKeyBackgroundProcessStatusPrefix),
	userPrefix:                formatDefaultMetadataKey(MetaKeyUserPrefix),
	rolePrefix:                formatDefaultMetadataKey(MetaKeyRolePrefix),
	userEmailPrefix:           formatDefaultMetadataKey(MetaKeyUserEmailPrefix),
	sessionPrefix:             formatDefaultMetadataKey(MetaKeySessionPrefix),
}

// NewMetadataKeys returns MetadataKeys for the specified MetadataID  If metadataID is empty string, returns the default (legacy) metadata keys.
// Key and prefix formatting is done in this constructor to minimize the work done per retrieval.
func NewMetadataKeys(metadataID string) *MetadataKeys {
	if metadataID == "" {
		return DefaultMetadataKeys
	} else {
		return &MetadataKeys{
			metadataID:                metadataID,
			syncSeq:                   formatMetadataKey(metadataID, MetaKeySeq),
			unusedSeqPrefix:           formatMetadataKey(metadataID, MetaKeyUnusedSeq),
			unusedSeqRangePrefix:      formatMetadataKey(metadataID, MetaKeyUnusedSeqRange),
			sgrStatusPrefix:           formatMetadataKey(metadataID, MetaKeySGRStatus),
			heartbeaterPrefix:         formatMetadataKey(metadataID, MetaKeyHeartbeaterPrefix),
			sgCfgPrefix:               formatMetadataKey(metadataID, MetaKeySGCfg),
			dcpBackfill:               formatMetadataKey(metadataID, MetaKeyDCPBackfill),
			dcpCheckpoint:             formatInvertedMetadataKey(metadataID, MetaKeyDCPCheckpoint),
			backgroundHeartbeatPrefix: formatMetadataKey(metadataID, MetaKeyBackgroundProcessHeartbeatPrefix),
			backgroundStatusPrefix:    formatMetadataKey(metadataID, MetaKeyBackgroundProcessStatusPrefix),
			userPrefix:                formatInvertedMetadataKey(metadataID, MetaKeyUserPrefix),
			rolePrefix:                formatInvertedMetadataKey(metadataID, MetaKeyRolePrefix),
			userEmailPrefix:           formatInvertedMetadataKey(metadataID, MetaKeyUserEmailPrefix),
			sessionPrefix:             formatInvertedMetadataKey(metadataID, MetaKeySessionPrefix),
		}
	}
}

func (m *MetadataKeys) serializeIfLonger(key string) string {
	if m == DefaultMetadataKeys {
		return key
	}
	return SerializeIfLonger(key, sha1HashLength)
}

// SyncSeqKey returns the key for the sequence counter document for a database
//
//	format: _sync:{m_$}:seq
func (m *MetadataKeys) SyncSeqKey() string {
	return m.syncSeq
}

// UnusedSeqKey returns the key used to store an unused sequence document for sequence seq.
// These documents are used to release sequences that are allocated but not used, so that they may be
// accounted for by all SG nodes in the cluster.
//
//	format: _sync:{m_$}:unusedSeq:[seq]
func (m *MetadataKeys) UnusedSeqKey(seq uint64) string {
	return m.unusedSeqPrefix + strconv.FormatUint(seq, 10)
}

// UnusedSeqPrefix returns just the prefix used for UnusedSeqKey documents (used for DCP filtering)
//
//	format: _sync:{m_$}:unusedSeq:
func (m *MetadataKeys) UnusedSeqPrefix() string {
	return m.unusedSeqPrefix
}

// ReplicationStatusKey generates the key used to store status information for an ISGR replication.  If replicationID
// is 40 characters or longer, an SHA-1 hash of the replicationID is used in the status key.
// If the replicationID is less than 40 characters, the ID can be used directly without worrying about final key length
// or collision with other sha-1 hashes.
//
//	format: _sync:{m_$}:sgrStatus:[replicationID]
func (m *MetadataKeys) ReplicationStatusKey(replicationID string) string {
	return m.sgrStatusPrefix + m.serializeIfLonger(replicationID)
}

// HeartbeaterPrefix returns a document prefix to use for heartbeat documents
//
//	format: _sync:{m_$}:hb:[groupID]:   (collections)
//	format: _sync:[groupID]:   (default)
func (m *MetadataKeys) HeartbeaterPrefix(groupID string) string {
	if groupID != "" {
		return m.heartbeaterPrefix + groupID + ":"
	}
	return m.heartbeaterPrefix
}

// SGCfgPrefix returns a document prefix to use for cfg documents (cbgt)
//
//	format: _sync:{m_$}:hb:[groupID]:   (collections)
//	format: _sync:[groupID]:   (default)
func (m *MetadataKeys) SGCfgPrefix(groupID string) string {
	if groupID != "" {
		return m.sgCfgPrefix + groupID + ":"
	}
	return m.sgCfgPrefix
}

// PersistentConfigKey returns a document key to use for persisted database configurations
//
//	format: _sync:{m_$}:db_config:[groupID]
func (m *MetadataKeys) PersistentConfigKey(groupID string) (string, error) {

	if groupID == "" {
		return "", errors.New("PersistentConfigKey requires a group ID, even if it's just `default`")
	}
	return m.persistentConfigPrefix + groupID, nil
}

// UnusedSeqRangeKey returns the key used to store an unused sequence document for sequence seq.
// These documents are used to release sequences that are allocated but not used, so that they may be
// accounted for by all SG nodes in the cluster.
//
//	format: _sync:{m_$}:unusedSeqs:[fromSeq]:[toSeq]
func (m *MetadataKeys) UnusedSeqRangeKey(fromSeq, toSeq uint64) string {

	return m.unusedSeqRangePrefix + strconv.FormatUint(fromSeq, 10) + ":" + strconv.FormatUint(toSeq, 10)
}

// UnusedSeqRangePrefix returns just the prefix used for UnusedSeqRangeKey documents (used for DCP filtering)
//
//	format: _sync:{m_$}:unusedSeqs:
func (m *MetadataKeys) UnusedSeqRangePrefix() string {
	return m.unusedSeqRangePrefix
}

// DCPCheckpointPrefix returns the prefix used to store DCP checkpoints.
//
//	format: _sync:dcp_ck:{m_$}:groupID:
func (m *MetadataKeys) DCPCheckpointPrefix(groupID string) string {
	if groupID != "" {
		return m.dcpCheckpoint + groupID + ":"
	}
	return m.dcpCheckpoint
}

// DCPBackfillKey returns the key used to store DCP backfill statistics.
//
//	format: _sync:{m_$}:dcp_backfill
func (m *MetadataKeys) DCPBackfillKey() string {
	return m.dcpBackfill
}

// UserKey returns the key used to store a user document
//
//	format: _sync:user:{m_$}:{username}
func (m *MetadataKeys) UserKey(username string) string {
	return m.userPrefix + m.serializeIfLonger(username)
}

// UserKeyPrefix returns the prefix used to store a user document
//
//	format: _sync:user:{m_$}:
func (m *MetadataKeys) UserKeyPrefix() string {
	return m.userPrefix
}

// RoleKey returns the key used to store a role document
//
//	format: _sync:role:{m_$}:{rolename}
func (m *MetadataKeys) RoleKey(name string) string {
	return m.rolePrefix + m.serializeIfLonger(name)
}

// RoleKeyPrefix returns the prefix used to store a role document
//
//	format: _sync:role:{m_$}:
func (m *MetadataKeys) RoleKeyPrefix() string {
	return m.rolePrefix
}

// UserEmailKey returns the key used to store a user email document
//
//	format: _sync:useremail:{m_$}:{username}
func (m *MetadataKeys) UserEmailKey(username string) string {
	return m.userEmailPrefix + m.serializeIfLonger(username)
}

// SessionKey returns the key used to store a session document
//
//	format: _sync:session:{m_$}:{sessionID}
func (m *MetadataKeys) SessionKey(sessionID string) string {
	return m.sessionPrefix + sessionID
}

// BackgroundProcessHeartbeatPrefix returns the prefix used to store background process heartbeats.
//
//	format: _sync:{m_$}:background_process:heartbeat:[processSuffix]
func (m *MetadataKeys) BackgroundProcessHeartbeatPrefix(processSuffix string) string {
	return m.backgroundHeartbeatPrefix + processSuffix
}

// BackgroundProcessStatusPrefix returns the prefix used to store background process status documents.
//
//	format: _sync:{m_$}:background_process:status:[processSuffix]
func (m *MetadataKeys) BackgroundProcessStatusPrefix(processSuffix string) string {
	return m.backgroundStatusPrefix + processSuffix
}

// formatMetadataKey formats key into the form _sync:m_[metadataID]:[metaKey]
func formatMetadataKey(metadataPrefix string, metaKey metadataKey) string {
	return SyncDocMetadataPrefix + metadataPrefix + ":" + metaKey.String()
}

// formatInvertedMetadataKey formats key into the form _sync:[metaKey][m_metadataID]:
// Used for documents that require consistent prefixing across databases, typically
// for indexing or key filtering
func formatInvertedMetadataKey(metadataPrefix string, metaKey metadataKey) string {
	return SyncDocPrefix + metaKey.String() + metadataPrefix + ":"
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

// CollectionSyncFunctionKeyWithGroupID returns a doc ID to use when storing the sync function.
func CollectionSyncFunctionKeyWithGroupID(groupID string, scopeName, collectionName string) string {
	// use legacy format for _default._default for backward compatibility
	if IsDefaultCollection(scopeName, collectionName) {
		return SyncFunctionKeyWithGroupID(groupID)
	}
	if groupID != "" {
		return fmt.Sprintf("%s:%s.%s:%s", CollectionSyncFunctionKeyWithoutGroupID, scopeName, collectionName, groupID)
	}
	return fmt.Sprintf("%s:%s.%s", CollectionSyncFunctionKeyWithoutGroupID, scopeName, collectionName)
}

// SyncInfo documents are stored in collections to identify the metadataID associated with sync metadata in that collection
type SyncInfo struct {
	MetadataID string `json:"metadataID"`
}

// initSyncInfo attempts to initialize syncInfo for a datastore
//  1. If syncInfo doesn't exist, it is created for the specified metadataID
//  2. If syncInfo exists with a matching metadataID, returns requiresResync=false
//  3. If syncInfo exists with a non-matching metadataID, returns requiresResync=true
func InitSyncInfo(ds DataStore, metadataID string) (requiresResync bool, err error) {

	var syncInfo SyncInfo
	_, fetchErr := ds.Get(SGSyncInfo, &syncInfo)
	if IsKeyNotFoundError(ds, fetchErr) {
		newSyncInfo := &SyncInfo{MetadataID: metadataID}
		_, addErr := ds.Add(SGSyncInfo, 0, newSyncInfo)
		if IsCasMismatch(addErr) {
			// attempt new fetch
			_, fetchErr = ds.Get(SGSyncInfo, &syncInfo)
			if fetchErr != nil {
				return true, fmt.Errorf("Error retrieving syncInfo (after failed add): %v", fetchErr)
			}
		} else if addErr != nil {
			return true, fmt.Errorf("Error adding syncInfo: %v", addErr)
		}
		// successfully added
		return false, nil
	} else if fetchErr != nil {
		return true, fmt.Errorf("Error retrieving syncInfo: %v", fetchErr)
	}

	return syncInfo.MetadataID != metadataID, nil
}

// SetSyncInfo sets syncInfo in a DataStore to the specified metadataID
func SetSyncInfo(ds DataStore, metadataID string) error {
	syncInfo := &SyncInfo{
		MetadataID: metadataID,
	}
	return ds.Set(SGSyncInfo, 0, nil, syncInfo)
}

// SerializeIfLonger returns name as a sha1 string if the length of the name is greater or equal to the length specificed. Otherwise, returns the original string.
func SerializeIfLonger(name string, length int) string {
	if len(name) < length {
		return name
	}
	return Sha1HashString(name, "")
}
