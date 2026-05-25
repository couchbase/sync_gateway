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
	"context"
	"errors"
	"fmt"
	"strconv"

	sgbucket "github.com/couchbase/sg-bucket"
)

const SyncDocPrefix = "_sync:"                                 // Prefix for all legacy (non-namespaced) Sync Gateway metadata documents
const MetadataIdPrefix = "m_"                                  // Prefix for metadataId, to prevent collisions between namespaced and non-namespaced metadata documents
const SyncDocMetadataPrefix = SyncDocPrefix + MetadataIdPrefix // Prefix for all namespaced Sync Gateway metadata documents
const DCPCheckpointPrefix = "dcp_ck:"

const DefaultMetadataID = "_default" // MetadataID assigned to databases that include _default._default, signalling legacy (unprefixed) metadata key format.

const minimumAttachmentMigrationMetadataVersion = "4.0.0" // minimum metadata version that needs to be defined for metadata migration.

type syncInfoEncodingType byte

const (
	// SyncInfoTypeUnknown is an unused byte value but here for clarity between the zero value
	SyncInfoTypeUnknown syncInfoEncodingType = iota
	// SyncInfoTypeV1 is used to denote a sync info document in version 4.1 and later
	SyncInfoTypeV1
)

// Sync Gateway Metadata document types
type metadataKey int

const (
	MetaKeySeq                              metadataKey = iota // "seq"
	MetaKeyUnusedSeq                                           // "unusedSeq:"
	MetaKeyUnusedSeqRange                                      // "unusedSeqs:"
	MetaKeySGRStatus                                           // "sgrStatus:"
	MetaKeySGCfg                                               // "cfg"
	MetaKeyHeartbeaterPrefix                                   // "hb:"
	MetaKeyDCPCheckpoint                                       // "dcp_ck:"
	MetaKeyBackgroundProcessHeartbeatPrefix                    // "background_process:heartbeat:"
	MetaKeyBackgroundProcessStatusPrefix                       // "background_process:status:"
	MetaKeyUserPrefix                                          // "user:"
	MetaKeyRolePrefix                                          // "role:"
	MetaKeyUserEmailPrefix                                     // "useremail:"
	MetaKeySessionPrefix                                       // "session:"
	MetaKeyResyncHeartBeaterPrefix                             // "resync_hb:"
	MetaKeyResyncCfgPrefix                                     // "resync_cfg:"
	MetaKeyDatabaseState                                       // "state"
)

var metadataKeyNames = []string{
	"seq",                           // counter document storing the sequence number for a database
	"unusedSeq:",                    // stores a sequence that was reserved by SG but was unused
	"unusedSeqs:",                   // stores a sequence range that was reserved by SG but was unused
	"sgrStatus:",                    // document prefix used to store ISGR status documents
	"cfg",                           // document prefix used to store CfgSG/cbgt data
	"hb:",                           // document prefix used to store SG node heartbeat documents
	DCPCheckpointPrefix,             // stores a DCP checkpoint
	"background_process:heartbeat:", // stores a background process heartbeat
	"background_process:status:",    // stores a background process status
	"user:",                         // stores a user
	"role:",                         // stores a role
	"useremail:",                    // stores a role
	"session:",                      // stores a session
	"resync_hb:",                    // document prefix used to store resync data
	"resync_cfg:",                   // document prefix used to store resync cfg data
	"state",                         // stores the database state
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
	dcpCheckpoint             string
	backgroundHeartbeatPrefix string
	backgroundStatusPrefix    string
	userPrefix                string
	rolePrefix                string
	userEmailPrefix           string
	sessionPrefix             string
	resyncHeartbeaterPrefix   string
	resyncCfgPrefix           string
	databaseState             string
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
	dcpCheckpoint:             formatDefaultMetadataKey(MetaKeyDCPCheckpoint),
	backgroundHeartbeatPrefix: formatDefaultMetadataKey(MetaKeyBackgroundProcessHeartbeatPrefix),
	backgroundStatusPrefix:    formatDefaultMetadataKey(MetaKeyBackgroundProcessStatusPrefix),
	userPrefix:                formatDefaultMetadataKey(MetaKeyUserPrefix),
	rolePrefix:                formatDefaultMetadataKey(MetaKeyRolePrefix),
	userEmailPrefix:           formatDefaultMetadataKey(MetaKeyUserEmailPrefix),
	sessionPrefix:             formatDefaultMetadataKey(MetaKeySessionPrefix),
	resyncHeartbeaterPrefix:   formatDefaultMetadataKey(MetaKeyResyncHeartBeaterPrefix),
	resyncCfgPrefix:           formatDefaultMetadataKey(MetaKeyResyncCfgPrefix),
	databaseState:             formatDefaultMetadataKey(MetaKeyDatabaseState),
}

// NewMetadataKeys returns MetadataKeys for the specified MetadataID. If metadataID is empty string or DefaultMetadataID,
// returns the default (legacy) metadata keys with no prefix.
// Key and prefix formatting is done in this constructor to minimize the work done per retrieval.
func NewMetadataKeys(metadataID string) *MetadataKeys {
	if metadataID == "" || metadataID == DefaultMetadataID {
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
			dcpCheckpoint:             formatInvertedMetadataKey(metadataID, MetaKeyDCPCheckpoint),
			backgroundHeartbeatPrefix: formatMetadataKey(metadataID, MetaKeyBackgroundProcessHeartbeatPrefix),
			backgroundStatusPrefix:    formatMetadataKey(metadataID, MetaKeyBackgroundProcessStatusPrefix),
			userPrefix:                formatInvertedMetadataKey(metadataID, MetaKeyUserPrefix),
			rolePrefix:                formatInvertedMetadataKey(metadataID, MetaKeyRolePrefix),
			userEmailPrefix:           formatInvertedMetadataKey(metadataID, MetaKeyUserEmailPrefix),
			sessionPrefix:             formatInvertedMetadataKey(metadataID, MetaKeySessionPrefix),
			resyncHeartbeaterPrefix:   formatMetadataKey(metadataID, MetaKeyResyncHeartBeaterPrefix),
			resyncCfgPrefix:           formatMetadataKey(metadataID, MetaKeyResyncCfgPrefix),
			databaseState:             formatMetadataKey(metadataID, MetaKeyDatabaseState),
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
//	format: _sync:{m_$}:seq (collections aware)
//	format: _sync:seq  (default)
func (m *MetadataKeys) SyncSeqKey() string {
	return m.syncSeq
}

// UnusedSeqKey returns the key used to store an unused sequence document for sequence seq.
// These documents are used to release sequences that are allocated but not used, so that they may be
// accounted for by all SG nodes in the cluster.
//
//	format: _sync:{m_$}:unusedSeq:{seq} (collections aware)
//	format: _sync:unusedSeq:{seq}  (default)
func (m *MetadataKeys) UnusedSeqKey(seq uint64) string {
	return m.unusedSeqPrefix + strconv.FormatUint(seq, 10)
}

// UnusedSeqPrefix returns just the prefix used for UnusedSeqKey documents (used for DCP filtering)
//
//	format: _sync:{m_$}:unusedSeq: (collections aware)
//	format: _sync:unusedSeq:  (default)
func (m *MetadataKeys) UnusedSeqPrefix() string {
	return m.unusedSeqPrefix
}

// ReplicationStatusKey generates the key used to store status information for an ISGR replication.  If replicationID
// is 40 characters or longer, an SHA-1 hash of the replicationID is used in the status key.
// If the replicationID is less than 40 characters, the ID can be used directly without worrying about final key length
// or collision with other sha-1 hashes.
//
//	format: _sync:{m_$}:sgrStatus:[aGroupID]{replicationID} (collections aware)
//	format: _sync:sgrStatus:[aGroupID]{replicationID}  (default)
func (m *MetadataKeys) ReplicationStatusKey(replicationID string) string {
	return m.sgrStatusPrefix + m.serializeIfLonger(replicationID)
}

// HeartbeaterPrefix returns a document prefix to use for heartbeat documents. For compatibility, an empty metadataID
// does not include the "hb:" component in the prefix.
//
//	format: _sync:{m_$}:hb:[groupID:]   (collections)
//	format: _sync:[groupID:]   (default)
func (m *MetadataKeys) HeartbeaterPrefix(groupID string) string {
	if groupID != "" {
		return m.heartbeaterPrefix + groupID + ":"
	}
	return m.heartbeaterPrefix
}

// SGCfgPrefix returns a document prefix to use for cfg documents (cbgt)
//
//	format: _sync:{m_$}:cfg[groupID:]   (collections)
//	format: _sync:cfg[groupID:]   (default)
func (m *MetadataKeys) SGCfgPrefix(groupID string) string {
	if groupID != "" {
		return m.sgCfgPrefix + groupID + ":"
	}
	return m.sgCfgPrefix
}

// ResyncHeartbeaterPrefix returns a document prefix to use for resync heartbeat documents.
//
//	format: _sync:{m_$}:resync_hb:   (collections)
//	format: _sync:resync_hb:   (default)
func (m *MetadataKeys) ResyncHeartbeaterPrefix() string {
	return m.resyncHeartbeaterPrefix
}

// ResyncCfgPrefix returns a document prefix to use for resync cfg documents
//
//	format: _sync:{m_$}:resync_cfg:   (collections)
//	format: _sync:resync_cfg:   (default)
func (m *MetadataKeys) ResyncCfgPrefix() string {
	return m.resyncCfgPrefix
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
//	format: _sync:{m_$}:unusedSeqs:[fromSeq]:[toSeq] (collections aware)
//	format: _sync:unusedSeqs:[fromSeq]:[toSeq]  (default)
func (m *MetadataKeys) UnusedSeqRangeKey(fromSeq, toSeq uint64) string {

	return m.unusedSeqRangePrefix + strconv.FormatUint(fromSeq, 10) + ":" + strconv.FormatUint(toSeq, 10)
}

// UnusedSeqRangePrefix returns just the prefix used for UnusedSeqRangeKey documents (used for DCP filtering)
//
//	format: _sync:{m_$}:unusedSeqs: (collections aware)
//	format: _sync:unusedSeqs:  (default)
func (m *MetadataKeys) UnusedSeqRangePrefix() string {
	return m.unusedSeqRangePrefix
}

// DCPCheckpointPrefix returns the prefix used to store DCP checkpoints.
//
//	format: _sync:dcp_ck:{m_$}:[groupID:] (collections aware)
//	format: _sync:dcp_ck:[groupID:] (default)
func (m *MetadataKeys) DCPCheckpointPrefix(groupID string) string {
	if groupID != "" {
		return m.dcpCheckpoint + groupID + ":"
	}
	return m.dcpCheckpoint
}

// DCPVersionedCheckpointPrefix returns the prefix used to store versioned DCP checkpoints.
//
//	format: _sync:dcp_ck:{m_$}:[{groupID:]{version:} (collections aware)
//	format: _sync:dcp_ck:[{groupID:]{version:} (default)
func (m *MetadataKeys) DCPVersionedCheckpointPrefix(groupID string, version uint64) string {
	checkpointPrefix := m.dcpCheckpoint
	if groupID != "" {
		checkpointPrefix = checkpointPrefix + groupID + ":"
	}
	if version != 0 {
		checkpointPrefix = checkpointPrefix + strconv.FormatUint(version, 10) + ":"
	}
	return checkpointPrefix
}

// UserKey returns the key used to store a user document
//
//	format: _sync:user:{m_$}:{username} (collections aware)
//	format: _sync:user:{username}  (default)
func (m *MetadataKeys) UserKey(username string) string {
	return m.userPrefix + m.serializeIfLonger(username)
}

// UserKeyPrefix returns the prefix used to store a user document
//
//	format: _sync:user:{m_$}: (collections aware)
//	format: _sync:user:  (default)
func (m *MetadataKeys) UserKeyPrefix() string {
	return m.userPrefix
}

// RoleKey returns the key used to store a role document
//
//	format: _sync:role:{m_$}:{rolename} (collections aware)
//	format: _sync:role:{rolename}  (default)
func (m *MetadataKeys) RoleKey(name string) string {
	return m.rolePrefix + m.serializeIfLonger(name)
}

// RoleKeyPrefix returns the prefix used to store a role document
//
//	format: _sync:role:{m_$}: (collections aware)
//	format: _sync:role:  (default)
func (m *MetadataKeys) RoleKeyPrefix() string {
	return m.rolePrefix
}

// UserEmailKey returns the key used to store a user email document
//
//	format: _sync:useremail:{m_$}:{username} (collections aware)
//	format: _sync:useremail:{username}  (default)
func (m *MetadataKeys) UserEmailKey(username string) string {
	return m.userEmailPrefix + m.serializeIfLonger(username)
}

// SessionKey returns the key used to store a session document
//
//	format: _sync:session:{m_$}:{sessionID} (collections aware)
//	format: _sync:session:{sessionID}  (default)
func (m *MetadataKeys) SessionKey(sessionID string) string {
	return m.sessionPrefix + sessionID
}

// BackgroundProcessHeartbeatPrefix returns the prefix used to store background process heartbeats.
//
//	format: _sync:{m_$}:background_process:heartbeat:[processSuffix] (collections aware)
//	format: _sync:background_process:heartbeat:[processSuffix]  (default)
func (m *MetadataKeys) BackgroundProcessHeartbeatPrefix(processSuffix string) string {
	return m.backgroundHeartbeatPrefix + processSuffix
}

// BackgroundProcessStatusPrefix returns the prefix used to store background process status documents.
//
//	format: _sync:{m_$}:background_process:status:[processSuffix] (collections aware)
//	format: _sync:background_process:status:[processSuffix]  (default)
func (m *MetadataKeys) BackgroundProcessStatusPrefix(processSuffix string) string {
	return m.backgroundStatusPrefix + processSuffix
}

// DatabaseStateKey returns the key used to store the database state document.
//
//	format: _sync:{m_$}:state (collections aware)
//	format: _sync:state       (default)
func (m *MetadataKeys) DatabaseStateKey() string {
	return m.databaseState
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
	MetadataID      *string `json:"metadataID,omitempty"`
	MetaDataVersion string  `json:"metadata_version,omitempty"`
}

// marshalSyncInfo serialises a SyncInfo, prepending the V1 version byte when the cluster
// has rolled forward to 4.1+. Older clusters keep writing bare JSON so a not-yet-upgraded
// peer can still read the document.
func marshalSyncInfo(syncInfo *SyncInfo, clusterCompatVersion *ClusterCompatVersion) ([]byte, error) {
	payload, err := JSONMarshal(syncInfo)
	if err != nil {
		return nil, fmt.Errorf("Error marshalling syncInfo: %v", err)
	}
	if clusterCompatVersion != nil && clusterCompatVersion.AtLeast(4, 1) {
		payload = append([]byte{byte(SyncInfoTypeV1)}, payload...)
	}
	return payload, nil
}

// syncInfoWriteOpts returns the appropriate WriteOptions for a marshalSyncInfo payload.
// V1-prefixed payloads must be written as binary datatype. Bare-JSON payloads (ccv < 4.1)
// continue to be written as JSON datatype so a not-yet-upgraded peer whose reader
// enforces datatype can still load it.
func syncInfoWriteOpts(payload []byte) sgbucket.WriteOptions {
	if len(payload) > 0 && payload[0] == byte(SyncInfoTypeV1) {
		return sgbucket.Raw
	}
	return 0
}

// DecodeSyncInfo decodes a raw syncInfo payload, handling the version-byte prefix
// introduced in ccv 4.1+.
func DecodeSyncInfo(data []byte) (SyncInfo, error) {
	var s SyncInfo
	if len(data) == 0 {
		return s, nil
	}
	switch {
	case data[0] == '{':
		// legacy bare-JSON encoding
	case data[0] == byte(SyncInfoTypeV1):
		data = data[1:]
	default:
		return s, fmt.Errorf("unrecognized syncInfo version byte: 0x%02x", data[0])
	}
	if err := JSONUnmarshal(data, &s); err != nil {
		return s, fmt.Errorf("Error unmarshalling syncInfo: %v", err)
	}
	return s, nil
}

// loadSyncInfo fetches and decodes syncInfo for a datastore. found=false means the doc does
// not exist; found=true with a zero-value SyncInfo means the doc exists but is empty.
func loadSyncInfo(ctx context.Context, ds DataStore) (syncInfo SyncInfo, found bool, err error) {
	raw, _, fetchErr := ds.GetRaw(ctx, SGSyncInfo)
	if IsDocNotFoundError(fetchErr) {
		return SyncInfo{}, false, nil
	}
	if fetchErr != nil {
		return SyncInfo{}, false, fmt.Errorf("Error retrieving syncInfo: %v", fetchErr)
	}
	s, err := DecodeSyncInfo(raw)
	return s, true, err
}

// InitSyncInfo attempts to initialize syncInfo for a datastore.
//  1. If syncInfo doesn't exist, it is created for the specified metadataID.
//  2. If syncInfo exists with a matching metadataID, returns requiresResync=false.
//  3. If syncInfo exists with a non-matching metadataID, returns requiresResync=true.
//     If syncInfo exists and has metaDataVersion >= 4.0, returns requiresAttachmentMigration=false,
//     otherwise requiresAttachmentMigration=true to migrate metadata attachments.
func InitSyncInfo(ctx context.Context, ds DataStore, metadataID string, clusterCompatVersion *ClusterCompatVersion) (requiresResync bool, requiresAttachmentMigration bool, err error) {
	syncInfo, found, err := loadSyncInfo(ctx, ds)
	if err != nil {
		return true, true, err
	}
	if !found {
		if metadataID == "" {
			return false, true, nil
		}
		newSyncInfo := SyncInfo{MetadataID: Ptr(metadataID)}
		payload, mErr := marshalSyncInfo(&newSyncInfo, clusterCompatVersion)
		if mErr != nil {
			return true, true, mErr
		}
		// WriteCas with cas=0 inserts the payload, picking the datatype tag from the
		// payload's prefix (binary for V1, JSON for bare-JSON ccv < 4.1). Using Add here
		// would unconditionally tag as JSON via SGJSONTranscoder, which loses the binary
		// datatype needed for V1 payloads.
		_, writeErr := ds.WriteCas(ctx, SGSyncInfo, 0, 0, payload, syncInfoWriteOpts(payload))
		switch {
		case IsCasMismatch(writeErr):
			// attempt new fetch
			var refound bool
			syncInfo, refound, err = loadSyncInfo(ctx, ds)
			if err != nil {
				return true, true, fmt.Errorf("Error retrieving syncInfo (after failed add): %v", err)
			}
			if !refound {
				return true, true, fmt.Errorf("syncInfo missing after CAS mismatch on add")
			}
		case writeErr != nil:
			return true, true, fmt.Errorf("Error adding syncInfo: %v", writeErr)
		default:
			syncInfo = newSyncInfo
		}
	}

	requiresResync = syncInfo.requiresResync(metadataID)
	requiresAttachmentMigration, err = CompareMetadataVersion(ctx, syncInfo.MetaDataVersion)
	if err != nil {
		return requiresResync, true, err
	}
	return requiresResync, requiresAttachmentMigration, nil
}

// requiresResync determines if a given SyncInfo document represents a collection requiring resync.
func (s *SyncInfo) requiresResync(metadataID string) bool {
	// if metadataID is not set, then the document was empty (pre collections) or it only ran attachment migration. Either
	// way, this means the associated collection only has a single database associated and doesn't need resync.
	if s.MetadataID == nil {
		return false
	}
	return *s.MetadataID != metadataID
}

// updateSyncInfo CAS-updates the syncInfo document by reading it, applying mutate to the
// decoded value, and writing back in the format dictated by clusterCompatVersion. Uses
// GetRaw + WriteCas with sgbucket.Raw rather than DataStore.Update so the doc can be read
// and written with binary datatype - gocb's RawJSONTranscoder (used by Update) rejects
// non-JSON datatypes, and the V1 syncInfo format is byte-prefixed binary. Retries on CAS
// mismatch indefinitely, mirroring the unbounded retry loop in DataStore.Update.
func updateSyncInfo(ctx context.Context, ds DataStore, clusterCompatVersion *ClusterCompatVersion, mutate func(*SyncInfo)) error {
	for {
		current, cas, getErr := ds.GetRaw(ctx, SGSyncInfo)
		notFound := IsDocNotFoundError(getErr)
		if getErr != nil && !notFound {
			return getErr
		}
		var syncInfo SyncInfo
		if !notFound {
			decoded, decodeErr := DecodeSyncInfo(current)
			if decodeErr != nil {
				return decodeErr
			}
			syncInfo = decoded
		} else {
			cas = 0
		}
		mutate(&syncInfo)
		bytes, marshalErr := marshalSyncInfo(&syncInfo, clusterCompatVersion)
		if marshalErr != nil {
			return marshalErr
		}
		if _, writeErr := ds.WriteCas(ctx, SGSyncInfo, 0, cas, bytes, syncInfoWriteOpts(bytes)); writeErr == nil {
			return nil
		} else if !IsCasMismatch(writeErr) {
			return writeErr
		}
	}
}

// SetSyncInfoMetadataID sets syncInfo in a DataStore to the specified metadataID, preserving metadata version if present.
func SetSyncInfoMetadataID(ctx context.Context, ds DataStore, metadataID string, clusterCompatVersion *ClusterCompatVersion) error {
	// If the metadataID isn't defined, don't persist SyncInfo.  Defensive handling for legacy use cases.
	if metadataID == "" {
		return nil
	}
	return updateSyncInfo(ctx, ds, clusterCompatVersion, func(s *SyncInfo) {
		s.MetadataID = Ptr(metadataID)
	})
}

// SetSyncInfoMetaVersion sets syncInfo in a DataStore to the specified metadata version, preserving metadataID if present.
func SetSyncInfoMetaVersion(ctx context.Context, ds DataStore, metaVersion string, clusterCompatVersion *ClusterCompatVersion) error {
	if metaVersion == "" {
		return nil
	}
	return updateSyncInfo(ctx, ds, clusterCompatVersion, func(s *SyncInfo) {
		s.MetaDataVersion = metaVersion
	})
}

// SerializeIfLonger returns name as a sha1 string if the length of the name is greater or equal to the length specified. Otherwise, returns the original string.
func SerializeIfLonger(name string, length int) string {
	if len(name) < length {
		return name
	}
	return Sha1HashString(name, "")
}

// CompareMetadataVersion Will build comparable build version for comparison with meta version defined in syncInfo, then
// will return true if we require attachment migration, false if not.
func CompareMetadataVersion(ctx context.Context, metaVersion string) (bool, error) {
	if metaVersion == "" {
		// no meta version passed in, thus attachment migration should take place
		return true, nil
	}
	syncInfoVersion, err := NewComparableBuildVersionFromString(metaVersion)
	if err != nil {
		return true, err
	}
	return CheckRequireAttachmentMigration(ctx, syncInfoVersion)
}

// CheckRequireAttachmentMigration will return true if current metaVersion < 4.0.0, else false
func CheckRequireAttachmentMigration(ctx context.Context, version *ComparableBuildVersion) (bool, error) {
	if version == nil {
		AssertfCtx(ctx, "failed to build comparable build version for syncInfo metaVersion")
		return true, fmt.Errorf("corrupt syncInfo metaVersion value")
	}
	minVerStr := minimumAttachmentMigrationMetadataVersion
	minVersion, err := NewComparableBuildVersionFromString(minVerStr)
	if err != nil {
		AssertfCtx(ctx, "failed to build comparable build version for minimum version for attachment migration")
		return true, err
	}

	if minVersion.AtLeastMinorDowngrade(version) {
		return true, nil
	}
	return false, nil
}
