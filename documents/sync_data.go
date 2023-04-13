//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package documents

import (
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Maps what users have access to what channels or roles, and when they got that access.
type UserAccessMap map[string]channels.TimedSet

type ChannelSetEntry struct {
	Name  string `json:"name"`
	Start uint64 `json:"start"`
	End   uint64 `json:"end,omitempty"`
}

// The sync-gateway metadata stored in the "_sync" property of a Couchbase document.
type SyncData struct {
	CurrentRev        string              `json:"rev"`
	NewestRev         string              `json:"new_rev,omitempty"` // Newest rev, if different from CurrentRev
	Flags             uint8               `json:"flags,omitempty"`
	Sequence          uint64              `json:"sequence,omitempty"`
	UnusedSequences   []uint64            `json:"unused_sequences,omitempty"` // unused sequences due to update conflicts/CAS retry
	RecentSequences   []uint64            `json:"recent_sequences,omitempty"` // recent sequences for this doc - used in server dedup handling
	Channels          channels.ChannelMap `json:"channels,omitempty"`
	Access            UserAccessMap       `json:"access,omitempty"`
	RoleAccess        UserAccessMap       `json:"role_access,omitempty"`
	Expiry            *time.Time          `json:"exp,omitempty"`                     // Document expiry.  Information only - actual expiry/delete handling is done by bucket storage.  Needs to be pointer for omitempty to work (see https://github.com/golang/go/issues/4357)
	Cas               string              `json:"cas"`                               // String representation of a cas value, populated via macro expansion
	Crc32c            string              `json:"value_crc32c"`                      // String representation of crc32c hash of doc body, populated via macro expansion
	Crc32cUserXattr   string              `json:"user_xattr_value_crc32c,omitempty"` // String representation of crc32c hash of user xattr
	TombstonedAt      int64               `json:"tombstoned_at,omitempty"`           // Time the document was tombstoned.  Used for view compaction
	Attachments       AttachmentsMeta     `json:"attachments,omitempty"`
	ChannelSet        []ChannelSetEntry   `json:"channel_set"`
	ChannelSetHistory []ChannelSetEntry   `json:"channel_set_history"`

	// Only used for performance metrics:
	TimeSaved time.Time `json:"time_saved,omitempty"` // Timestamp of save.

	ClusterUUID string `json:"cluster_uuid,omitempty"` // Couchbase Server UUID when the document is updated

	// Backward compatibility (the "deleted" field was, um, deleted in commit 4194f81, 2/17/14)
	Deleted_OLD bool `json:"deleted,omitempty"`
	// History should be marshalled last to optimize indexing (CBG-2559)
	History RevTree `json:"history"`

	addedRevisionBodies     []string          // revIDs of non-winning revision bodies that have been added (and so require persistence)
	removedRevisionBodyKeys map[string]string // keys of non-winning revisions that have been removed (and so may require deletion), indexed by revID
}

func (sd *SyncData) HashRedact(salt string) SyncData {

	// Creating a new SyncData with the redacted info. We copy all the information which stays the same and create new
	// items for the redacted data. The data to be redacted is populated below.
	redactedSyncData := SyncData{
		CurrentRev:      sd.CurrentRev,
		NewestRev:       sd.NewestRev,
		Flags:           sd.Flags,
		Sequence:        sd.Sequence,
		UnusedSequences: sd.UnusedSequences,
		RecentSequences: sd.RecentSequences,
		Channels:        channels.ChannelMap{},
		Access:          UserAccessMap{},
		RoleAccess:      UserAccessMap{},
		Expiry:          sd.Expiry,
		Cas:             sd.Cas,
		Crc32c:          sd.Crc32c,
		TombstonedAt:    sd.TombstonedAt,
		Attachments:     AttachmentsMeta{},
	}

	// Populate and redact channels
	for k, v := range sd.Channels {
		redactedSyncData.Channels[base.Sha1HashString(k, salt)] = v
	}

	// Populate and redact history. This is done as it also includes channel names
	redactedSyncData.History = sd.History.Redacted(salt)

	// Populate and redact user access
	for k, v := range sd.Access {
		accessTimerSet := map[string]channels.VbSequence{}
		for channelName, vbStats := range v {
			accessTimerSet[base.Sha1HashString(channelName, salt)] = vbStats
		}
		redactedSyncData.Access[base.Sha1HashString(k, salt)] = accessTimerSet
	}

	// Populate and redact user role access
	for k, v := range sd.RoleAccess {
		accessTimerSet := map[string]channels.VbSequence{}
		for channelName, vbStats := range v {
			accessTimerSet[base.Sha1HashString(channelName, salt)] = vbStats
		}
		redactedSyncData.RoleAccess[base.Sha1HashString(k, salt)] = accessTimerSet
	}

	// Populate and redact attachment names
	for k, v := range sd.Attachments {
		redactedSyncData.Attachments[base.Sha1HashString(k, salt)] = v
	}

	return redactedSyncData
}

type DocumentRoot struct {
	SyncData *SyncData `json:"_sync"`
}

// Unmarshals just a document's sync metadata from JSON data.
// (This is somewhat faster, if all you need is the sync data without the doc body.)
func UnmarshalDocumentSyncData(data []byte, needHistory bool) (*SyncData, error) {
	var root DocumentRoot
	if needHistory {
		root.SyncData = &SyncData{}
	}
	if err := base.JSONUnmarshal(data, &root); err != nil {
		return nil, err
	}
	if root.SyncData != nil && root.SyncData.Deleted_OLD {
		root.SyncData.Deleted_OLD = false
		root.SyncData.Flags |= channels.Deleted // Backward compatibility with old Deleted property
	}
	return root.SyncData, nil
}

// Unmarshals sync metadata for a document arriving via DCP.  Includes handling for xattr content
// being included in data.  If not present in either xattr or document body, returns nil but no error.
// Returns the raw body, in case it's needed for import.

// TODO: Using a pool of unmarshal workers may help prevent memory spikes under load
func UnmarshalDocumentSyncDataFromFeed(data []byte, dataType uint8, userXattrKey string, needHistory bool) (result *SyncData, rawBody []byte, rawXattr []byte, rawUserXattr []byte, err error) {

	var body []byte

	// If attr datatype flag is set, data includes both xattrs and document body.  Check for presence of sync xattr.
	// Note that there could be a non-sync xattr present
	if dataType&base.MemcachedDataTypeXattr != 0 {
		var syncXattr []byte
		var userXattr []byte
		body, syncXattr, userXattr, err = ParseXattrStreamData(base.SyncXattrName, userXattrKey, data)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		// If the sync xattr is present, use that to build SyncData
		if syncXattr != nil && len(syncXattr) > 0 {
			result = &SyncData{}
			err = base.JSONUnmarshal(syncXattr, result)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			return result, body, syncXattr, userXattr, nil
		}
	} else {
		// Xattr flag not set - data is just the document body
		body = data
	}

	// Non-xattr data, or sync xattr not present.  Attempt to retrieve sync metadata from document body
	result, err = UnmarshalDocumentSyncData(body, needHistory)
	return result, body, rawUserXattr, nil, err
}

func (s *SyncData) HasValidSyncData() bool {

	valid := s != nil && s.CurrentRev != "" && (s.Sequence > 0)
	return valid
}

// Converts the string hex encoding that's stored in the sync metadata to a uint64 cas value
func (s *SyncData) GetSyncCas() uint64 {

	if s.Cas == "" {
		return 0
	}

	return base.HexCasToUint64(s.Cas)
}

func HasUserXattrChanged(userXattr []byte, prevUserXattrHash string) bool {
	// If hash is empty but userXattr is not empty an xattr has been added. Import
	if prevUserXattrHash == "" && len(userXattr) > 0 {
		return true
	}

	// Otherwise check hash value
	return userXattrCrc32cHash(userXattr) != prevUserXattrHash
}

// SyncData.IsSGWrite - used during feed-based import
func (s *SyncData) IsSGWrite(cas uint64, rawBody []byte, rawUserXattr []byte) (isSGWrite bool, crc32Match bool, bodyChanged bool) {

	// If cas matches, it was a SG write
	if cas == s.GetSyncCas() {
		return true, false, false
	}

	// If crc32c hash of body doesn't match value stored in SG metadata then import is required
	if base.Crc32cHashString(rawBody) != s.Crc32c {
		return false, false, true
	}

	if HasUserXattrChanged(rawUserXattr, s.Crc32cUserXattr) {
		return false, false, false
	}

	return true, true, false
}
