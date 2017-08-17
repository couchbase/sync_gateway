//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Maps what users have access to what channels or roles, and when they got that access.
type UserAccessMap map[string]channels.TimedSet

// The sync-gateway metadata stored in the "_sync" property of a Couchbase document.
type syncData struct {
	CurrentRev      string              `json:"rev"`
	NewestRev       string              `json:"new_rev,omitempty"` // Newest rev, if different from CurrentRev
	Flags           uint8               `json:"flags,omitempty"`
	Sequence        uint64              `json:"sequence,omitempty"`
	UnusedSequences []uint64            `json:"unused_sequences,omitempty"` // unused due to update conflicts
	RecentSequences []uint64            `json:"recent_sequences,omitempty"` // recent sequences for this doc - used in server dedup handling
	History         RevTree             `json:"history"`
	Channels        channels.ChannelMap `json:"channels,omitempty"`
	Access          UserAccessMap       `json:"access,omitempty"`
	RoleAccess      UserAccessMap       `json:"role_access,omitempty"`
	Expiry          *time.Time          `json:"exp,omitempty"`           // Document expiry.  Information only - actual expiry/delete handling is done by bucket storage.  Needs to be pointer for omitempty to work (see https://github.com/golang/go/issues/4357)
	Cas             string              `json:"cas"`                     // String representation of a cas value, populated via macro expansion
	TombstonedAt    int64               `json:"tombstoned_at,omitempty"` // Time the document was tombstoned.  Used for view compaction

	// Fields used by bucket-shadowing:
	UpstreamCAS *uint64 `json:"upstream_cas,omitempty"` // CAS value of remote doc
	UpstreamRev string  `json:"upstream_rev,omitempty"` // Rev ID remote doc was saved as

	// Only used for performance metrics:
	TimeSaved time.Time `json:"time_saved,omitempty"` // Timestamp of save.

	// Backward compatibility (the "deleted" field was, um, deleted in commit 4194f81, 2/17/14)
	Deleted_OLD bool `json:"deleted,omitempty"`
}

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
// In its JSON form, the body's properties are at top-level while the syncData is in a special
// "_sync" property.
type document struct {
	syncData
	body Body
	ID   string `json:"-"`
	Cas  uint64
}

// Returns a new empty document.
func newDocument(docid string) *document {
	return &document{ID: docid, syncData: syncData{History: make(RevTree)}}
}

// Unmarshals a document from JSON data. The doc ID isn't in the data and must be given.
func unmarshalDocument(docid string, data []byte) (*document, error) {
	doc := newDocument(docid)
	if len(data) > 0 {
		if err := json.Unmarshal(data, doc); err != nil {
			return nil, err
		}
		if doc != nil && doc.Deleted_OLD {
			doc.Deleted_OLD = false
			doc.Flags |= channels.Deleted // Backward compatibility with old Deleted property
		}
	}
	return doc, nil
}

func unmarshalDocumentWithXattr(docid string, data []byte, xattrData []byte, cas uint64) (doc *document, err error) {

	if xattrData == nil || len(xattrData) == 0 {
		// If no xattr data, unmarshal as standard doc
		doc, err = unmarshalDocument(docid, data)
	} else {
		doc = newDocument(docid)
		err = doc.UnmarshalWithXattr(data, xattrData)
	}
	if err != nil {
		return nil, err
	}
	doc.Cas = cas
	return doc, nil
}

// Unmarshals just a document's sync metadata from JSON data.
// (This is somewhat faster, if all you need is the sync data without the doc body.)
func UnmarshalDocumentSyncData(data []byte, needHistory bool) (*syncData, error) {
	var root documentRoot
	if needHistory {
		root.SyncData = &syncData{History: make(RevTree)}
	}
	if err := json.Unmarshal(data, &root); err != nil {
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
func UnmarshalDocumentSyncDataFromFeed(data []byte, dataType uint8, needHistory bool) (result *syncData, rawBody []byte, rawXattr []byte, err error) {

	var body []byte

	// If attr datatype flag is set, data includes both xattrs and document body.  Check for presence of sync xattr.
	// Note that there could be a non-sync xattr present
	if dataType&base.MemcachedDataTypeXattr != 0 {
		var syncXattr []byte
		body, syncXattr, err = parseXattrStreamData(KSyncXattrName, data)
		if err != nil {
			return nil, nil, nil, err
		}

		// If the sync xattr is present, use that to build syncData
		if syncXattr != nil && len(syncXattr) > 0 {
			result = &syncData{}
			if needHistory {
				result.History = make(RevTree)
			}
			err = json.Unmarshal(syncXattr, result)
			if err != nil {
				return nil, nil, nil, err
			}
			return result, body, syncXattr, nil
		}
	} else {
		// Xattr flag not set - data is just the document body
		body = data
	}

	// Non-xattr data, or sync xattr not present.  Attempt to retrieve sync metadata from document body
	result, err = UnmarshalDocumentSyncData(body, needHistory)
	return result, body, nil, err
}

// parseXattrStreamData returns the raw bytes of the body and the requested xattr (when present) from the raw DCP data bytes.
// Details on format (taken from https://docs.google.com/document/d/18UVa5j8KyufnLLy29VObbWRtoBn9vs8pcxttuMt6rz8/edit#heading=h.caqiui1pmmmb.):
/*
	When the XATTR bit is set the first uint32_t in the body contains the size of the entire XATTR section.


	      Byte/     0       |       1       |       2       |       3       |
	         /              |               |               |               |
	        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	        +---------------+---------------+---------------+---------------+
	       0| Total xattr length in network byte order                      |
	        +---------------+---------------+---------------+---------------+

	Following the length you'll find an iovector-style encoding of all of the XATTR key-value pairs with the following encoding:

	uint32_t length of next xattr pair (network order)
	xattr key in modified UTF-8
	0x00
	xattr value in modified UTF-8
	0x00

	The 0x00 byte after the key saves us from storing a key length, and the trailing 0x00 is just for convenience to allow us to use string functions to search in them.
*/

func parseXattrStreamData(xattrName string, data []byte) (body []byte, xattr []byte, err error) {

	xattrsLen := binary.BigEndian.Uint32(data[0:4])
	body = data[xattrsLen+4:]
	if xattrsLen == 0 {
		return body, nil, nil
	}

	// In the xattr key/value pairs, key and value are both terminated by 0x00 (byte(0)).  Use this as a separator to split the byte slice
	separator := []byte("\x00")

	// Iterate over xattr key/value pairs
	pos := uint32(4)
	for pos < xattrsLen {
		pairLen := binary.BigEndian.Uint32(data[pos : pos+4])
		if pairLen == 0 || int(pos+pairLen) > len(data) {
			return nil, nil, fmt.Errorf("Unexpected xattr pair length (%d) - unable to parse xattrs", pairLen)
		}
		pos += 4
		pairBytes := data[pos : pos+pairLen]
		components := bytes.Split(pairBytes, separator)
		// xattr pair has the format [key]0x00[value]0x00, and so should split into three components
		if len(components) != 3 {
			return nil, nil, fmt.Errorf("Unexpected number of components found in xattr pair: %s", pairBytes)
		}
		xattrKey := string(components[0])
		// If this is the xattr we're looking for , we're done
		if xattrName == xattrKey {
			return body, components[1], nil
		}
		pos += pairLen
	}

	return body, xattr, nil
}

func (doc *syncData) HasValidSyncData(requireSequence bool) bool {
	valid := doc != nil && doc.CurrentRev != "" && (doc.Sequence > 0 || !requireSequence)
	return valid
}

// Converts the string hex encoding that's stored in the sync metadata to a uint64 cas value
func (s *syncData) GetSyncCas() uint64 {

	if s.Cas == "" {
		return 0
	}

	casBytes, err := hex.DecodeString(strings.TrimPrefix(s.Cas, "0x"))
	if err != nil || len(casBytes) != 8 {
		// Invalid cas - return zero
		return 0
	}

	return binary.LittleEndian.Uint64(casBytes[0:8])
}

// Checks whether the specified cas matches the cas value in the sync metadata.
func (s *syncData) IsSGWrite(cas uint64) bool {
	return cas == s.GetSyncCas()
}

func (doc *document) IsSGWrite() bool {
	result := doc.syncData.IsSGWrite(doc.Cas)
	if result == false {
		base.LogTo("CRUD+", "Doc %s is not an SG write, based on cas. cas:%x syncCas:%q", doc.ID, doc.Cas, doc.syncData.Cas)
	}
	return result
}

func (doc *document) hasFlag(flag uint8) bool {
	return doc.Flags&flag != 0
}

func (doc *document) setFlag(flag uint8, state bool) {
	if state {
		doc.Flags |= flag
	} else {
		doc.Flags &^= flag
	}
}

func (doc *document) newestRevID() string {
	if doc.NewestRev != "" {
		return doc.NewestRev
	}
	return doc.CurrentRev
}

// Fetches the body of a revision as a map, or nil if it's not available.
func (doc *document) getRevision(revid string) Body {
	var body Body
	if revid == doc.CurrentRev {
		body = doc.body
	} else {
		body = doc.History.getParsedRevisionBody(revid)
		if body == nil {
			return nil
		}
	}
	return body
}

// Fetches the body of a revision as JSON, or nil if it's not available.
func (doc *document) getRevisionJSON(revid string) []byte {
	var bodyJSON []byte
	if revid == doc.CurrentRev {
		bodyJSON, _ = json.Marshal(doc.body)
	} else {
		bodyJSON, _ = doc.History.getRevisionBody(revid)
	}
	return bodyJSON
}

// Adds a revision body to a document.
func (doc *document) setRevision(revid string, body Body) {
	strippedBody := stripSpecialProperties(body)
	if revid == doc.CurrentRev {
		doc.body = strippedBody
	} else {
		var asJson []byte
		if len(body) > 0 {
			asJson, _ = json.Marshal(stripSpecialProperties(body))
		}
		doc.History.setRevisionBody(revid, asJson)
	}
}

// Updates the expiry for a document
func (doc *document) UpdateExpiry(expiry uint32) {

	if expiry == 0 {
		doc.Expiry = nil
	} else {
		expireTime := base.CbsExpiryToTime(expiry)
		doc.Expiry = &expireTime
	}
}

//////// CHANNELS & ACCESS:

// Updates the Channels property of a document object with current & past channels.
// Returns the set of channels that have changed (document joined or left in this revision)
func (doc *document) updateChannels(newChannels base.Set) (changedChannels base.Set) {
	var changed []string
	oldChannels := doc.Channels
	if oldChannels == nil {
		oldChannels = channels.ChannelMap{}
		doc.Channels = oldChannels
	} else {
		// Mark every no-longer-current channel as unsubscribed:
		curSequence := doc.Sequence
		for channel, removal := range oldChannels {
			if removal == nil && !newChannels.Contains(channel) {
				oldChannels[channel] = &channels.ChannelRemoval{
					Seq:     curSequence,
					RevID:   doc.CurrentRev,
					Deleted: doc.hasFlag(channels.Deleted)}
				changed = append(changed, channel)
			}
		}
	}

	// Mark every current channel as subscribed:
	for channel := range newChannels {
		if value, exists := oldChannels[channel]; value != nil || !exists {
			oldChannels[channel] = nil
			changed = append(changed, channel)
		}
	}
	if changed != nil {
		base.LogTo("CRUD", "\tDoc %q in channels %q", doc.ID, newChannels)
		changedChannels = channels.SetOf(changed...)
	}
	return
}

// Determine whether the specified revision was a channel removal, based on doc.Channels.  If so, construct the standard document body for a
// removal notification (_removed=true)
func (doc *document) IsChannelRemoval(revID string) (body Body, history Body, channels base.Set, isRemoval bool) {

	channels = make(base.Set)

	// Iterate over the document's channel history, looking for channels that were removed at revID.  If found, also identify whether the removal was a tombstone.
	isDelete := false
	for channel, removal := range doc.Channels {
		if removal != nil && removal.RevID == revID {
			channels[channel] = struct{}{}
			if removal.Deleted == true {
				isDelete = true
			}
		}
	}
	// If no matches found, return isRemoval=false
	if len(channels) == 0 {
		return nil, nil, nil, false
	}

	// Construct removal body
	body = Body{
		"_id":      doc.ID,
		"_rev":     revID,
		"_removed": true,
	}
	if isDelete {
		body["_deleted"] = true
	}

	// Build revision history for revID
	revHistory := doc.History.getHistory(revID)
	// If there's no history (because the revision has been pruned from the rev tree), treat revision history as only the specified rev id.
	if len(revHistory) == 0 {
		revHistory = []string{revID}
	}
	history = encodeRevisions(revHistory)

	return body, history, channels, true
}

// Updates a document's channel/role UserAccessMap with new access settings from an AccessMap.
// Returns an array of the user/role names whose access has changed as a result.
func (accessMap *UserAccessMap) updateAccess(doc *document, newAccess channels.AccessMap) (changedUsers []string) {
	// Update users already appearing in doc.Access:
	for name, access := range *accessMap {
		if access.UpdateAtSequence(newAccess[name], doc.Sequence) {
			if len(access) == 0 {
				delete(*accessMap, name)
			}
			changedUsers = append(changedUsers, name)
		}
	}
	// Add new users who are in newAccess but not accessMap:
	for name, access := range newAccess {
		if _, existed := (*accessMap)[name]; !existed {
			if *accessMap == nil {
				*accessMap = UserAccessMap{}
			}
			(*accessMap)[name] = channels.AtSequence(access, doc.Sequence)
			changedUsers = append(changedUsers, name)
		}
	}
	if changedUsers != nil {
		what := "channel"
		if accessMap == &doc.RoleAccess {
			what = "role"
		}
		base.LogTo("Access", "Doc %q grants %s access: %v", doc.ID, what, *accessMap)
	}
	return changedUsers
}

//////// MARSHALING ////////

type documentRoot struct {
	SyncData *syncData `json:"_sync"`
}

func (doc *document) UnmarshalJSON(data []byte) error {
	if doc.ID == "" {
		panic("Doc was unmarshaled without ID set")
	}
	root := documentRoot{SyncData: &syncData{History: make(RevTree)}}
	err := json.Unmarshal([]byte(data), &root)
	if err != nil {
		base.Warn("Error unmarshaling doc %q: %s", doc.ID, err)
		return err
	}
	if root.SyncData != nil {
		doc.syncData = *root.SyncData
	}

	if err := doc.unmarshalBody(data); err != nil {
		return err
	}

	delete(doc.body, "_sync")
	return nil
}

func (doc *document) MarshalJSON() ([]byte, error) {
	body := doc.body
	if body == nil {
		body = Body{}
	}
	body["_sync"] = &doc.syncData
	data, err := json.Marshal(body)
	delete(body, "_sync")
	return data, err
}

func (doc *document) UnmarshalWithXattr(data []byte, xdata []byte) error {
	if doc.ID == "" {
		base.Warn("Attempted to unmarshal document without ID set")
		return errors.New("Document was unmarshalled without ID set")
	}

	// Unmarshal sync metadata
	doc.syncData = syncData{History: make(RevTree)}
	if err := json.Unmarshal(xdata, &doc.syncData); err != nil {
		return err
	}
	// Unmarshal document body, if present
	if len(data) > 0 {
		return doc.unmarshalBody(data)
	}

	// If there's no body, but there is an xattr, set body as {"_deleted":true} to align with non-xattr handling
	if len(xdata) > 0 {
		doc.body = Body{}
		doc.body["_deleted"] = true
	}
	return nil
}

func (doc *document) MarshalWithXattr() (data []byte, xdata []byte, err error) {

	body := doc.body
	// If body is non-empty and non-deleted, unmarshal and return
	if body != nil {
		deleted, _ := body["_deleted"].(bool)
		if !deleted {
			data, err = json.Marshal(body)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	xdata, err = json.Marshal(doc.syncData)
	if err != nil {
		return nil, nil, err
	}

	return data, xdata, nil
}

func (doc *document) unmarshalBody(data []byte) error {
	err := json.Unmarshal([]byte(data), &doc.body)
	if err != nil {
		base.Warn("Error unmarshaling body of doc %q: %s", doc.ID, err)
	}
	return err
}
