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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
)

// When external revision storage is used, maximum body size (in bytes) to store inline.
// Non-winning bodies smaller than this size are more efficient to store inline.
const MaximumInlineBodySize = 250

type DocumentUnmarshalLevel uint8

// emptyDoc denotes an empty JSON document
const emptyDoc = `{}`

const (
	DocUnmarshalAll       = DocumentUnmarshalLevel(iota) // Unmarshals sync metadata and body
	DocUnmarshalSync                                     // Unmarshals all sync metadata
	DocUnmarshalNoHistory                                // Unmarshals sync metadata excluding history
	DocUnmarshalRev                                      // Unmarshals rev + CAS only
	DocUnmarshalCAS                                      // Unmarshals CAS (for import check) only
	DocUnmarshalNone                                     // No unmarshalling (skips import/upgrade check)
)

// Maps what users have access to what channels or roles, and when they got that access.
type UserAccessMap map[string]channels.TimedSet

type AttachmentsMeta map[string]interface{} // AttachmentsMeta metadata as included in sync metadata

// The sync-gateway metadata stored in the "_sync" property of a Couchbase document.
type SyncData struct {
	CurrentRev      string              `json:"rev"`
	NewestRev       string              `json:"new_rev,omitempty"` // Newest rev, if different from CurrentRev
	Flags           uint8               `json:"flags,omitempty"`
	Sequence        uint64              `json:"sequence,omitempty"`
	UnusedSequences []uint64            `json:"unused_sequences,omitempty"` // unused sequences due to update conflicts/CAS retry
	RecentSequences []uint64            `json:"recent_sequences,omitempty"` // recent sequences for this doc - used in server dedup handling
	History         RevTree             `json:"history"`
	Channels        channels.ChannelMap `json:"channels,omitempty"`
	Access          UserAccessMap       `json:"access,omitempty"`
	RoleAccess      UserAccessMap       `json:"role_access,omitempty"`
	Expiry          *time.Time          `json:"exp,omitempty"`           // Document expiry.  Information only - actual expiry/delete handling is done by bucket storage.  Needs to be pointer for omitempty to work (see https://github.com/golang/go/issues/4357)
	Cas             string              `json:"cas"`                     // String representation of a cas value, populated via macro expansion
	Crc32c          string              `json:"value_crc32c"`            // String representation of crc32c hash of doc body, populated via macro expansion
	TombstonedAt    int64               `json:"tombstoned_at,omitempty"` // Time the document was tombstoned.  Used for view compaction
	Attachments     AttachmentsMeta     `json:"attachments,omitempty"`

	// Only used for performance metrics:
	TimeSaved time.Time `json:"time_saved,omitempty"` // Timestamp of save.

	// Backward compatibility (the "deleted" field was, um, deleted in commit 4194f81, 2/17/14)
	Deleted_OLD bool `json:"deleted,omitempty"`

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
		History:         RevTree{},
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
	for k, revInfo := range sd.History {

		if revInfo.Channels != nil {
			redactedChannels := base.Set{}
			for existingChanKey := range revInfo.Channels {
				redactedChannels.Add(base.Sha1HashString(existingChanKey, salt))
			}
			revInfo.Channels = redactedChannels
		}

		redactedSyncData.History[k] = revInfo
	}

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

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
// In its JSON form, the body's properties are at top-level while the SyncData is in a special
// "_sync" property.
// Document doesn't do any locking - document instances aren't intended to be shared across multiple goroutines.
type Document struct {
	SyncData        // Sync metadata
	_body    Body   // Marshalled document body.  Unmarshalled lazily - should be accessed using Body()
	_rawBody []byte // Raw document body, as retrieved from the bucket.  Marshaled lazily - should be accessed using BodyBytes()
	ID       string `json:"-"` // Doc id.  (We're already using a custom MarshalJSON for *document that's based on body, so the json:"-" probably isn't needed here)
	Cas      uint64 // Document cas

	Deleted        bool
	DocExpiry      uint32
	RevID          string
	DocAttachments AttachmentsMeta
}

type revOnlySyncData struct {
	casOnlySyncData
	CurrentRev string `json:"rev"`
}

type casOnlySyncData struct {
	Cas string `json:"cas"`
}

func (doc *Document) UpdateBodyBytes(bodyBytes []byte) {
	doc._rawBody = bodyBytes
	doc._body = nil
}

func (doc *Document) UpdateBody(body Body) {
	doc._body = body
	doc._rawBody = nil
}

// Marshals both the body and sync data for a given document. If there is no rawbody already available then we will
// marshall it all in one go. Otherwise we will reduce marshalling as much as possible by only marshalling the sync data
// and injecting it into the existing raw body.
func (doc *Document) MarshalBodyAndSync() (retBytes []byte, err error) {
	if doc._rawBody != nil {
		bodyBytes, err := doc.BodyBytes()
		if err != nil {
			return nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalBodyAndSync() doc with id: %s. Error %v", base.UD(doc.ID), err))
		}
		return base.InjectJSONProperties(bodyBytes, base.KVPair{Key: base.SyncXattrName, Val: doc.SyncData})
	} else {
		return base.JSONMarshal(doc)
	}
}

func (doc *Document) MarshalBodyForWebhook() (retBytes []byte, err error) {
	bodyBytes, err := doc.BodyBytes()
	if err != nil {
		return nil, err
	}

	bodyBytes, err = base.InjectJSONProperties(
		bodyBytes,
		base.KVPair{Key: BodyId, Val: doc.ID},
		base.KVPair{Key: BodyRev, Val: doc.CurrentRev},
	)
	if err != nil {
		return nil, err
	}

	return bodyBytes, nil
}

// Returns a new empty document.
func NewDocument(docid string) *Document {
	return &Document{ID: docid, SyncData: SyncData{History: make(RevTree)}}
}

// Accessors for document properties.  To support lazy unmarshalling of document contents, all access should be done through accessors
func (doc *Document) Body() Body {
	var caller string
	if base.ConsoleLogLevel().Enabled(base.LevelTrace) {
		caller = base.GetCallersName(1, true)
	}

	if doc._body != nil {
		base.Tracef(base.KeyAll, "Already had doc body %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
		return doc._body
	}

	if doc._rawBody == nil {
		base.Tracef(base.KeyAll, "Empty doc body/rawBody %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
		doc._body = Body{}
		doc._rawBody = []byte(emptyDoc)
		return doc._body
	}

	base.Tracef(base.KeyAll, "        UNMARSHAL doc body %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
	err := doc._body.Unmarshal(doc._rawBody)
	if err != nil {
		base.Warnf(base.KeyAll, "Unable to unmarshal document body from raw body : %s", err)
		return nil
	}
	return doc._body
}

func (doc *Document) GetMutableBody() Body {
	if doc._body != nil {
		// already have a body unmarshalled, so we'll need to deep copy so callers can mutate
		return doc._body.Copy(BodyDeepCopy)
	}
	// Otherwise unmarshal as normal, but don't store it in the document
	var b Body
	if err := b.Unmarshal(doc._rawBody); err != nil {
		base.Warnf(base.KeyAll, "Unable to unmarshal document body from raw body : %s", err)
		return nil
	}
	return b
}

func (doc *Document) RemoveBody() {
	doc._body = nil
	doc._rawBody = nil
}

func (doc *Document) BodyBytes() ([]byte, error) {
	var caller string
	if base.ConsoleLogLevel().Enabled(base.LevelTrace) {
		caller = base.GetCallersName(1, true)
	}

	if doc._rawBody != nil {
		base.Tracef(base.KeyAll, "Already had doc rawBody %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
		return doc._rawBody, nil
	}

	if doc._body == nil {
		base.Tracef(base.KeyAll, "Empty doc body/rawBody %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
		doc._body = Body{}
		doc._rawBody = []byte(emptyDoc)
		return doc._rawBody, nil
	}

	base.Tracef(base.KeyAll, "        MARSHAL doc body %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
	bodyBytes, err := base.JSONMarshal(doc._body)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "Error marshalling document body")
	}
	doc._rawBody = bodyBytes
	return doc._rawBody, nil
}

// Unmarshals a document from JSON data. The doc ID isn't in the data and must be given.  Uses decode to ensure
// UseNumber handling is applied to numbers in the body.
func unmarshalDocument(docid string, data []byte) (*Document, error) {
	doc := NewDocument(docid)
	if len(data) > 0 {
		decoder := base.JSONDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(doc); err != nil {
			return nil, pkgerrors.Wrapf(err, "Error unmarshalling doc.")
		}

		if doc != nil && doc.Deleted_OLD {
			doc.Deleted_OLD = false
			doc.Flags |= channels.Deleted // Backward compatibility with old Deleted property
		}
	}
	return doc, nil
}

func unmarshalDocumentWithXattr(docid string, data []byte, xattrData []byte, cas uint64, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {

	if xattrData == nil || len(xattrData) == 0 {
		// If no xattr data, unmarshal as standard doc
		doc, err = unmarshalDocument(docid, data)
	} else {
		doc = NewDocument(docid)
		err = doc.UnmarshalWithXattr(data, xattrData, unmarshalLevel)
	}
	if err != nil {
		return nil, err
	}
	doc.Cas = cas
	return doc, nil
}

// Unmarshals just a document's sync metadata from JSON data.
// (This is somewhat faster, if all you need is the sync data without the doc body.)
func UnmarshalDocumentSyncData(data []byte, needHistory bool) (*SyncData, error) {
	var root documentRoot
	if needHistory {
		root.SyncData = &SyncData{History: make(RevTree)}
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
func UnmarshalDocumentSyncDataFromFeed(data []byte, dataType uint8, needHistory bool) (result *SyncData, rawBody []byte, rawXattr []byte, err error) {

	var body []byte

	// If attr datatype flag is set, data includes both xattrs and document body.  Check for presence of sync xattr.
	// Note that there could be a non-sync xattr present
	if dataType&base.MemcachedDataTypeXattr != 0 {
		var syncXattr []byte
		body, syncXattr, err = parseXattrStreamData(base.SyncXattrName, data)
		if err != nil {
			return nil, nil, nil, err
		}

		// If the sync xattr is present, use that to build SyncData
		if syncXattr != nil && len(syncXattr) > 0 {
			result = &SyncData{}
			if needHistory {
				result.History = make(RevTree)
			}
			err = base.JSONUnmarshal(syncXattr, result)
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

	if len(data) < 4 {
		return nil, nil, base.ErrEmptyMetadata
	}

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

func (doc *SyncData) HasValidSyncData() bool {

	valid := doc != nil && doc.CurrentRev != "" && (doc.Sequence > 0)
	return valid
}

// Converts the string hex encoding that's stored in the sync metadata to a uint64 cas value
func (s *SyncData) GetSyncCas() uint64 {

	if s.Cas == "" {
		return 0
	}

	return base.HexCasToUint64(s.Cas)
}

// SyncData.IsSGWrite - used during feed-based import
func (s *SyncData) IsSGWrite(cas uint64, rawBody []byte) (isSGWrite bool, crc32Match bool) {

	// If cas matches, it was a SG write
	if cas == s.GetSyncCas() {
		return true, false
	}

	// If crc32c hash of body matches value stored in SG metadata, SG metadata is still valid
	if base.Crc32cHashString(rawBody) == s.Crc32c {
		return true, true
	}

	return false, false
}

// doc.IsSGWrite - used during on-demand import.  Doesn't invoke SyncData.IsSGWrite so that we
// can complete the inexpensive cas check before the (potential) doc marshalling.
func (doc *Document) IsSGWrite(rawBody []byte) (isSGWrite bool, crc32Match bool) {

	// If the raw body is available, use SyncData.IsSGWrite
	if rawBody != nil && len(rawBody) > 0 {

		isSgWriteFeed, crc32MatchFeed := doc.SyncData.IsSGWrite(doc.Cas, rawBody)
		if !isSgWriteFeed {
			base.Debugf(base.KeyCRUD, "Doc %s is not an SG write, based on cas and body hash. cas:%x syncCas:%q", base.UD(doc.ID), doc.Cas, doc.SyncData.Cas)
		}

		return isSgWriteFeed, crc32MatchFeed

	}

	// If raw body isn't available, first do the inexpensive cas check
	if doc.Cas == doc.SyncData.GetSyncCas() {
		return true, false
	}

	// Since raw body isn't available, marshal from the document to perform body hash comparison
	bodyBytes, err := doc.BodyBytes()
	if err != nil {
		base.Warnf(base.KeyAll, "Unable to marshal doc body during SG write check for doc %s. Error: %v", base.UD(doc.ID), err)
		return false, false
	}
	if base.Crc32cHashString(bodyBytes) == doc.SyncData.Crc32c {
		return true, true
	}

	base.Debugf(base.KeyCRUD, "Doc %s is not an SG write, based on cas and body hash. cas:%x syncCas:%q", base.UD(doc.ID), doc.Cas, doc.SyncData.Cas)
	return false, false
}

func (doc *Document) hasFlag(flag uint8) bool {
	return doc.Flags&flag != 0
}

func (doc *Document) setFlag(flag uint8, state bool) {
	if state {
		doc.Flags |= flag
	} else {
		doc.Flags &^= flag
	}
}

func (doc *Document) newestRevID() string {
	if doc.NewestRev != "" {
		return doc.NewestRev
	}
	return doc.CurrentRev
}

// RevLoaderFunc and RevWriterFunc manage persistence of non-winning revision bodies that are stored outside the document.
type RevLoaderFunc func(key string) ([]byte, error)

func (db *DatabaseContext) RevisionBodyLoader(key string) ([]byte, error) {
	body, _, err := db.Bucket.GetRaw(key)
	return body, err
}

// Fetches the body of a revision as a map, or nil if it's not available.
func (doc *Document) getRevisionBody(revid string, loader RevLoaderFunc) Body {
	var body Body
	if revid == doc.CurrentRev {
		body = doc.Body()
	} else {
		body = doc.getNonWinningRevisionBody(revid, loader)
	}
	return body
}

// Retrieves a non-winning revision body.  If not already loaded in the document (either because inline,
// or was previously requested), loader function is used to retrieve from the bucket.
func (doc *Document) getNonWinningRevisionBody(revid string, loader RevLoaderFunc) Body {
	var body Body
	bodyBytes, found := doc.History.getRevisionBody(revid, loader)
	if !found || len(bodyBytes) == 0 {
		return nil
	}

	if err := body.Unmarshal(bodyBytes); err != nil {
		base.Warnf(base.KeyAll, "Unexpected error parsing body of rev %q: %v", revid, err)
		return nil
	}
	return body
}

// Fetches the body of a revision as JSON, or nil if it's not available.
func (doc *Document) getRevisionBodyJSON(revid string, loader RevLoaderFunc) []byte {
	var bodyJSON []byte
	if revid == doc.CurrentRev {
		var marshalErr error
		bodyJSON, marshalErr = doc.BodyBytes()
		if marshalErr != nil {
			base.Warnf(base.KeyAll, "Marshal error when retrieving active current revision body: %v", marshalErr)
		}
	} else {
		bodyJSON, _ = doc.History.getRevisionBody(revid, loader)
	}
	return bodyJSON
}

func (doc *Document) removeRevisionBody(revID string) {
	removedBodyKey := doc.History.removeRevisionBody(revID)
	if removedBodyKey != "" {
		if doc.removedRevisionBodyKeys == nil {
			doc.removedRevisionBodyKeys = make(map[string]string)
		}
		doc.removedRevisionBodyKeys[revID] = removedBodyKey
	}
}

// makeBodyActive moves a previously non-winning revision body from the rev tree to the document body
func (doc *Document) promoteNonWinningRevisionBody(revid string, loader RevLoaderFunc) {
	// If the new revision is not current, transfer the current revision's
	// body to the top level doc._body:
	doc.UpdateBody(doc.getNonWinningRevisionBody(revid, loader))
	doc.removeRevisionBody(revid)
}

func (doc *Document) pruneRevisions(maxDepth uint32, keepRev string) int {
	numPruned, prunedTombstoneBodyKeys := doc.History.pruneRevisions(maxDepth, keepRev)
	for revID, bodyKey := range prunedTombstoneBodyKeys {
		if doc.removedRevisionBodyKeys == nil {
			doc.removedRevisionBodyKeys = make(map[string]string)
		}
		doc.removedRevisionBodyKeys[revID] = bodyKey
	}
	return numPruned
}

// Adds a revision body (as Body) to a document.  Removes special properties first.
func (doc *Document) setRevisionBody(revid string, newDoc *Document, storeInline bool) {
	if revid == doc.CurrentRev {
		doc._body = newDoc._body
		doc._rawBody = newDoc._rawBody
	} else {
		bodyBytes, _ := newDoc.BodyBytes()
		doc.setNonWinningRevisionBody(revid, bodyBytes, storeInline)
	}
}

// Adds a revision body (as []byte) to a document.  Flags for external storage when appropriate
func (doc *Document) setNonWinningRevisionBody(revid string, body []byte, storeInline bool) {
	revBodyKey := ""
	if !storeInline && len(body) > MaximumInlineBodySize {
		revBodyKey = generateRevBodyKey(doc.ID, revid)
		doc.addedRevisionBodies = append(doc.addedRevisionBodies, revid)
	}
	doc.History.setRevisionBody(revid, body, revBodyKey)
}

// persistModifiedRevisionBodies writes new non-inline revisions to the bucket.
// Should be invoked BEFORE the document is successfully committed.
func (doc *Document) persistModifiedRevisionBodies(bucket base.Bucket) error {

	for _, revID := range doc.addedRevisionBodies {
		// if this rev is also in the delete set, skip add/delete
		_, ok := doc.removedRevisionBodyKeys[revID]
		if ok {
			delete(doc.removedRevisionBodyKeys, revID)
			continue
		}

		revInfo, err := doc.History.getInfo(revID)
		if revInfo == nil || err != nil {
			return err
		}
		if revInfo.BodyKey == "" || len(revInfo.Body) == 0 {
			return base.RedactErrorf("Missing key or body for revision during external persistence.  doc: %s rev:%s key: %s  len(body): %d", base.UD(doc.ID), revID, base.UD(revInfo.BodyKey), len(revInfo.Body))
		}

		// If addRaw indicates that the doc already exists, can ignore.  Another writer already persisted this rev backup.
		addErr := doc.persistRevisionBody(bucket, revInfo.BodyKey, revInfo.Body)
		if addErr != nil {
			return err
		}
	}

	doc.addedRevisionBodies = []string{}
	return nil
}

// deleteRemovedRevisionBodies deletes obsolete non-inline revisions from the bucket.
// Should be invoked AFTER the document is successfully committed.
func (doc *Document) deleteRemovedRevisionBodies(bucket base.Bucket) {

	for _, revBodyKey := range doc.removedRevisionBodyKeys {
		deleteErr := bucket.Delete(revBodyKey)
		if deleteErr != nil {
			base.Warnf(base.KeyAll, "Unable to delete old revision body using key %s - will not be deleted from bucket.", revBodyKey)
		}
	}
	doc.removedRevisionBodyKeys = map[string]string{}
}

func (doc *Document) persistRevisionBody(bucket base.Bucket, key string, body []byte) error {
	_, err := bucket.AddRaw(key, 0, body)
	return err
}

// Move any large revision bodies to external document storage
func (doc *Document) migrateRevisionBodies(bucket base.Bucket) error {

	for _, revID := range doc.History.GetLeaves() {
		revInfo, err := doc.History.getInfo(revID)
		if err != nil {
			continue
		}
		if len(revInfo.Body) > MaximumInlineBodySize {
			bodyKey := generateRevBodyKey(doc.ID, revID)
			persistErr := doc.persistRevisionBody(bucket, bodyKey, revInfo.Body)
			if persistErr != nil {
				base.Warnf(base.KeyAll, "Unable to store revision body for doc %s, rev %s externally: %v", base.UD(doc.ID), revID, persistErr)
				continue
			}
			revInfo.BodyKey = bodyKey
		}
	}
	return nil
}

func generateRevBodyKey(docid, revid string) (revBodyKey string) {
	return base.RevBodyPrefix + generateRevDigest(docid, revid)
}

func generateRevDigest(docid, revid string) string {
	digester := sha256.New()
	digester.Write([]byte(docid))
	digester.Write([]byte(revid))
	return base64.StdEncoding.EncodeToString(digester.Sum(nil))
}

// Updates the expiry for a document
func (doc *Document) UpdateExpiry(expiry uint32) {

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
func (doc *Document) updateChannels(newChannels base.Set) (changedChannels base.Set, err error) {
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
		base.Infof(base.KeyCRUD, "\tDoc %q / %q in channels %q", base.UD(doc.ID), doc.CurrentRev, base.UD(newChannels))
		changedChannels, err = channels.SetFromArray(changed, channels.KeepStar)
	}
	return
}

// Determine whether the specified revision was a channel removal, based on doc.Channels.  If so, construct the standard document body for a
// removal notification (_removed=true)
func (doc *Document) IsChannelRemoval(revID string) (bodyBytes []byte, history Revisions, channels base.Set, isRemoval bool, isDelete bool, err error) {

	channels = make(base.Set)

	// Iterate over the document's channel history, looking for channels that were removed at revID.  If found, also identify whether the removal was a tombstone.
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
		return nil, nil, nil, false, false, nil
	}

	// Construct removal body
	// doc ID and rev ID aren't required to be inserted here, as both of those are available in the request.
	if isDelete {
		bodyBytes = []byte(`{"` + BodyDeleted + `":true,"` + BodyRemoved + `":true}`)
	} else {
		bodyBytes = []byte(`{"` + BodyRemoved + `":true}`)
	}

	// Build revision history for revID
	revHistory, err := doc.History.getHistory(revID)
	if err != nil {
		return nil, nil, nil, false, false, err
	}

	// If there's no history (because the revision has been pruned from the rev tree), treat revision history as only the specified rev id.
	if len(revHistory) == 0 {
		revHistory = []string{revID}
	}
	history = encodeRevisions(revHistory)

	return bodyBytes, history, channels, true, isDelete, nil
}

// Updates a document's channel/role UserAccessMap with new access settings from an AccessMap.
// Returns an array of the user/role names whose access has changed as a result.
func (accessMap *UserAccessMap) updateAccess(doc *Document, newAccess channels.AccessMap) (changedUsers []string) {
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
		base.Infof(base.KeyAccess, "Doc %q grants %s access: %v", base.UD(doc.ID), what, base.UD(*accessMap))
	}
	return changedUsers
}

//////// MARSHALING ////////

type documentRoot struct {
	SyncData *SyncData `json:"_sync"`
}

func (doc *Document) UnmarshalJSON(data []byte) error {
	if doc.ID == "" {
		panic("Doc was unmarshaled without ID set")
	}

	// Unmarshal only sync data (into a typed struct)
	syncData := documentRoot{SyncData: &SyncData{History: make(RevTree)}}
	err := base.JSONUnmarshal(data, &syncData)
	if err != nil {
		return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalJSON() doc with id: %s.  Error: %v", base.UD(doc.ID), err))
	}
	if syncData.SyncData != nil {
		doc.SyncData = *syncData.SyncData
	}

	// Unmarshal the rest of the doc body as map[string]interface{}
	if err := doc._body.Unmarshal(data); err != nil {
		return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalJSON() doc with id: %s.  Error: %v", base.UD(doc.ID), err))
	}
	// Remove _sync from body
	delete(doc._body, base.SyncXattrName)

	return nil
}

func (doc *Document) MarshalJSON() (data []byte, err error) {
	if doc._rawBody != nil {
		data, err = base.InjectJSONProperties(doc._rawBody, base.KVPair{
			Key: base.SyncXattrName,
			Val: doc.SyncData,
		})
	} else {
		body := doc._body
		if body == nil {
			body = Body{}
		}
		body[base.SyncXattrName] = &doc.SyncData
		data, err = base.JSONMarshal(body)
		delete(body, base.SyncXattrName)
		if err != nil {
			err = pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalJSON() doc with id: %s.  Error: %v", base.UD(doc.ID), err))
		}
	}
	return data, err
}

// UnmarshalWithXattr unmarshals the provided raw document and xattr bytes.  The provided DocumentUnmarshalLevel
// (unmarshalLevel) specifies how much of the provided document/xattr needs to be initially unmarshalled.  If
// unmarshalLevel is anything less than the full document + metadata, the raw data is retained for subsequent
// lazy unmarshalling as needed.
func (doc *Document) UnmarshalWithXattr(data []byte, xdata []byte, unmarshalLevel DocumentUnmarshalLevel) error {
	if doc.ID == "" {
		base.Warnf(base.KeyAll, "Attempted to unmarshal document without ID set")
		return errors.New("Document was unmarshalled without ID set")
	}

	switch unmarshalLevel {
	case DocUnmarshalAll, DocUnmarshalSync:
		// Unmarshal full document and/or sync metadata
		doc.SyncData = SyncData{History: make(RevTree)}
		unmarshalErr := base.JSONUnmarshal(xdata, &doc.SyncData)
		if unmarshalErr != nil {
			return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattr() doc with id: %s (DocUnmarshalAll/Sync).  Error: %v", base.UD(doc.ID), unmarshalErr))
		}
		// Unmarshal body if requested and present
		if unmarshalLevel == DocUnmarshalAll && len(data) > 0 {
			return doc._body.Unmarshal(data)
		} else {
			doc._rawBody = data
		}

	case DocUnmarshalNoHistory:
		// Unmarshal sync metadata only, excluding history
		doc.SyncData = SyncData{}
		unmarshalErr := base.JSONUnmarshal(xdata, &doc.SyncData)
		if unmarshalErr != nil {
			return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattr() doc with id: %s (DocUnmarshalNoHistory).  Error: %v", base.UD(doc.ID), unmarshalErr))
		}
		doc._rawBody = data
	case DocUnmarshalRev:
		// Unmarshal only rev and cas from sync metadata
		var revOnlyMeta revOnlySyncData
		unmarshalErr := base.JSONUnmarshal(xdata, &revOnlyMeta)
		if unmarshalErr != nil {
			return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattr() doc with id: %s (DocUnmarshalRev).  Error: %v", base.UD(doc.ID), unmarshalErr))
		}
		doc.SyncData = SyncData{
			CurrentRev: revOnlyMeta.CurrentRev,
			Cas:        revOnlyMeta.Cas,
		}
		doc._rawBody = data
	case DocUnmarshalCAS:
		// Unmarshal only cas from sync metadata
		var casOnlyMeta casOnlySyncData
		unmarshalErr := base.JSONUnmarshal(xdata, &casOnlyMeta)
		if unmarshalErr != nil {
			return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattr() doc with id: %s (DocUnmarshalCAS).  Error: %v", base.UD(doc.ID), unmarshalErr))
		}
		doc.SyncData = SyncData{
			Cas: casOnlyMeta.Cas,
		}
		doc._rawBody = data
	}

	// If there's no body, but there is an xattr, set deleted flag
	if len(data) == 0 && len(xdata) > 0 {
		doc.Deleted = true
	}
	return nil
}

func (doc *Document) MarshalWithXattr() (data []byte, xdata []byte, err error) {
	// Grab the rawBody if it's already marshalled, otherwise unmarshal the body
	if doc._rawBody != nil {
		data = doc._rawBody
	} else {
		body := doc._body
		// If body is non-empty and non-deleted, unmarshal and return
		if body != nil {
			// TODO: Could we check doc.Deleted?
			deleted, _ := body[BodyDeleted].(bool)
			if !deleted {
				data, err = base.JSONMarshal(body)
				if err != nil {
					return nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattr() doc body with id: %s.  Error: %v", base.UD(doc.ID), err))
				}
			}
		}
	}

	xdata, err = base.JSONMarshal(doc.SyncData)
	if err != nil {
		return nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattr() doc SyncData with id: %s.  Error: %v", base.UD(doc.ID), err))
	}

	return data, xdata, nil
}
