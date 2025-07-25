//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
)

// When external revision storage is used, maximum body size (in bytes) to store inline.
// Non-winning bodies smaller than this size are more efficient to store inline.
const MaximumInlineBodySize = 250

// DocumentHistoryMaxEntriesPerChannel is the maximum allowed entries per channel in the Document ChannelSetHistory
const DocumentHistoryMaxEntriesPerChannel = 5

type DocumentUnmarshalLevel uint8

const (
	DocUnmarshalAll       = DocumentUnmarshalLevel(iota) // Unmarshals metadata and body
	DocUnmarshalSync                                     // Unmarshals metadata
	DocUnmarshalNoHistory                                // Unmarshals metadata excluding revtree history
	DocUnmarshalHistory                                  // Unmarshals revtree history + rev + CAS only
	DocUnmarshalRev                                      // Unmarshals revTreeID + CAS only (no HLV)
	DocUnmarshalCAS                                      // Unmarshals CAS (for import check) only
	DocUnmarshalNone                                     // No unmarshalling (skips import/upgrade check)
)

const (
	// RemovedRedactedDocument is returned by SG when a given document has been dropped out of a channel
	RemovedRedactedDocument = `{"` + BodyRemoved + `":true}`
	// DeletedDocument is returned by SG when a given document has been deleted
	DeletedDocument = `{"` + BodyDeleted + `":true}`
)

// Maps what users have access to what channels or roles, and when they got that access.
type UserAccessMap map[string]channels.TimedSet

type AttachmentsMeta map[string]interface{} // AttachmentsMeta metadata as included in sync metadata

type ChannelSetEntry struct {
	Name      string `json:"name"`
	Start     uint64 `json:"start"`
	End       uint64 `json:"end,omitempty"`
	Compacted bool   `json:"compacted,omitempty"`
}

// MetadataOnlyUpdate represents a cas value of a document modification if it only updated xattrs and not the document body. The previous cas and revSeqNo are stored as the version of the document before any metadata was modified. This is serialized as _mou.
type MetadataOnlyUpdate struct {
	HexCAS           string `json:"cas,omitempty"`  // 0x0 hex value from Couchbase Server
	PreviousHexCAS   string `json:"pCas,omitempty"` // 0x0 hex value from Couchbase Server
	PreviousRevSeqNo uint64 `json:"pRev,omitempty"`
}

func (m *MetadataOnlyUpdate) String() string {
	return fmt.Sprintf("{CAS:%d PreviousCAS:%d PreviousRevSeqNo:%d}", m.CAS(), m.PreviousCAS(), m.PreviousRevSeqNo)
}

// CAS returns the CAS value as a uint64
func (m *MetadataOnlyUpdate) CAS() uint64 {
	return base.HexCasToUint64(m.HexCAS)
}

// PreviousCAS returns the previous CAS value as a uint64
func (m *MetadataOnlyUpdate) PreviousCAS() uint64 {
	return base.HexCasToUint64(m.PreviousHexCAS)
}

// The sync-gateway metadata stored in the "_sync" property of a Couchbase document.
type SyncData struct {
	CurrentRev          string               `json:"-"`                 // CurrentRev.  Persisted as RevAndVersion in SyncDataJSON
	NewestRev           string               `json:"new_rev,omitempty"` // Newest rev, if different from CurrentRev
	Flags               uint8                `json:"flags,omitempty"`
	Sequence            uint64               `json:"sequence,omitempty"`
	UnusedSequences     []uint64             `json:"unused_sequences,omitempty"` // unused sequences due to update conflicts/CAS retry
	RecentSequences     []uint64             `json:"recent_sequences,omitempty"` // recent sequences for this doc - used in server dedup handling
	Channels            channels.ChannelMap  `json:"channels,omitempty"`
	Access              UserAccessMap        `json:"access,omitempty"`
	RoleAccess          UserAccessMap        `json:"role_access,omitempty"`
	Expiry              *time.Time           `json:"exp,omitempty"`                     // Document expiry.  Information only - actual expiry/delete handling is done by bucket storage.  Needs to be pointer for omitempty to work (see https://github.com/golang/go/issues/4357)
	Cas                 string               `json:"cas"`                               // String representation of a cas value, populated via macro expansion
	Crc32c              string               `json:"value_crc32c"`                      // String representation of crc32c hash of doc body, populated via macro expansion
	Crc32cUserXattr     string               `json:"user_xattr_value_crc32c,omitempty"` // String representation of crc32c hash of user xattr
	TombstonedAt        int64                `json:"tombstoned_at,omitempty"`           // Time the document was tombstoned.  Used for view compaction
	AttachmentsPre4dot0 AttachmentsMeta      `json:"attachments,omitempty"`             // Location of _attachments metadata for pre-4.0 attachments, stored in _sync xattr or _sync inline. In 4.0 and later, attachments are in _globalSync._attachments_meta
	ChannelSet          []ChannelSetEntry    `json:"channel_set"`
	ChannelSetHistory   []ChannelSetEntry    `json:"channel_set_history"`
	HLV                 *HybridLogicalVector `json:"-"` // Marshalled/Unmarshalled separately from SyncData for storage in _vv, see MarshalWithXattrs/UnmarshalWithXattrs

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

// determine set of current channels based on removal entries.
func (sd *SyncData) getCurrentChannels() base.Set {
	ch := base.SetOf()
	for channelName, channelRemoval := range sd.Channels {
		if channelRemoval == nil || channelRemoval.Seq == 0 {
			ch.Add(channelName)
		}
	}
	return ch
}

func (sd *SyncData) HashRedact(salt string) SyncData {

	// Creating a new SyncData with the redacted info. We copy all the information which stays the same and create new
	// items for the redacted data. The data to be redacted is populated below.
	redactedSyncData := SyncData{
		CurrentRev:          sd.CurrentRev,
		NewestRev:           sd.NewestRev,
		Flags:               sd.Flags,
		Sequence:            sd.Sequence,
		UnusedSequences:     sd.UnusedSequences,
		RecentSequences:     sd.RecentSequences,
		History:             RevTree{},
		Channels:            channels.ChannelMap{},
		Access:              UserAccessMap{},
		RoleAccess:          UserAccessMap{},
		Expiry:              sd.Expiry,
		Cas:                 sd.Cas,
		Crc32c:              sd.Crc32c,
		TombstonedAt:        sd.TombstonedAt,
		AttachmentsPre4dot0: AttachmentsMeta{},
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
	for k, v := range sd.AttachmentsPre4dot0 {
		redactedSyncData.AttachmentsPre4dot0[base.Sha1HashString(k, salt)] = v
	}

	return redactedSyncData
}

// A document as stored in Couchbase. Contains the body of the current revision plus metadata.
// In its JSON form, the body's properties are at top-level while the SyncData is in a special
// "_sync" property.
// Document doesn't do any locking - document instances aren't intended to be shared across multiple goroutines.
type Document struct {
	SyncData                               // Sync metadata
	_globalSync        GlobalSyncData      // Global sync metadata, this will hold non cluster specific sync metadata to be copied by XDCR
	_body              Body                // Marshalled document body.  Unmarshalled lazily - should be accessed using Body()
	_rawBody           []byte              // Raw document body, as retrieved from the bucket.  Marshaled lazily - should be accessed using BodyBytes()
	ID                 string              `json:"-"` // Doc id.  (We're already using a custom MarshalJSON for *document that's based on body, so the json:"-" probably isn't needed here)
	Cas                uint64              // Document cas
	rawUserXattr       []byte              // Raw user xattr as retrieved from the bucket
	MetadataOnlyUpdate *MetadataOnlyUpdate // Contents of _mou xattr, marshalled/unmarshalled with document from xattrs

	Deleted        bool
	DocExpiry      uint32
	RevID          string
	inlineSyncData bool
	RevSeqNo       uint64 // Server rev seq no for a document
}

// GlobalSyncData is the structure for the system xattr that is migrated with XDCR.
type GlobalSyncData struct {
	Attachments AttachmentsMeta `json:"attachments_meta,omitempty"`
}

type historyOnlySyncData struct {
	revOnlySyncData
	History RevTree `json:"history"`
}

type revOnlySyncData struct {
	casOnlySyncData
	CurrentRev channels.RevAndVersion `json:"rev"`
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
		return base.InjectJSONProperties(doc._rawBody, base.KVPair{Key: base.SyncPropertyName, Val: doc.SyncData})
	} else {
		return base.JSONMarshal(doc)
	}
}

func (doc *Document) IsDeleted() bool {
	return doc.hasFlag(channels.Deleted)
}

func (doc *Document) BodyWithSpecialProperties(ctx context.Context) ([]byte, error) {
	bodyBytes, err := doc.BodyBytes(ctx)
	if err != nil {
		return nil, err
	}

	kvPairs := []base.KVPair{
		{Key: BodyId, Val: doc.ID},
		{Key: BodyRev, Val: doc.CurrentRev},
	}

	if doc.IsDeleted() {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyDeleted, Val: true})
	}

	if len(doc.Attachments()) > 0 {
		kvPairs = append(kvPairs, base.KVPair{Key: BodyAttachments, Val: doc.Attachments()})
	}

	bodyBytes, err = base.InjectJSONProperties(bodyBytes, kvPairs...)
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
func (doc *Document) Body(ctx context.Context) Body {
	var caller string
	if base.ConsoleLogLevel().Enabled(base.LevelTrace) {
		caller = base.GetCallersName(1, true)
	}

	if doc._body != nil {
		base.TracefCtx(ctx, base.KeyAll, "Already had doc body %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
		return doc._body
	}

	if doc._rawBody == nil {
		return nil
	}

	base.TracefCtx(ctx, base.KeyAll, "        UNMARSHAL doc body %s/%s from %s", base.UD(doc.ID), base.UD(doc.RevID), caller)
	err := doc._body.Unmarshal(doc._rawBody)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to unmarshal document body from raw body : %s", err)
		return nil
	}
	return doc._body
}

// Get a deep mutable copy of the body, using _rawBody.  Initializes _rawBody based on _body if not already present.
func (doc *Document) GetDeepMutableBody() (Body, error) {
	// If doc._rawBody isn't present, marshal from doc.Body
	if doc._rawBody == nil {
		if doc._body == nil {
			return nil, fmt.Errorf("Unable to get document body due to an empty raw body and body in the document")
		}
		var err error
		doc._rawBody, err = base.JSONMarshal(doc._body)
		if err != nil {
			return nil, fmt.Errorf("Unable to marshal document body into raw body : %w", err)
		}
	}

	var mutableBody Body
	err := mutableBody.Unmarshal(doc._rawBody)
	if err != nil {
		return nil, fmt.Errorf("Unable to unmarshal document body into raw body : %w", err)
	}

	return mutableBody, nil
}

func (doc *Document) RemoveBody() {
	doc._body = nil
	doc._rawBody = nil
}

// HasBody returns true if the given document has either an unmarshalled body, or raw bytes available.
func (doc *Document) HasBody() bool {
	return doc._body != nil || doc._rawBody != nil
}

func (doc *Document) BodyBytes(ctx context.Context) ([]byte, error) {
	if doc._rawBody != nil {
		return doc._rawBody, nil
	}

	if doc._body == nil {
		return nil, nil
	}

	bodyBytes, err := base.JSONMarshal(doc._body)
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "Error marshalling document body")
	}
	doc._rawBody = bodyBytes
	return doc._rawBody, nil
}

// Builds the Meta Map for use in the Sync Function. This meta map currently only includes the user xattr, however, this
// can be expanded upon in the future.
// NOTE: emptyMetaMap() is used within tests in channelmapper_test.go and therefore this should be expanded if the below is
func (doc *Document) GetMetaMap(userXattrKey string) (map[string]interface{}, error) {
	xattrsMap := map[string]interface{}{}

	if userXattrKey != "" {
		var userXattr interface{}

		if len(doc.rawUserXattr) > 0 {
			err := base.JSONUnmarshal(doc.rawUserXattr, &userXattr)
			if err != nil {
				return nil, err
			}
		}
		xattrsMap[userXattrKey] = userXattr
	}

	return map[string]interface{}{
		base.MetaMapXattrsKey: xattrsMap,
	}, nil
}

func (doc *Document) SetCrc32cUserXattrHash() {
	doc.SyncData.Crc32cUserXattr = userXattrCrc32cHash(doc.rawUserXattr)
}

func userXattrCrc32cHash(userXattr []byte) string {
	// If user xattr is empty then set hash to empty (feature has been disabled or doc doesn't have xattr)
	if len(userXattr) == 0 {
		return ""
	}
	return base.Crc32cHashString(userXattr)
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
		doc.SetAttachments(mergeAttachments(doc.SyncData.AttachmentsPre4dot0, doc.Attachments()))
		doc.SyncData.AttachmentsPre4dot0 = nil

	}
	return doc, nil
}

func unmarshalDocumentWithXattrs(ctx context.Context, docid string, data, syncXattrData, hlvXattrData, mouXattrData, userXattrData, revSeqNo []byte, globalSyncData []byte, cas uint64, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	if len(syncXattrData) == 0 && len(hlvXattrData) == 0 {
		// If no xattr data, unmarshal as standard doc
		doc, err = unmarshalDocument(docid, data)
		if doc != nil {
			doc.RevSeqNo, err = unmarshalRevSeqNo(revSeqNo)
			if err != nil {
				return nil, pkgerrors.WithStack(base.RedactErrorf("Failed convert rev seq number during UnmarshalWithXattrs() doc with id: %s. Error: %v", base.UD(doc.ID), err))
			}
		}
	} else {
		doc = NewDocument(docid)
		err = doc.UnmarshalWithXattrs(ctx, data, syncXattrData, hlvXattrData, revSeqNo, globalSyncData, unmarshalLevel)
	}
	if err != nil {
		return nil, err
	}

	if len(mouXattrData) > 0 {
		if err := base.JSONUnmarshal(mouXattrData, &doc.MetadataOnlyUpdate); err != nil {
			base.WarnfCtx(ctx, "Failed to unmarshal mouXattr for key %v, mou will be ignored. Err: %v mou:%s", base.UD(docid), err, mouXattrData)
		}
	}
	if len(userXattrData) > 0 {
		doc.rawUserXattr = userXattrData
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
		return nil, fmt.Errorf("Could not unmarshal _sync out of document body: %w", err)
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
func UnmarshalDocumentSyncDataFromFeed(data []byte, dataType uint8, userXattrKey string, needHistory bool) (result *SyncData, rawBody []byte, rawXattrs map[string][]byte, err error) {
	var body []byte

	// If xattr datatype flag is set, data includes both xattrs and document body.  Check for presence of sync xattr.
	// Note that there could be a non-sync xattr present
	var xattrValues map[string][]byte
	var hlv *HybridLogicalVector
	if dataType&base.MemcachedDataTypeXattr != 0 {
		xattrKeys := []string{base.SyncXattrName, base.MouXattrName, base.VvXattrName, base.GlobalXattrName}
		if userXattrKey != "" {
			xattrKeys = append(xattrKeys, userXattrKey)
		}
		body, xattrValues, err = sgbucket.DecodeValueWithXattrs(xattrKeys, data)
		if err != nil {
			return nil, nil, nil, err
		}

		// If the sync xattr is present, use that to build SyncData
		syncXattr, ok := xattrValues[base.SyncXattrName]

		if vvXattr, ok := xattrValues[base.VvXattrName]; ok {
			err = base.JSONUnmarshal(vvXattr, &hlv)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("error unmarshalling HLV: %w", err)
			}
		}

		if ok && len(syncXattr) > 0 {
			result = &SyncData{}
			if needHistory {
				result.History = make(RevTree)
			}
			err = base.JSONUnmarshal(syncXattr, result)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("Found _sync xattr (%q), but could not unmarshal: %w", string(syncXattr), err)
			}

			if hlv != nil {
				result.HLV = hlv
			}
			return result, body, xattrValues, nil
		}

	} else {
		// Xattr flag not set - data is just the document body
		body = data
	}

	// Non-xattr data, or sync xattr not present.  Attempt to retrieve sync metadata from document body
	if len(body) != 0 {
		result, err = UnmarshalDocumentSyncData(body, needHistory)
	}

	// If no sync data was found but HLV was present, initialize empty sync data
	if result == nil && hlv != nil {
		result = &SyncData{}
	}
	// If HLV was found, add to sync data
	if hlv != nil {
		result.HLV = hlv
	}
	return result, body, xattrValues, err
}

func UnmarshalDocumentFromFeed(ctx context.Context, docid string, cas uint64, data []byte, dataType uint8, userXattrKey string) (doc *Document, err error) {
	if dataType&base.MemcachedDataTypeXattr == 0 {
		return unmarshalDocument(docid, data)
	}
	xattrKeys := []string{base.SyncXattrName}
	if userXattrKey != "" {
		xattrKeys = append(xattrKeys, userXattrKey)
	}
	body, xattrs, err := sgbucket.DecodeValueWithXattrs(xattrKeys, data)
	if err != nil {
		return nil, err
	}
	return unmarshalDocumentWithXattrs(ctx, docid, body, xattrs[base.SyncXattrName], xattrs[base.VvXattrName], xattrs[base.MouXattrName], xattrs[userXattrKey], xattrs[base.VirtualXattrRevSeqNo], nil, cas, DocUnmarshalAll)
}

func (doc *SyncData) HasValidSyncData() bool {

	valid := doc != nil && doc.CurrentRev != "" && (doc.Sequence > 0)
	validHistory := doc != nil && len(doc.History) > 0 && doc.History[doc.CurrentRev] != nil
	return valid && validHistory
}

func (doc *SyncData) HasValidSyncDataForImport() bool {
	valid := doc != nil && doc.CurrentRev != "" && (doc.Sequence > 0)
	validHistory := doc != nil && len(doc.History) > 0 && doc.History[doc.CurrentRev] != nil
	syncValid := valid && validHistory
	if !syncValid && doc != nil && doc.HLV != nil {
		// If HLV is present, we can consider the sync data valid for import given we can have doc with _vv
		// but no sync data over the import feed
		return true
	}
	return syncValid
}

func (doc *SyncData) HasValidHLV() bool {
	return doc.HLV != nil && doc.HLV.SourceID != "" && doc.HLV.Version != 0
}

// validateSyncDataForImport validates for a non-empty sync data that the sync data is valid for import. If s=ofund to
// not be valid will return HTTP error 422 (Unprocessable Entity) and increment the import error count.
func (s *SyncData) validateSyncDataForImport(ctx context.Context, db *DatabaseContext, docID string) error {
	if !s.SyncIsEmpty() && !s.HasValidSyncData() {
		base.WarnfCtx(ctx, "Invalid sync data for doc %s - not importing.", base.UD(docID))
		db.DbStats.SharedBucketImportStats.ImportErrorCount.Add(1)
		return base.HTTPErrorf(http.StatusNotFound, "Not imported, invalid sync data found")
	}
	return nil
}

func (s *SyncData) SyncIsEmpty() bool {
	if s == nil {
		return true
	}
	isEmpty := s.CurrentRev == "" && len(s.History) == 0 && s.Sequence == 0
	return isEmpty
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

// doc.IsSGWrite - used during on-demand import.  Doesn't invoke SyncData.IsSGWrite so that we
// can complete the inexpensive cas check before the (potential) doc marshalling.
func (doc *Document) IsSGWrite(ctx context.Context, rawBody []byte) (isSGWrite bool, crc32Match bool, bodyChanged bool) {

	// If the raw body is available, use SyncData.IsSGWrite
	if rawBody != nil && len(rawBody) > 0 {

		isSgWriteFeed, crc32MatchFeed, bodyChangedFeed := doc.SyncData.IsSGWrite(doc.Cas, rawBody, doc.rawUserXattr)
		if !isSgWriteFeed {
			base.DebugfCtx(ctx, base.KeyCRUD, "Doc %s is not an SG write, based on cas and body hash. cas:%x syncCas:%q", base.UD(doc.ID), doc.Cas, doc.SyncData.Cas)
		}

		return isSgWriteFeed, crc32MatchFeed, bodyChangedFeed
	}

	// If raw body isn't available, first do the inexpensive cas check
	if doc.Cas == doc.SyncData.GetSyncCas() {
		return true, false, false
	}

	// Since raw body isn't available, marshal from the document to perform body hash comparison
	bodyBytes, err := doc.BodyBytes(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to marshal doc body during SG write check for doc %s. Error: %v", base.UD(doc.ID), err)
		return false, false, false
	}
	// The bodyBytes would be replaced with "{}" if the document is a "Delete" and it can’t be used for
	// CRC-32 checksum comparison to determine whether the document has already been imported. So the value
	// currentBodyCrc32c needs to be revised to "0x00".
	currentBodyCrc32c := base.Crc32cHashString(bodyBytes)
	if doc.Deleted {
		currentBodyCrc32c = base.DeleteCrc32c // revert back to the correct crc32c before we replace bodyBytes
	}

	// If the current body crc32c matches the one in doc.SyncData, this was an SG write (i.e. has already been imported)
	if currentBodyCrc32c != doc.SyncData.Crc32c {
		base.DebugfCtx(ctx, base.KeyCRUD, "Doc %s is not an SG write, based on cas and body hash. cas:%x syncCas:%q", base.UD(doc.ID), doc.Cas, doc.SyncData.Cas)
		return false, false, true
	}

	if HasUserXattrChanged(doc.rawUserXattr, doc.Crc32cUserXattr) {
		base.DebugfCtx(ctx, base.KeyCRUD, "Doc %s is not an SG write, based on user xattr hash", base.UD(doc.ID))
		return false, false, false
	}

	return true, true, false
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

// RevLoaderFunc and RevWriterFunc manage persistence of non-winning revision bodies that are stored outside the document.
type RevLoaderFunc func(key string) ([]byte, error)

// RevisionBodyLoader retrieves a non-winning revision body stored outside the document metadata
func (c *DatabaseCollection) RevisionBodyLoader(key string) ([]byte, error) {
	body, _, err := c.dataStore.GetRaw(key)
	return body, err
}

// Retrieves a non-winning revision body.  If not already loaded in the document (either because inline,
// or was previously requested), loader function is used to retrieve from the bucket.
func (doc *Document) getNonWinningRevisionBody(ctx context.Context, revid string, loader RevLoaderFunc) Body {
	var body Body
	bodyBytes, found := doc.History.getRevisionBody(revid, loader)
	if !found || len(bodyBytes) == 0 {
		return nil
	}

	if err := body.Unmarshal(bodyBytes); err != nil {
		base.WarnfCtx(ctx, "Unexpected error parsing body of rev %q: %v", revid, err)
		return nil
	}
	return body
}

// Fetches the body of a revision as JSON, or nil if it's not available.
func (doc *Document) getRevisionBodyJSON(ctx context.Context, revid string, loader RevLoaderFunc) []byte {
	var bodyJSON []byte
	if revid == doc.CurrentRev {
		var marshalErr error
		bodyJSON, marshalErr = doc.BodyBytes(ctx)
		if marshalErr != nil {
			base.WarnfCtx(ctx, "Marshal error when retrieving active current revision body: %v", marshalErr)
		}
	} else {
		bodyJSON, _ = doc.History.getRevisionBody(revid, loader)
	}
	return bodyJSON
}

func (doc *Document) removeRevisionBody(ctx context.Context, revID string) {
	removedBodyKey := doc.History.removeRevisionBody(ctx, revID)
	if removedBodyKey != "" {
		if doc.removedRevisionBodyKeys == nil {
			doc.removedRevisionBodyKeys = make(map[string]string)
		}
		doc.removedRevisionBodyKeys[revID] = removedBodyKey
	}
}

// makeBodyActive moves a previously non-winning revision body from the rev tree to the document body
func (doc *Document) promoteNonWinningRevisionBody(ctx context.Context, revid string, loader RevLoaderFunc) {
	// If the new revision is not current, transfer the current revision's
	// body to the top level doc._body:
	doc.UpdateBody(doc.getNonWinningRevisionBody(ctx, revid, loader))
	doc.removeRevisionBody(ctx, revid)
}

func (doc *Document) pruneRevisions(ctx context.Context, maxDepth uint32, keepRev string) int {
	numPruned, prunedTombstoneBodyKeys := doc.History.pruneRevisions(ctx, maxDepth, keepRev)
	for revID, bodyKey := range prunedTombstoneBodyKeys {
		if doc.removedRevisionBodyKeys == nil {
			doc.removedRevisionBodyKeys = make(map[string]string)
		}
		doc.removedRevisionBodyKeys[revID] = bodyKey
	}
	return numPruned
}

// Adds a revision body (as Body) to a document.  Removes special properties first.
func (doc *Document) setRevisionBody(ctx context.Context, revid string, newDoc *Document, storeInline, hasAttachments bool) {
	if revid == doc.CurrentRev {
		doc._body = newDoc._body
		doc._rawBody = newDoc._rawBody
	} else {
		bodyBytes, _ := newDoc.BodyBytes(ctx)
		doc.setNonWinningRevisionBody(revid, bodyBytes, storeInline, hasAttachments)
	}
}

// Adds a revision body (as []byte) to a document.  Flags for external storage when appropriate
func (doc *Document) setNonWinningRevisionBody(revid string, body []byte, storeInline bool, hasAttachments bool) {
	revBodyKey := ""
	if !storeInline && len(body) > MaximumInlineBodySize {
		revBodyKey = generateRevBodyKey(doc.ID, revid)
		doc.addedRevisionBodies = append(doc.addedRevisionBodies, revid)
	}
	doc.History.setRevisionBody(revid, body, revBodyKey, hasAttachments)
}

// persistModifiedRevisionBodies writes new non-inline revisions to the bucket.
// Should be invoked BEFORE the document is successfully committed.
func (doc *Document) persistModifiedRevisionBodies(datastore sgbucket.DataStore) error {

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
		addErr := doc.persistRevisionBody(datastore, revInfo.BodyKey, revInfo.Body)
		if addErr != nil {
			return err
		}
	}

	doc.addedRevisionBodies = []string{}
	return nil
}

// deleteRemovedRevisionBodies deletes obsolete non-inline revisions from the bucket.
// Should be invoked AFTER the document is successfully committed.
func (doc *Document) deleteRemovedRevisionBodies(ctx context.Context, dataStore base.DataStore) {

	for _, revBodyKey := range doc.removedRevisionBodyKeys {
		deleteErr := dataStore.Delete(revBodyKey)
		if deleteErr != nil {
			base.WarnfCtx(ctx, "Unable to delete old revision body using key %s - will not be deleted from bucket.", revBodyKey)
		}
	}
	doc.removedRevisionBodyKeys = map[string]string{}
}

func (doc *Document) persistRevisionBody(datastore sgbucket.DataStore, key string, body []byte) error {
	_, err := datastore.AddRaw(key, 0, body)
	return err
}

// Move any large revision bodies to external document storage
func (doc *Document) migrateRevisionBodies(ctx context.Context, dataStore base.DataStore) error {

	for _, revID := range doc.History.GetLeaves() {
		revInfo, err := doc.History.getInfo(revID)
		if err != nil {
			continue
		}
		if len(revInfo.Body) > MaximumInlineBodySize {
			bodyKey := generateRevBodyKey(doc.ID, revID)
			persistErr := doc.persistRevisionBody(dataStore, bodyKey, revInfo.Body)
			if persistErr != nil {
				base.WarnfCtx(ctx, "Unable to store revision body for doc %s, rev %s externally: %v", base.UD(doc.ID), revID, persistErr)
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

// ////// CHANNELS & ACCESS:

func (doc *Document) updateChannelHistory(channelName string, seq uint64, addition bool) {
	// Check if we already have an entry for this channel
	for idx, historyEntry := range doc.ChannelSet {
		if historyEntry.Name == channelName {
			// If we are here there is an existing entry for this channel
			// If addition we need to:
			// - Move existing history entry to old channels
			// - Remove existing entry from current channels
			// - Add new entry with start seq
			// If removal / not addition then we can simply add the end seq
			if addition {
				// If there is no end for the current entry we're in an unexpected state as you can't have an addition
				// of the same channel twice without a removal in between. If we're somehow in this state we don't know
				// when the removal happened so best we can do is skip this addition work and keep the existing start.
				if doc.ChannelSet[idx].End == 0 {
					return
				}
				doc.addToChannelSetHistory(channelName, historyEntry)
				doc.ChannelSet[idx] = ChannelSetEntry{Name: channelName, Start: seq}
			} else {
				doc.ChannelSet[idx].End = seq
			}
			return
		}
	}

	// If we get to this point there is no existing entry
	// If addition we can just simply add it
	// If its a removal / not addition then its a legacy document. Start seq is 1 because we don't know when legacy
	// channels were added
	if addition {
		doc.ChannelSet = append(doc.ChannelSet, ChannelSetEntry{
			Name:  channelName,
			Start: seq,
		})
	} else {
		doc.ChannelSet = append(doc.ChannelSet, ChannelSetEntry{
			Name:  channelName,
			Start: 1,
			End:   seq,
		})
	}

}

func (doc *Document) addToChannelSetHistory(channelName string, historyEntry ChannelSetEntry) {
	// Before adding the entry we need to verify the number of items in the document channel history to check
	// whether we need to prune them.
	// As we iterate over the existing channels we will keep track of the oldest and second oldest entries along with
	// their location in the slice. If we need to then prune we can 'merge' the oldest and second oldest and remove
	// the oldest.

	var oldestEntryStartSeq uint64 = math.MaxUint64
	var secondOldestEntryStartSeq uint64 = math.MaxUint64

	oldestEntryIdx := -1
	secondOldestEntryIdx := -1

	entryCount := 0

	for entryIdx, entry := range doc.ChannelSetHistory {
		if entry.Name == channelName {
			entryCount++

			if entry.Start < oldestEntryStartSeq {
				secondOldestEntryStartSeq = oldestEntryStartSeq
				oldestEntryStartSeq = entry.Start

				secondOldestEntryIdx = oldestEntryIdx
				oldestEntryIdx = entryIdx
				continue
			}

			if entry.Start < secondOldestEntryStartSeq {
				secondOldestEntryStartSeq = entry.Start
				secondOldestEntryIdx = entryIdx
			}
		}
	}

	if entryCount >= DocumentHistoryMaxEntriesPerChannel {
		doc.ChannelSetHistory[secondOldestEntryIdx].Start = oldestEntryStartSeq
		doc.ChannelSetHistory[secondOldestEntryIdx].Compacted = true
		doc.ChannelSetHistory = append(doc.ChannelSetHistory[:oldestEntryIdx], doc.ChannelSetHistory[oldestEntryIdx+1:]...)
	}

	doc.ChannelSetHistory = append(doc.ChannelSetHistory, historyEntry)
}

// Updates the Channels property of a document object with current & past channels.
// Returns the set of channels that have changed (document joined or left in this revision)
func (doc *Document) updateChannels(ctx context.Context, newChannels base.Set) (changedChannels base.Set, revokedChannelsRequiringExpansion []string, err error) {
	var changed []string
	oldChannels := doc.Channels
	if oldChannels == nil {
		oldChannels = channels.ChannelMap{}
		doc.Channels = oldChannels
	} else {
		// Mark every no-longer-current channel as unsubscribed:
		curSequence := doc.Sequence
		curRevAndVersion := doc.GetRevAndVersion()
		for channel, removal := range oldChannels {
			if removal == nil && !newChannels.Contains(channel) {
				oldChannels[channel] = &channels.ChannelRemoval{
					Seq:     curSequence,
					Rev:     curRevAndVersion,
					Deleted: doc.hasFlag(channels.Deleted)}
				doc.updateChannelHistory(channel, curSequence, false)
				changed = append(changed, channel)
				// If the current version requires macro expansion, new removal in channel map will also require macro expansion
				if doc.HLV != nil && doc.HLV.Version == expandMacroCASValueUint64 {
					revokedChannelsRequiringExpansion = append(revokedChannelsRequiringExpansion, channel)
				}
			}
		}
	}

	// Mark every current channel as subscribed:
	for channel := range newChannels {
		if value, exists := oldChannels[channel]; value != nil || !exists {
			oldChannels[channel] = nil
			changed = append(changed, channel)
			doc.updateChannelHistory(channel, doc.Sequence, true)
		}
	}
	if changed != nil {
		base.InfofCtx(ctx, base.KeyCRUD, "\tDoc %q / %q in channels %q", base.UD(doc.ID), doc.CurrentRev, base.UD(newChannels))
		changedChannels, err = channels.SetFromArray(changed, channels.KeepStar)
	}
	return
}

// Determine whether the specified revision was a channel removal, based on doc.Channels.  If so, construct the standard document body for a
// removal notification (_removed=true)
// Set of channels returned from IsChannelRemoval are "Active" channels and NOT "Removed".
func (doc *Document) IsChannelRemoval(ctx context.Context, revID string) (bodyBytes []byte, history Revisions, channels base.Set, isRemoval bool, isDelete bool, err error) {

	removedChannels := make(base.Set)

	// Iterate over the document's channel history, looking for channels that were removed at revID.  If found, also identify whether the removal was a tombstone.
	for channel, removal := range doc.Channels {
		if removal != nil && removal.Rev.RevTreeID == revID {
			removedChannels[channel] = struct{}{}
			if removal.Deleted == true {
				isDelete = true
			}
		}
	}
	// If no matches found, return isRemoval=false
	if len(removedChannels) == 0 {
		return nil, nil, nil, false, false, nil
	}

	// Construct removal body
	// doc ID and rev ID aren't required to be inserted here, as both of those are available in the request.
	bodyBytes = []byte(RemovedRedactedDocument)

	activeChannels := make(base.Set)
	// Add active channels to the channel set if the the revision is available in the revision tree.
	if revInfo, ok := doc.History[revID]; ok {
		for channel, _ := range revInfo.Channels {
			activeChannels[channel] = struct{}{}
		}
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
	history = encodeRevisions(ctx, doc.ID, revHistory)

	return bodyBytes, history, activeChannels, true, isDelete, nil
}

// Updates a document's channel/role UserAccessMap with new access settings from an AccessMap.
// Returns an array of the user/role names whose access has changed as a result.
func (accessMap *UserAccessMap) updateAccess(ctx context.Context, doc *Document, newAccess channels.AccessMap) (changedUsers []string) {
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
		base.InfofCtx(ctx, base.KeyAccess, "Doc %q grants %s access: %v", base.UD(doc.ID), what, base.UD(*accessMap))
	}
	return changedUsers
}

// ////// MARSHALING ////////

type documentRoot struct {
	SyncData *SyncData `json:"_sync"`
}

func (doc *Document) UnmarshalJSON(data []byte) error {
	if doc.ID == "" {
		// TODO: CBG-1948
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
	delete(doc._body, base.SyncPropertyName)

	return nil
}

func (doc *Document) MarshalJSON() (data []byte, err error) {
	// The calling code in ImportDoc will marshal the document since it takes a body with _sync inline.
	// Make sure that _sync._attachments is present in the document body since _globalSync._attachments_meta
	// can not be seen in an inline body.
	doc.SyncData.AttachmentsPre4dot0 = doc.Attachments()
	defer func() { doc.SyncData.AttachmentsPre4dot0 = nil }()
	if doc._rawBody != nil {
		data, err = base.InjectJSONProperties(doc._rawBody, base.KVPair{
			Key: base.SyncPropertyName,
			Val: doc.SyncData,
		})
	} else {
		body := doc._body
		if body == nil {
			body = Body{}
		}
		body[base.SyncPropertyName] = &doc.SyncData
		data, err = base.JSONMarshal(body)
		delete(body, base.SyncPropertyName)
		if err != nil {
			err = pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalJSON() doc with id: %s.  Error: %v", base.UD(doc.ID), err))
		}
	}
	return data, err
}

// UnmarshalWithXattrs unmarshals the provided raw document and xattr bytes when present.  The provided DocumentUnmarshalLevel
// (unmarshalLevel) specifies how much of the provided document/xattr needs to be initially unmarshalled.  If
// unmarshalLevel is anything less than the full document + metadata, the raw data is retained for subsequent
// lazy unmarshalling as needed.
// Must handle cases where document body and hlvXattrData are present without syncXattrData for all DocumentUnmarshalLevel
func (doc *Document) UnmarshalWithXattrs(ctx context.Context, data, syncXattrData, hlvXattrData, revSeqNo []byte, globalSyncData []byte, unmarshalLevel DocumentUnmarshalLevel) error {
	if doc.ID == "" {
		base.WarnfCtx(ctx, "Attempted to unmarshal document without ID set")
		return errors.New("Document was unmarshalled without ID set")
	}

	switch unmarshalLevel {
	case DocUnmarshalAll, DocUnmarshalSync:
		// Unmarshal full document and/or sync metadata. Documents written by XDCR may have HLV but no sync data
		doc.SyncData = SyncData{History: make(RevTree)}
		if syncXattrData != nil {
			unmarshalErr := base.JSONUnmarshal(syncXattrData, &doc.SyncData)
			if unmarshalErr != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattrs() doc with id: %s (DocUnmarshalAll/Sync).  Error: %v", base.UD(doc.ID), unmarshalErr))
			}
		}
		if hlvXattrData != nil {
			// parse the raw bytes of the hlv and convert deltas back to full values in memory
			err := base.JSONUnmarshal(hlvXattrData, &doc.HLV)
			if err != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to unmarshal HLV during UnmarshalWithXattrs() doc with id: %s (DocUnmarshalAll/Sync).  Error: %v", base.UD(doc.ID), err))
			}
		}
		if revSeqNo != nil {
			var err error
			doc.RevSeqNo, err = unmarshalRevSeqNo(revSeqNo)
			if err != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to unmarshal RevSeqNo during UnmarshalWithXattrs() doc with id: %s (DocUnmarshalAll/Sync).  Error: %v", base.UD(doc.ID), err))
			}
		}
		if len(globalSyncData) > 0 {
			if err := base.JSONUnmarshal(globalSyncData, &doc._globalSync); err != nil {
				base.WarnfCtx(ctx, "Failed to unmarshal globalSync xattr for key %v, globalSync will be ignored. Err: %v globalSync:%s", base.UD(doc.ID), err, globalSyncData)
			}
		}
		doc.SetAttachments(mergeAttachments(doc.SyncData.AttachmentsPre4dot0, doc.Attachments()))
		doc.SyncData.AttachmentsPre4dot0 = nil
		doc._rawBody = data
		// Unmarshal body if requested and present
		if unmarshalLevel == DocUnmarshalAll && len(data) > 0 {
			return doc._body.Unmarshal(data)
		}
	case DocUnmarshalNoHistory:
		// Unmarshal sync metadata only, excluding history
		doc.SyncData = SyncData{}
		if syncXattrData != nil {
			unmarshalErr := base.JSONUnmarshal(syncXattrData, &doc.SyncData)
			if unmarshalErr != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattrs() doc with id: %s (DocUnmarshalNoHistory).  Error: %v", base.UD(doc.ID), unmarshalErr))
			}
		}
		if hlvXattrData != nil {
			// parse the raw bytes of the hlv and convert deltas back to full values in memory
			err := base.JSONUnmarshal(hlvXattrData, &doc.HLV)
			if err != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to unmarshal HLV during UnmarshalWithXattrs() doc with id: %s (DocUnmarshalNoHistory).  Error: %v", base.UD(doc.ID), err))
			}
		}
		if len(globalSyncData) > 0 {
			if err := base.JSONUnmarshal(globalSyncData, &doc._globalSync); err != nil {
				base.WarnfCtx(ctx, "Failed to unmarshal globalSync xattr for key %v, globalSync will be ignored. Err: %v globalSync:%s", base.UD(doc.ID), err, globalSyncData)
			}
		}
		doc.SetAttachments(mergeAttachments(doc.SyncData.AttachmentsPre4dot0, doc.Attachments()))
		doc.SyncData.AttachmentsPre4dot0 = nil
		doc._rawBody = data
	case DocUnmarshalHistory:
		if syncXattrData != nil {
			historyOnlyMeta := historyOnlySyncData{History: make(RevTree)}
			unmarshalErr := base.JSONUnmarshal(syncXattrData, &historyOnlyMeta)
			if unmarshalErr != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattrs() doc with id: %s (DocUnmarshalHistory).  Error: %v", base.UD(doc.ID), unmarshalErr))
			}
			doc.SyncData = SyncData{
				CurrentRev: historyOnlyMeta.CurrentRev.RevTreeID,
				History:    historyOnlyMeta.History,
				Cas:        historyOnlyMeta.Cas,
			}
		} else {
			doc.SyncData = SyncData{}
		}
		doc._rawBody = data
	case DocUnmarshalRev:
		// Unmarshal only rev and cas from sync metadata
		if syncXattrData != nil {
			var revOnlyMeta revOnlySyncData
			unmarshalErr := base.JSONUnmarshal(syncXattrData, &revOnlyMeta)
			if unmarshalErr != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattrs() doc with id: %s (DocUnmarshalRev).  Error: %v", base.UD(doc.ID), unmarshalErr))
			}
			doc.SyncData = SyncData{
				CurrentRev: revOnlyMeta.CurrentRev.RevTreeID,
				Cas:        revOnlyMeta.Cas,
			}
		} else {
			doc.SyncData = SyncData{}
		}
		doc._rawBody = data
	case DocUnmarshalCAS:
		// Unmarshal only cas from sync metadata
		if syncXattrData != nil {
			var casOnlyMeta casOnlySyncData
			unmarshalErr := base.JSONUnmarshal(syncXattrData, &casOnlyMeta)
			if unmarshalErr != nil {
				return pkgerrors.WithStack(base.RedactErrorf("Failed to UnmarshalWithXattrs() doc with id: %s (DocUnmarshalCAS).  Error: %v", base.UD(doc.ID), unmarshalErr))
			}
			doc.SyncData = SyncData{
				Cas: casOnlyMeta.Cas,
			}
		} else {
			doc.SyncData = SyncData{}
		}
		doc._rawBody = data
	}

	// If there's no body, but there is an xattr, set deleted flag and initialize an empty body
	if len(data) == 0 && len(syncXattrData) > 0 {
		doc._body = Body{}
		doc._rawBody = []byte(base.EmptyDocument)
		doc.Deleted = true
	}
	return nil
}

// MarshalWithXattrs marshals the Document into body, and sync, vv and mou xattrs for persistence.
func (doc *Document) MarshalWithXattrs() (data, syncXattr, vvXattr, mouXattr, globalXattr []byte, err error) {
	// Grab the rawBody if it's already marshalled, otherwise unmarshal the body
	if doc._rawBody != nil {
		if !doc.IsDeleted() {
			data = doc._rawBody
		}
	} else {
		body := doc._body
		// If body is non-empty and non-deleted, unmarshal and return
		if body != nil {
			// TODO: Could we check doc.Deleted?
			//       apparently we can't... doing this causes invalid argument errors from Couchbase Server
			//       when running 'TestGetRemovedAndDeleted' and 'TestNoConflictsMode' for some reason...
			deleted, _ := body[BodyDeleted].(bool)
			if !deleted {
				data, err = base.JSONMarshal(body)
				if err != nil {
					return nil, nil, nil, nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattrs() doc body with id: %s.  Error: %v", base.UD(doc.ID), err))
				}
			}
		}
	}
	if doc.SyncData.HLV != nil {
		vvXattr, err = base.JSONMarshal(doc.SyncData.HLV)
		if err != nil {
			return nil, nil, nil, nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattrs() doc vv with id: %s.  Error: %v", base.UD(doc.ID), err))
		}
	}
	syncXattr, err = base.JSONMarshal(doc.SyncData)
	if err != nil {
		return nil, nil, nil, nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattrs() doc SyncData with id: %s.  Error: %v", base.UD(doc.ID), err))
	}

	if doc.MetadataOnlyUpdate != nil {
		mouXattr, err = base.JSONMarshal(doc.MetadataOnlyUpdate)
		if err != nil {
			return nil, nil, nil, nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattrs() doc MouData with id: %s.  Error: %v", base.UD(doc.ID), err))
		}
	}
	// marshal global xattrs if there are attachments defined
	if len(doc.Attachments()) > 0 {
		globalXattr, err = base.JSONMarshal(doc._globalSync)
		if err != nil {
			return nil, nil, nil, nil, nil, pkgerrors.WithStack(base.RedactErrorf("Failed to MarshalWithXattrs() doc GlobalXattr with id: %s.  Error: %v", base.UD(doc.ID), err))
		}
	}

	return data, syncXattr, vvXattr, mouXattr, globalXattr, nil
}

// computeMetadataOnlyUpdate computes a new metadataOnlyUpdate based on the existing document's CAS and metadataOnlyUpdate
func computeMetadataOnlyUpdate(currentCas uint64, revNo uint64, currentMou *MetadataOnlyUpdate) *MetadataOnlyUpdate {
	var prevCas string
	currentCasString := base.CasToString(currentCas)
	if currentMou != nil && currentCasString == currentMou.HexCAS {
		prevCas = currentMou.PreviousHexCAS
	} else {
		prevCas = currentCasString
	}

	metadataOnlyUpdate := &MetadataOnlyUpdate{
		HexCAS:           expandMacroCASValueString, // when non-empty, this is replaced with cas macro expansion
		PreviousHexCAS:   prevCas,
		PreviousRevSeqNo: revNo,
	}
	return metadataOnlyUpdate
}

// HasCurrentVersion Compares the specified CV with the fetched documents CV, returns error on mismatch between the two
func (d *Document) HasCurrentVersion(ctx context.Context, cv Version) error {
	if d.HLV == nil {
		return base.RedactErrorf("no HLV present in fetched doc %s", base.UD(d.ID))
	}

	// fetch the current version for the loaded doc and compare against the CV specified in the IDandCV key
	fetchedDocSource, fetchedDocVersion := d.HLV.GetCurrentVersion()
	if fetchedDocSource != cv.SourceID || fetchedDocVersion != cv.Value {
		base.DebugfCtx(ctx, base.KeyCRUD, "mismatch between specified current version and fetched document current version for doc %s", base.UD(d.ID))
		// return not found as specified cv does not match fetched doc cv
		return base.ErrNotFound
	}
	return nil
}

// SetAttachments updates the attachments metadata for the document.
func (d *Document) SetAttachments(attachments AttachmentsMeta) {
	d._globalSync.Attachments = attachments
}

// Attachments returns the attachments for the document.
func (d *Document) Attachments() AttachmentsMeta {
	return d._globalSync.Attachments
}

// SyncDataAlias is an alias for SyncData that doesn't define custom MarshalJSON/UnmarshalJSON
type SyncDataAlias SyncData

// SyncDataJSON is the persisted form of SyncData, with RevAndVersion populated at marshal time
type SyncDataJSON struct {
	*SyncDataAlias
	RevAndVersion channels.RevAndVersion `json:"rev"`
}

// MarshalJSON populates RevAndVersion using CurrentRev and the HLV (current) source and version.
// Marshals using SyncDataAlias to avoid recursion, and SyncDataJSON to add the combined RevAndVersion.
func (s SyncData) MarshalJSON() (data []byte, err error) {

	var sdj SyncDataJSON
	var sd SyncDataAlias
	sd = (SyncDataAlias)(s)
	sdj.SyncDataAlias = &sd
	sdj.RevAndVersion = s.GetRevAndVersion()
	return base.JSONMarshal(sdj)
}

// UnmarshalJSON unmarshals using SyncDataJSON, then sets currentRev on SyncData based on the value in RevAndVersion.
// The HLV's current version stored in RevAndVersion is ignored at unmarshal time - the value in the HLV is the source
// of truth.
func (s *SyncData) UnmarshalJSON(data []byte) error {

	var sdj *SyncDataJSON
	err := base.JSONUnmarshal(data, &sdj)
	if err != nil {
		return err
	}
	if sdj.SyncDataAlias != nil {
		*s = SyncData(*sdj.SyncDataAlias)
		s.CurrentRev = sdj.RevAndVersion.RevTreeID
	}
	return nil
}

func (s *SyncData) GetRevAndVersion() (rav channels.RevAndVersion) {
	rav.RevTreeID = s.CurrentRev
	if s.HLV != nil {
		rav.CurrentSource = s.HLV.SourceID
		rav.CurrentVersion = string(base.Uint64CASToLittleEndianHex(s.HLV.Version))
	}
	return rav
}

// unmarshalRevSeqNo unmarshals the rev seq number from the provided bytes, expects a string representation of the uint64.
func unmarshalRevSeqNo(revSeqNoBytes []byte) (uint64, error) {
	if len(revSeqNoBytes) == 0 {
		return 0, nil
	}
	var revSeqNoString string
	err := base.JSONUnmarshal(revSeqNoBytes, &revSeqNoString)
	if err != nil {
		return 0, fmt.Errorf("Failed to unmarshal rev seq number %s", revSeqNoBytes)
	}
	revSeqNo, err := strconv.ParseUint(revSeqNoString, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Failed convert rev seq number %s", revSeqNoBytes)
	}
	return revSeqNo, nil
}
