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
	"encoding/json"
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
	Expiry          *time.Time          `json:"exp,omitempty"` // Document expiry.  Information only - actual expiry/delete handling is done by bucket storage.  Needs to be pointer for omitempty to work (see https://github.com/golang/go/issues/4357)

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

func (doc *syncData) HasValidSyncData(requireSequence bool) bool {
	valid := doc != nil && doc.CurrentRev != "" && (doc.Sequence > 0 || !requireSequence)
	// Additional diagnostics if sync metadata exists but isn't valid
	if !valid && doc != nil {
		base.LogTo("CRUD+", "Invalid sync metadata (may be expected):  Current rev: %s, Sequence: %v", doc.CurrentRev, doc.Sequence)
	}
	return valid
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

	err = json.Unmarshal([]byte(data), &doc.body)
	if err != nil {
		base.Warn("Error unmarshaling body of doc %q: %s", doc.ID, err)
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
