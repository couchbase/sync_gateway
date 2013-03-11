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
	
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

type ChannelRemoval struct {
	Seq uint64 `json:"seq"`
	Rev string `json:"rev"`
}
type ChannelMap map[string]*ChannelRemoval

// The sync-gateway metadata stored in the "_sync" property of a Couchbase document.
type syncData struct {
	CurrentRev string             `json:"rev"`
	Deleted    bool               `json:"deleted,omitempty"`
	Sequence   uint64             `json:"sequence"`
	History    RevTree            `json:"history"`
	Channels   ChannelMap         `json:"channels,omitempty"`
	Access     channels.AccessMap `json:"access,omitempty"`
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
	}
	return doc, nil
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
	body["_id"] = doc.ID
	body["_rev"] = revid
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
