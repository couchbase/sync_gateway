//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

import (
	"crypto/sha1"
	"encoding/base64"
)

// Key for retrieving an attachment from Couchbase.
type AttachmentKey string

// Structure of the _attachments dictionary in a document. (Maps attachment name to metadata dict.)
type Attachments map[string]Body

// Given a CouchDB document body about to be stored in the database, goes through the _attachments
// dict, finds attachments with inline bodies, copies the bodies into the Couchbase db, and replaces
// the bodies with the 'digest' attributes which are the keys to retrieving them.
func (db *Database) storeAttachments(doc *document, body Body, generation int, parentRev string) error {
	rawAtts, ok := body["_attachments"]
	if !ok {
		return nil
	}
	atts := rawAtts.(map[string]interface{})
	var parentAttachments map[string]interface{}
	for name, value := range atts {
		meta := value.(map[string]interface{})
		data, exists := meta["data"]
		if exists {
			// Attachment contains data, so store it in the db:
			attachment, err := decodeAttachment(data)
			if err != nil {
				return err
			}
			key, err := db.setAttachment(attachment)
			if err != nil {
				return err
			}
			delete(meta, "data")
			meta["stub"] = true
			meta["length"] = len(attachment)
			meta["digest"] = string(key)
			meta["revpos"] = generation
		} else {
			// No data given; look it up from the parent revision.
			if parentAttachments == nil {
				parent, err := db.getAvailableRev(doc, parentRev)
				if err != nil {
					return err
				}
				parentAttachments, exists = parent["_attachments"].(map[string]interface{})
				if !exists {
					return &HTTPError{400, "Unknown attachment " + name}
				}
			}
			parentAttachment := parentAttachments[name]
			if parentAttachment == nil {
				return &HTTPError{400, "Unknown attachment " + name}
			}
			atts[name] = parentAttachment
		}
	}
	return nil
}

// Goes through a revisions '_attachments' map, loads attachments (by their 'digest' properties)
// and adds 'data' properties containing the data. The data is added as raw []byte; the JSON
// marshaler will convert that to base64.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are loaded.
func (db *Database) loadBodyAttachments(body Body, minRevpos int) error {
	atts := body["_attachments"]
	if atts == nil {
		return nil
	}
	for _, value := range atts.(map[string]interface{}) {
		meta := value.(map[string]interface{})
		revpos := int(meta["revpos"].(float64))
		if revpos >= minRevpos {
			key := AttachmentKey(meta["digest"].(string))
			data, err := db.getAttachment(key)
			if err != nil {
				return err
			}
			meta["data"] = data
			delete(meta, "stub")
		}
	}
	return nil
}

// Retrieves an attachment, base64-encoded, given its key.
func (db *Database) getAttachment(key AttachmentKey) ([]byte, error) {
	return db.bucket.GetRaw(attachmentKeyToString(key))
}

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *Database) setAttachment(attachment []byte) (AttachmentKey, error) {
	digester := sha1.New()
	digester.Write(attachment)
	digest := base64.StdEncoding.EncodeToString(digester.Sum(nil))

	key := AttachmentKey("sha1-" + digest)
	_, err := db.bucket.AddRaw(attachmentKeyToString(key), 0, attachment)
	return key, err
}

//////// HELPERS:

func attachmentKeyToString(key AttachmentKey) string {
	return "att:" + string(key)
}

func decodeAttachment(att interface{}) ([]byte, error) {
	switch att := att.(type) {
		case string:
			return base64.StdEncoding.DecodeString(att)
		case []byte:
			return att, nil
	}
	return nil, &HTTPError{400, "invalid attachment data"}
}