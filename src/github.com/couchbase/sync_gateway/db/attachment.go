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
	"crypto/sha1"
	"encoding/base64"

	"github.com/couchbase/sync_gateway/base"
)

// Key for retrieving an attachment from Couchbase.
// In practice it's "sha1-" followed by a hex SHA-1 digest.
type AttachmentKey string

// Given a CouchDB document body about to be stored in the database, goes through the _attachments
// dict, finds attachments with inline bodies, copies the bodies into the Couchbase db, and replaces
// the bodies with the 'digest' attributes which are the keys to retrieving them.
func (db *Database) storeAttachments(doc *document, body Body, generation int, parentRev string) error {
	var parentAttachments map[string]interface{}
	atts := BodyAttachments(body)
	if atts == nil && body["_attachments"] != nil {
		return base.HTTPErrorf(400, "Invalid _attachments")
	}
	for name, value := range atts {
		meta, ok := value.(map[string]interface{})
		if !ok {
			return base.HTTPErrorf(400, "Invalid _attachments")
		}
		data, exists := meta["data"]
		if exists {
			// Attachment contains data, so store it in the db:
			attachment, err := DecodeAttachment(data)
			if err != nil {
				return err
			}
			key, err := db.setAttachment(attachment)
			if err != nil {
				return err
			}

			newMeta := map[string]interface{}{
				"stub":   true,
				"digest": string(key),
				"revpos": generation,
			}
			if contentType, ok := meta["content_type"].(string); ok {
				newMeta["content_type"] = contentType
			}
			if encoding := meta["encoding"]; encoding != nil {
				newMeta["encoding"] = encoding
				newMeta["encoded_length"] = len(attachment)
				if length, ok := meta["length"].(float64); ok {
					newMeta["length"] = length
				}
			} else {
				newMeta["length"] = len(attachment)
			}
			atts[name] = newMeta

		} else {
			// Attachment must be a stub that repeats a parent attachment
			if meta["stub"] != true {
				return base.HTTPErrorf(400, "Missing data of attachment %q", name)
			}
			if revpos, ok := base.ToInt64(meta["revpos"]); !ok || revpos < 1 {
				return base.HTTPErrorf(400, "Missing/invalid revpos in stub attachment %q", name)
			}
			// Try to look up the attachment in the parent revision:
			if parentAttachments == nil {
				if parent, _ := db.getAvailableRev(doc, parentRev); parent != nil {
					parentAttachments, _ = parent["_attachments"].(map[string]interface{})
				}
			}
			if parentAttachments != nil {
				if parentAttachment := parentAttachments[name]; parentAttachment != nil {
					atts[name] = parentAttachment
				}
			} else if meta["digest"] == nil {
				return base.HTTPErrorf(400, "Missing digest in stub attachment %q", name)
			}
		}
	}
	return nil
}

// Goes through a revisions '_attachments' map, loads attachments (by their 'digest' properties)
// and adds 'data' properties containing the data. The data is added as raw []byte; the JSON
// marshaler will convert that to base64.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are loaded.
func (db *Database) loadBodyAttachments(body Body, minRevpos int, deltaSrcKeys map[string]AttachmentKey) (Body, error) {
	body = body.ImmutableAttachmentsCopy()
	for name, value := range BodyAttachments(body) {
		meta := value.(map[string]interface{})
		revpos, ok := base.ToInt64(meta["revpos"])
		if ok && revpos >= int64(minRevpos) {
			key := AttachmentKey(meta["digest"].(string))
			var sourceKeys []AttachmentKey
			if _, ok := meta["encoding"].(string); !ok { // leave encoded attachment alone
				if srcKey, ok := deltaSrcKeys[name]; ok {
					sourceKeys = []AttachmentKey{srcKey}
				}
			}
			data, srcKey, err := db.GetAttachmentMaybeAsDelta(key, sourceKeys)
			if err != nil {
				return nil, err
			}
			meta["data"] = data
			delete(meta, "stub")
			if srcKey != "" {
				meta["encoding"] = "zdelta"
				meta["deltasrc"] = srcKey
			}
		}
	}
	return body, nil
}

// Retrieves an attachment's body given its key.
func (db *Database) GetAttachment(key AttachmentKey) ([]byte, error) {
	return db.Bucket.GetRaw(attachmentKeyToDocKey(key))
}

// Retrieves an attachment's body, preferably as a delta from one of the versions specified
// in `sourceKeys`
func (db *Database) GetAttachmentMaybeAsDelta(key AttachmentKey, sourceKeys []AttachmentKey) (result []byte, sourceKey AttachmentKey, err error) {
	// First, attempt to reuse a cached delta without even having to load the attachment:
	for _, sourceKey = range sourceKeys {
		if result = db.getCachedAttachmentZDelta(sourceKey, key); result != nil {
			// Found a cached delta
			if len(result) == 0 {
				// ... but it's not worth using
				sourceKey = ""
				result, err = db.GetAttachment(key)
			}
			return
		}
	}

	// No cached deltas, so create one:
	target, err := db.GetAttachment(key)
	if err != nil {
		return
	}

	for _, sourceKey = range sourceKeys {
		if src, _ := db.Bucket.GetRaw(attachmentKeyToDocKey(sourceKey)); src != nil {
			// Found a previous revision; generate a delta:
			result = db.generateAttachmentZDelta(src, target, sourceKey, key)
			if result != nil {
				if len(result) == 0 {
					// ... but it's not worth using
					break
				}
				return
			}
		}
	}

	// No previous attachments available so return entire body:
	result = target
	sourceKey = ""
	return
}

// Returns the digests of all attachments in a Body, as a map of attachment names to keys.
func getAttachmentDigests(body Body) map[string]AttachmentKey {
	keys := map[string]AttachmentKey{}
	for name, value := range BodyAttachments(body) {
		meta := value.(map[string]interface{})
		if key := AttachmentKey(meta["digest"].(string)); key != "" {
			keys[name] = key
		}
	}
	return keys
}

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *Database) setAttachment(attachment []byte) (AttachmentKey, error) {
	key := SHA1DigestKey(attachment)
	_, err := db.Bucket.AddRaw(attachmentKeyToDocKey(key), 0, attachment)
	if err == nil {
		base.LogTo("Attach", "\tAdded attachment %q", key)
	}
	return key, err
}

//////// HELPERS:

// Returns an AttachmentKey for an attachment body, based on its SHA-1 digest.
func SHA1DigestKey(data []byte) AttachmentKey {
	digester := sha1.New()
	digester.Write(data)
	return AttachmentKey("sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil)))
}

// Returns the "_attachments" property as a map.
func BodyAttachments(body Body) map[string]interface{} {
	atts, _ := body["_attachments"].(map[string]interface{})
	return atts
}

// The Couchbase bucket key under which to store an attachment
func attachmentKeyToDocKey(key AttachmentKey) string {
	return "_sync:att:" + string(key)
}

// Base64-encodes an attachment if it's present as a raw byte array
func DecodeAttachment(att interface{}) ([]byte, error) {
	switch att := att.(type) {
	case []byte:
		return att, nil
	case string:
		return base64.StdEncoding.DecodeString(att)
	default:
		return nil, base.HTTPErrorf(400, "invalid attachment data (type %T)", att)
	}
}
