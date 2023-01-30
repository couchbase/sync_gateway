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
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"

	"github.com/couchbase/sync_gateway/base"
)

// Key for retrieving an attachment from Couchbase.
type AttachmentKey string
type AttachmentData map[AttachmentKey][]byte

// A map of keys -> DocAttachments.
type AttachmentMap map[string]*DocAttachment

// A struct which models an attachment.  Currently only used by test code, however
// new code or refactoring in the main codebase should try to use where appropriate.
type DocAttachment struct {
	ContentType string `json:"content_type,omitempty"`
	Digest      string `json:"digest,omitempty"`
	Length      int    `json:"length,omitempty"`
	Revpos      int    `json:"revpos,omitempty"`
	Stub        bool   `json:"stub,omitempty"`
	Data        []byte `json:"-"` // tell json marshal/unmarshal to ignore this field
}

// Given Attachments Meta to be stored in the database, storeAttachments goes through the map, finds attachments with
// inline bodies, copies the bodies into the Couchbase db, and replaces the bodies with the 'digest' attributes which
// are the keys to retrieving them.
func (db *Database) storeAttachments(doc *Document, newAttachmentsMeta AttachmentsMeta, generation int, parentRev string, docHistory []string) (AttachmentData, error) {
	if len(newAttachmentsMeta) == 0 {
		return nil, nil
	}

	var parentAttachments map[string]interface{}
	newAttachmentData := make(AttachmentData, 0)
	atts := newAttachmentsMeta
	for name, value := range atts {
		meta, ok := value.(map[string]interface{})
		if !ok {
			return nil, base.HTTPErrorf(400, "Invalid _attachments")
		}
		data := meta["data"]
		if data != nil {
			// Attachment contains data, so store it in the db:
			attachment, err := DecodeAttachment(data)
			if err != nil {
				return nil, err
			}
			key := AttachmentKey(Sha1DigestKey(attachment))
			newAttachmentData[key] = attachment

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
				return nil, base.HTTPErrorf(400, "Missing data of attachment %q", name)
			}
			if revpos, ok := base.ToInt64(meta["revpos"]); !ok || revpos < 1 {
				return nil, base.HTTPErrorf(400, "Missing/invalid revpos in stub attachment %q", name)
			}
			// Try to look up the attachment in ancestor attachments
			if parentAttachments == nil {
				parentAttachments = db.retrieveAncestorAttachments(doc, parentRev, docHistory)
			}

			if parentAttachments != nil {
				if parentAttachment := parentAttachments[name]; parentAttachment != nil {
					atts[name] = parentAttachment
				}
			} else if meta["digest"] == nil {
				return nil, base.HTTPErrorf(400, "Missing digest in stub attachment %q", name)
			}
		}
	}
	return newAttachmentData, nil
}

// Attempts to retrieve ancestor attachments for a document. First attempts to find and use a non-pruned ancestor.
// If no non-pruned ancestor is available, checks whether the currently active doc has a common ancestor with the new revision.
// If it does, can use the attachments on the active revision with revpos earlier than that common ancestor.
func (db *Database) retrieveAncestorAttachments(doc *Document, parentRev string, docHistory []string) map[string]interface{} {

	// Attempt to find a non-pruned parent or ancestor
	if ancestorAttachments, foundAncestor := db.getAvailableRevAttachments(doc, parentRev); foundAncestor {
		return ancestorAttachments
	}

	// No non-pruned ancestor is available
	if commonAncestor := doc.History.findAncestorFromSet(doc.CurrentRev, docHistory); commonAncestor != "" {
		parentAttachments := make(map[string]interface{})
		commonAncestorGen := int64(genOfRevID(commonAncestor))
		for name, activeAttachment := range GetBodyAttachments(doc.Body()) {
			if attachmentMeta, ok := activeAttachment.(map[string]interface{}); ok {
				activeRevpos, ok := base.ToInt64(attachmentMeta["revpos"])
				if ok && activeRevpos <= commonAncestorGen {
					parentAttachments[name] = activeAttachment
				}
			}
		}
		return parentAttachments
	}

	return nil
}

// Goes through a given attachments map, loads attachments (by their 'digest' properties)
// and adds 'data' properties containing the data. The data is added as raw []byte; the JSON
// marshaler will convert that to base64.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are loaded.
func (db *Database) loadAttachmentsData(attachments AttachmentsMeta, minRevpos int, docid string) (newAttachments AttachmentsMeta, err error) {
	newAttachments = attachments.ShallowCopy()

	for attachmentName, value := range newAttachments {
		meta := value.(map[string]interface{})
		revpos, ok := base.ToInt64(meta["revpos"])
		if ok && revpos >= int64(minRevpos) {
			digest, ok := meta["digest"]
			if !ok {
				return nil, base.RedactErrorf("Unable to load attachment for doc: %v with name: %v and revpos: %v due to missing digest field", base.UD(docid), base.UD(attachmentName), revpos)
			}
			digestStr, ok := digest.(string)
			if !ok {
				return nil, base.RedactErrorf("Unable to load attachment for doc: %v with name: %v and revpos: %v due to unexpected digest field: %v", base.UD(docid), base.UD(attachmentName), revpos, digest)
			}
			key := AttachmentKey(digestStr)
			data, err := db.GetAttachment(key)
			if err != nil {
				return nil, err
			}
			meta["data"] = data
			delete(meta, "stub")
		}
	}

	return newAttachments, nil
}

// Retrieves an attachment given its key.
func (db *Database) GetAttachment(key AttachmentKey) ([]byte, error) {
	v, _, err := db.Bucket.GetRaw(attachmentKeyToString(key))
	return v, err
}

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *Database) setAttachment(attachment []byte) (AttachmentKey, error) {
	key := AttachmentKey(Sha1DigestKey(attachment))
	_, err := db.Bucket.AddRaw(attachmentKeyToString(key), 0, attachment)
	if err == nil {
		base.InfofCtx(db.Ctx, base.KeyCRUD, "\tAdded attachment %q", base.UD(key))
	}
	return key, err
}

func (db *Database) setAttachments(attachments AttachmentData) error {

	for key, data := range attachments {
		attachmentSize := int64(len(data))
		_, err := db.Bucket.AddRaw(attachmentKeyToString(key), 0, data)
		if err == nil {
			base.InfofCtx(db.Ctx, base.KeyCRUD, "\tAdded attachment %q", base.UD(key))
			db.DbStats.CBLReplicationPush().AttachmentPushCount.Add(1)
			db.DbStats.CBLReplicationPush().AttachmentPushBytes.Add(attachmentSize)
		} else {
			return err
		}
	}
	return nil
}

type AttachmentCallback func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error)

// Given a document body, invokes the callback once for each attachment that doesn't include
// its data. The callback is told whether the attachment body is known to the database, according
// to its digest. If the attachment isn't known, the callback can return data for it, which will
// be added to the metadata as a "data" property.
func (db *Database) ForEachStubAttachment(body Body, minRevpos int, callback AttachmentCallback) error {
	atts := GetBodyAttachments(body)
	if atts == nil && body[BodyAttachments] != nil {
		return base.HTTPErrorf(400, "Invalid _attachments")
	}
	for name, value := range atts {
		meta, ok := value.(map[string]interface{})
		if !ok {
			return base.HTTPErrorf(400, "Invalid attachment")
		}
		if meta["data"] == nil {
			if revpos, ok := base.ToInt64(meta["revpos"]); revpos < int64(minRevpos) || !ok {
				continue
			}
			digest, ok := meta["digest"].(string)
			if !ok {
				return base.HTTPErrorf(400, "Invalid attachment")
			}
			data, err := db.GetAttachment(AttachmentKey(digest))
			if err != nil && !base.IsDocNotFoundError(err) {
				return err
			}

			if newData, err := callback(name, digest, data, meta); err != nil {
				return err
			} else if newData != nil {
				meta["data"] = newData
				delete(meta, "stub")
				delete(meta, "follows")
			}
		}
	}
	return nil
}

// GenerateProofOfAttachment returns a nonce and proof for an attachment body.
func GenerateProofOfAttachment(attachmentData []byte) (nonce []byte, proof string) {
	nonce = make([]byte, 20)
	if _, err := rand.Read(nonce); err != nil {
		base.Panicf("Failed to generate random data: %s", err)
	}
	proof = ProveAttachment(attachmentData, nonce)
	base.Tracef(base.KeyCRUD, "Generated nonce %v and proof %q for attachment: %v", nonce, proof, attachmentData)
	return nonce, proof
}

// ProveAttachment returns the proof for an attachment body and nonce pair.
func ProveAttachment(attachmentData, nonce []byte) (proof string) {
	d := sha1.New()
	d.Write([]byte{byte(len(nonce))})
	d.Write(nonce)
	d.Write(attachmentData)
	proof = "sha1-" + base64.StdEncoding.EncodeToString(d.Sum(nil))
	base.Tracef(base.KeyCRUD, "Generated proof %q using nonce %v for attachment: %v", proof, nonce, attachmentData)
	return proof
}

//////// HELPERS:
// Returns _attachments property from body, when found.  Checks for either map[string]interface{} (unmarshalled with body),
// or AttachmentsMeta (written by body by SG)
func GetBodyAttachments(body Body) AttachmentsMeta {
	switch atts := body[BodyAttachments].(type) {
	case AttachmentsMeta:
		return atts
	case map[string]interface{}:
		return AttachmentsMeta(atts)
	default:
		return nil
	}
}

// AttachmentDigests returns a list of attachment digests contained in the given AttachmentsMeta
func AttachmentDigests(attachments AttachmentsMeta) []string {
	var digests = make([]string, 0, len(attachments))
	for _, att := range attachments {
		if attMap, ok := att.(map[string]interface{}); ok {
			if digest, ok := attMap["digest"]; ok {
				if digestString, ok := digest.(string); ok {
					digests = append(digests, digestString)
				}
			}
		}
	}
	return digests
}

func attachmentKeyToString(key AttachmentKey) string {
	return base.AttPrefix + string(key)
}

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

func Sha1DigestKey(data []byte) string {
	digester := sha1.New()
	digester.Write(data)
	return "sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}
