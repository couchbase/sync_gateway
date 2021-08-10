//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// AttVersion1 attachments are persisted to the bucket based on attachment body digest.
	AttVersion1 int = 1

	// AttVersion2 attachments are persisted to the bucket based on docID and body digest.
	AttVersion2 int = 2
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
	Version     int    `json:"ver,omitempty"`
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
			digest := Sha1DigestKey(attachment)
			key := AttachmentKey(sha256Digest([]byte(doc.ID)) + ":" + digest)
			newAttachmentData[key] = attachment

			newMeta := map[string]interface{}{
				"stub":   true,
				"digest": digest,
				"revpos": generation,
				"ver":    AttVersion2,
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

			revpos, ok := base.ToInt64(meta["revpos"])
			if !ok || revpos < 1 {
				return nil, base.HTTPErrorf(400, "Missing/invalid revpos in stub attachment %q", name)
			}
			// Try to look up the attachment in ancestor attachments
			if parentAttachments == nil {
				parentAttachments = db.retrieveAncestorAttachments(doc, parentRev, docHistory)
			}

			if parentAttachments != nil {
				if parentAttachment := parentAttachments[name]; parentAttachment != nil {
					parentrevpos, ok := base.ToInt64(parentAttachment.(map[string]interface{})["revpos"])

					if ok && revpos <= parentrevpos {
						atts[name] = parentAttachment
					}
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
			version := GetAttVersion(meta)
			attKey := MakeAttachmentKey(version, docid, digestStr)
			data, err := db.GetAttachmentBy(version, attKey)
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

// GetAttachmentBy retrieves an attachment from the bucket by using its version, docID, and digest.
func (db *Database) GetAttachmentBy(version int, attKey AttachmentKey) ([]byte, error) {
	var key string
	if version == AttVersion2 {
		key = attachmentV2KeyToString(attKey)
	} else {
		key = attachmentKeyToString(attKey)
	}
	v, _, err := db.Bucket.GetRaw(key)
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
		var attKey string
		if strings.Contains(string(key), ":") {
			attKey = attachmentV2KeyToString(key)
		} else {
			attKey = attachmentKeyToString(key)
		}
		_, err := db.Bucket.AddRaw(attKey, 0, data)
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
func (db *Database) ForEachStubAttachment(body Body, minRevpos int, docID string, callback AttachmentCallback) error {
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
			version := GetAttVersion(meta)
			attKey := MakeAttachmentKey(version, docID, digest)
			data, err := db.GetAttachmentBy(version, attKey)
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

func GetAttVersion(meta map[string]interface{}) int {
	if ver, ok := meta["ver"].(json.Number); ok {
		if version, err := ver.Int64(); err != nil {
			return AttVersion1
		} else {
			return int(version)
		}
	} else if ver, ok := meta["ver"].(int); ok {
		return ver
	} else if ver, ok := meta["ver"].(float64); ok {
		return int(ver)
	}
	return AttVersion1
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

// AttachmentStorageMeta holds the metadata for building
// the key for attachment storage and retrieval.
type AttachmentStorageMeta struct {
	digest  string
	version int
}

// ToAttachmentStorageMeta returns a slice of AttachmentStorageMeta, which is contains the
// necessary metadata properties to build the key for attachment storage and retrieval.
func ToAttachmentStorageMeta(attachments AttachmentsMeta) []AttachmentStorageMeta {
	meta := make([]AttachmentStorageMeta, 0, len(attachments))
	for _, att := range attachments {
		if attMap, ok := att.(map[string]interface{}); ok {
			if digest, ok := attMap["digest"]; ok {
				if digestString, ok := digest.(string); ok {
					m := AttachmentStorageMeta{
						digest:  digestString,
						version: GetAttVersion(attMap),
					}
					meta = append(meta, m)
				}
			}
		}
	}
	return meta
}

func attachmentKeyToString(key AttachmentKey) string {
	return base.AttPrefix + string(key)
}

func attachmentV2KeyToString(key AttachmentKey) string {
	return base.Att2Prefix + string(key)
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

// MakeAttachmentKey returns the unique for attachment storage and retrieval.
func MakeAttachmentKey(version int, docID, digest string) AttachmentKey {
	if version == AttVersion2 {
		return AttachmentKey(sha256Digest([]byte(docID)) + ":" + digest)
	}
	return AttachmentKey(digest)
}

// sha256Digest returns sha256 digest of the input bytes encoded
// by using the standard base64 encoding, as defined in RFC 4648.
func sha256Digest(key []byte) string {
	digester := sha256.New()
	digester.Write(key)
	return base64.StdEncoding.EncodeToString(digester.Sum(nil))
}
