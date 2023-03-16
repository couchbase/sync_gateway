//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/document"
)

// AttachmentData holds the attachment key and value bytes.
type AttachmentData map[string][]byte

const maxAttachmentSizeBytes = 20 * 1024 * 1024

var (
	// ErrAttachmentVersion is thrown in case of any error in parsing version from the attachment meta.
	ErrAttachmentVersion = base.HTTPErrorf(http.StatusBadRequest, "invalid version found in attachment meta")

	// ErrAttachmentMeta returned when the document contains invalid _attachments metadata properties.
	ErrAttachmentMeta = base.HTTPErrorf(http.StatusBadRequest, "Invalid _attachments")
)

// Given Attachments Meta to be stored in the database, storeAttachments goes through the map, finds attachments with
// inline bodies, copies the bodies into the Couchbase db, and replaces the bodies with the 'digest' attributes which
// are the keys to retrieving them.
func (db *DatabaseCollectionWithUser) storeAttachments(ctx context.Context, doc *Document, newAttachmentsMeta AttachmentsMeta, generation int, parentRev string, docHistory []string) (AttachmentData, error) {
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
			attachment, err := document.DecodeAttachment(data)
			if err != nil {
				return nil, err
			}
			digest := Sha1DigestKey(attachment)
			key := MakeAttachmentKey(document.AttVersion2, doc.ID, digest)
			newAttachmentData[key] = attachment

			newMeta := map[string]interface{}{
				"stub":   true,
				"digest": digest,
				"revpos": generation,
				"ver":    document.AttVersion2,
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
			// Try to look up the attachment in ancestor attachments
			if parentAttachments == nil {
				parentAttachments = db.retrieveAncestorAttachments(ctx, doc, parentRev, docHistory)
			}

			// Note: in a non-conflict CAS retry, parentAttachments may be nil, because the attachment
			//  data was persisted prior to the CAS failure writing the doc.  In this scenario the
			//  incoming doc attachment metadata has already been updated to stub=true to avoid attempting to
			//  persist the attachment again, even though there is not an attachment on an ancestor.
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

// retrieveV2AttachmentKeys returns the list of V2 attachment keys from the attachment metadata that can be used for
// identifying obsolete attachments and triggering subsequent removal of those attachments to reclaim the storage.
func retrieveV2AttachmentKeys(docID string, docAttachments AttachmentsMeta) (attachments map[string]struct{}, err error) {
	attachments = make(map[string]struct{})
	for _, value := range docAttachments {
		meta, ok := value.(map[string]interface{})
		if !ok {
			return nil, ErrAttachmentMeta
		}
		digest, ok := meta["digest"].(string)
		if !ok {
			return nil, ErrAttachmentMeta
		}
		version, _ := document.GetAttachmentVersion(meta)
		if version != AttVersion2 {
			continue
		}
		key := MakeAttachmentKey(version, docID, digest)
		attachments[key] = struct{}{}
	}
	return attachments, nil
}

// Attempts to retrieve ancestor attachments for a document. First attempts to find and use a non-pruned ancestor.
// If no non-pruned ancestor is available, checks whether the currently active doc has a common ancestor with the new revision.
// If it does, can use the attachments on the active revision with revpos earlier than that common ancestor.
func (db *DatabaseCollectionWithUser) retrieveAncestorAttachments(ctx context.Context, doc *Document, parentRev string, docHistory []string) map[string]interface{} {

	// Attempt to find a non-pruned parent or ancestor
	if ancestorAttachments, foundAncestor := db.getAvailableRevAttachments(ctx, doc, parentRev); foundAncestor {
		return ancestorAttachments
	}

	// No non-pruned ancestor is available
	if commonAncestor := doc.History.FindAncestorFromSet(doc.CurrentRev, docHistory); commonAncestor != "" {
		parentAttachments := make(map[string]interface{})
		commonAncestorGen := int64(document.GenOfRevID(commonAncestor))

		inlineAttachments, _ := doc.InlineAttachments()
		for name, activeAttachment := range inlineAttachments {
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
// marshaller will convert that to base64.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are loaded.
func (c *DatabaseCollection) LoadAttachmentsData(attachments AttachmentsMeta, minRevpos int, docid string) (newAttachments AttachmentsMeta, err error) {
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
			version, ok := document.GetAttachmentVersion(meta)
			if !ok {
				return nil, base.RedactErrorf("Unable to load attachment for doc: %v with name: %v, revpos: %v and digest: %v due to unexpected version value: %v", base.UD(docid), base.UD(attachmentName), revpos, digest, version)
			}
			attachmentKey := MakeAttachmentKey(version, docid, digestStr)
			data, err := c.GetAttachment(attachmentKey)
			if err != nil {
				return nil, err
			}
			meta["data"] = data
			delete(meta, "stub")
		}
	}

	return newAttachments, nil
}

// GetAttachment retrieves an attachment given its key.
func (c *DatabaseCollection) GetAttachment(key string) ([]byte, error) {
	v, _, err := c.dataStore.GetRaw(key)
	return v, err
}

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *DatabaseCollectionWithUser) setAttachment(ctx context.Context, key string, value []byte) error {
	_, err := db.dataStore.AddRaw(key, 0, value)
	if err == nil {
		base.InfofCtx(ctx, base.KeyCRUD, "\tAdded attachment %q", base.UD(key))
	}
	return err
}

func (db *DatabaseCollectionWithUser) setAttachments(ctx context.Context, attachments AttachmentData) error {
	for key, data := range attachments {
		attachmentSize := int64(len(data))
		if attachmentSize > int64(maxAttachmentSizeBytes) {
			return document.ErrAttachmentTooLarge
		}
		_, err := db.dataStore.AddRaw(key, 0, data)
		if err == nil {
			base.InfofCtx(ctx, base.KeyCRUD, "\tAdded attachment %q", base.UD(key))
			db.dbStats().CBLReplicationPush().AttachmentPushCount.Add(1)
			db.dbStats().CBLReplicationPush().AttachmentPushBytes.Add(attachmentSize)
		} else {
			return err
		}
	}
	return nil
}

type AttachmentCallback func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error)

// Given a document body, invokes the callback once for each attachment that doesn't include
// its data, and isn't present with a matching digest on the existing doc (existingDigests).
// The callback is told whether the attachment body is known to the database, according
// to its digest. If the attachment isn't known, the callback can return data for it, which will
// be added to the metadata as a "data" property.
func (c *DatabaseCollection) ForEachStubAttachment(body Body, minRevpos int, docID string, existingDigests map[string]string, callback AttachmentCallback) error {
	atts := document.GetBodyAttachments(body)
	if atts == nil && body[BodyAttachments] != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid _attachments")
	}
	for name, value := range atts {
		meta, ok := value.(map[string]interface{})
		if !ok {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid attachment")
		}
		if meta["data"] == nil {
			if revpos, ok := base.ToInt64(meta["revpos"]); revpos < int64(minRevpos) || !ok {
				continue
			}
			digest, ok := meta["digest"].(string)
			if !ok {
				return base.HTTPErrorf(http.StatusBadRequest, "Invalid attachment")
			}

			// If digest matches the one on existing doc, SG doesn't need to prove/get
			existingDigest, existingOk := existingDigests[name]
			if existingOk && existingDigest == digest {
				// see CBG-2010 for discussion of potential existence check here
				continue
			}

			// Assumes the attachment is always AttVersion2 while checking whether it has already been uploaded.
			attachmentKey := MakeAttachmentKey(AttVersion2, docID, digest)
			data, err := c.GetAttachment(attachmentKey)
			if err != nil && !base.IsDocNotFoundError(err) {
				return err
			}
			newData, err := callback(name, digest, data, meta)
			if err != nil {
				return err
			}
			if newData != nil {
				meta["data"] = newData
				delete(meta, "stub")
				delete(meta, "follows")
			} else {
				// Update version in the case where this is a new attachment on the doc sharing a V2 digest with
				// an existing attachment
				meta["ver"] = AttVersion2
			}
		}
	}
	return nil
}

// ////// HELPERS:

// MakeAttachmentKey returns the unique for attachment storage and retrieval.
func MakeAttachmentKey(version int, docID, digest string) string {
	if version == AttVersion2 {
		return base.Att2Prefix + sha256Digest([]byte(docID)) + ":" + digest
	}
	return base.AttPrefix + digest
}

func Sha1DigestKey(data []byte) string {
	digester := sha1.New()
	digester.Write(data)
	return "sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}

// sha256Digest returns sha256 digest of the input bytes encoded
// by using the standard base64 encoding, as defined in RFC 4648.
func sha256Digest(key []byte) string {
	digester := sha256.New()
	digester.Write(key)
	return base64.StdEncoding.EncodeToString(digester.Sum(nil))
}
