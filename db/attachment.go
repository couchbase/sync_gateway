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
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/document"
)

// AttachmentData holds the attachment key and value bytes.
type AttachmentData map[string][]byte

const maxAttachmentSizeBytes = 20 * 1024 * 1024

var (
	// ErrAttachmentTooLarge is returned when an attempt to attach an oversize attachment is made.
	ErrAttachmentTooLarge = errors.New("attachment too large")
)

// Given Attachments Meta to be stored in the database, storeAttachments goes through the map, finds attachments with
// inline bodies, copies the bodies into the Couchbase db, and replaces the bodies with the 'digest' attributes which
// are the keys to retrieving them.
func (db *DatabaseCollectionWithUser) storeAttachments(ctx context.Context, doc *Document, newAttachmentsMeta AttachmentsMeta, generation int, parentRev string, docHistory []string) (AttachmentData, error) {
	if len(newAttachmentsMeta) == 0 {
		return nil, nil
	}

	var parentAttachments AttachmentsMeta
	newAttachmentData := make(AttachmentData, 0)
	atts := newAttachmentsMeta
	for name, meta := range atts {
		data := meta.Data
		if data != nil {
			// Attachment contains data, so store it in the db:
			attachment, err := document.DecodeAttachment(data)
			if err != nil {
				return nil, err
			}
			digest := Sha1DigestKey(attachment)
			key := MakeAttachmentKey(document.AttVersion2, doc.ID, digest)
			newAttachmentData[key] = attachment

			newMeta := *meta
			newMeta.Data = nil
			newMeta.Stub = true
			newMeta.Digest = digest
			newMeta.Revpos = generation
			newMeta.Version = document.AttVersion2

			if meta.Encoding != "" {
				newMeta.EncodedLength = len(attachment)
			} else {
				newMeta.Length = len(attachment)
			}
			atts[name] = &newMeta

		} else {
			// Attachment must be a stub that repeats a parent attachment
			if !meta.Stub {
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
			} else if meta.Digest == "" {
				return nil, base.HTTPErrorf(400, "Missing digest in stub attachment %q", name)
			}
			meta.Data = nil
		}
	}
	return newAttachmentData, nil
}

// retrieveV2AttachmentKeys returns the list of V2 attachment keys from the attachment metadata that can be used for
// identifying obsolete attachments and triggering subsequent removal of those attachments to reclaim the storage.
func retrieveV2AttachmentKeys(docID string, docAttachments AttachmentsMeta) (attachments map[string]struct{}, err error) {
	attachments = make(map[string]struct{})
	for _, meta := range docAttachments {
		if meta.Digest == "" {
			return nil, document.ErrAttachmentMeta
		}
		if meta.Version == AttVersion2 {
			key := MakeAttachmentKey(meta.Version, docID, meta.Digest)
			attachments[key] = struct{}{}
		}
	}
	return attachments, nil
}

// Attempts to retrieve ancestor attachments for a document. First attempts to find and use a non-pruned ancestor.
// If no non-pruned ancestor is available, checks whether the currently active doc has a common ancestor with the new revision.
// If it does, can use the attachments on the active revision with revpos earlier than that common ancestor.
func (db *DatabaseCollectionWithUser) retrieveAncestorAttachments(ctx context.Context, doc *Document, parentRev string, docHistory []string) AttachmentsMeta {

	// Attempt to find a non-pruned parent or ancestor
	if ancestorAttachments, foundAncestor := db.getAvailableRevAttachments(ctx, doc, parentRev); foundAncestor {
		return ancestorAttachments
	}

	// No non-pruned ancestor is available
	if commonAncestor := doc.History.FindAncestorFromSet(doc.CurrentRev, docHistory); commonAncestor != "" {
		parentAttachments := make(AttachmentsMeta)
		commonAncestorGen := document.GenOfRevID(commonAncestor)

		inlineAttachments, _ := doc.InlineAttachments()
		for name, attachmentMeta := range inlineAttachments {
			if attachmentMeta.Revpos <= commonAncestorGen {
				parentAttachments[name] = attachmentMeta
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

	for attachmentName, meta := range newAttachments {
		if revpos := meta.Revpos; revpos >= minRevpos {
			if meta.Digest == "" {
				return nil, base.RedactErrorf("Unable to load attachment for doc: %v with name: %v and revpos: %v due to missing digest field", base.UD(docid), base.UD(attachmentName), revpos)
			}
			attachmentKey := MakeAttachmentKey(meta.Version, docid, meta.Digest)
			data, err := c.GetAttachment(attachmentKey)
			if err != nil {
				return nil, err
			}
			meta.Data = data
			meta.Stub = false
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
			return ErrAttachmentTooLarge
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

type AttachmentCallback func(name string, digest string, knownData []byte, meta *DocAttachment) ([]byte, error)

// Given a document body, invokes the callback once for each attachment that doesn't include
// its data, and isn't present with a matching digest on the existing doc (existingDigests).
// The callback is told whether the attachment body is known to the database, according
// to its digest. If the attachment isn't known, the callback can return data for it, which will
// be added to the metadata as a "data" property.
func (c *DatabaseCollection) ForEachStubAttachment(body Body, minRevpos int, docID string, existingDigests map[string]string, callback AttachmentCallback) error {
	atts, err := body.GetAttachments()
	if err != nil {
		return err
	}
	for name, meta := range atts {
		if meta.Data == nil {
			if revpos := meta.Revpos; revpos < minRevpos {
				continue
			}
			digest := meta.Digest
			if digest == "" {
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
				meta.Data = newData
				meta.Stub = false
				meta.Follows = false
			} else {
				// Update version in the case where this is a new attachment on the doc sharing a V2 digest with
				// an existing attachment
				meta.Version = AttVersion2
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

// GenerateProofOfAttachment returns a nonce and proof for an attachment body.
func GenerateProofOfAttachment(attachmentData []byte) (nonce []byte, proof string, err error) {
	nonce = make([]byte, 20)
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", base.HTTPErrorf(http.StatusInternalServerError, fmt.Sprintf("Failed to generate random data: %s", err))
	}
	proof = ProveAttachment(attachmentData, nonce)
	base.TracefCtx(context.Background(), base.KeyCRUD, "Generated nonce %v and proof %q for attachment: %v", nonce, proof, attachmentData)
	return nonce, proof, nil
}

// ProveAttachment returns the proof for an attachment body and nonce pair.
func ProveAttachment(attachmentData, nonce []byte) (proof string) {
	d := sha1.New()
	d.Write([]byte{byte(len(nonce))})
	d.Write(nonce)
	d.Write(attachmentData)
	proof = "sha1-" + base64.StdEncoding.EncodeToString(d.Sum(nil))
	base.TracefCtx(context.Background(), base.KeyCRUD, "Generated proof %q using nonce %v for attachment: %v", proof, nonce, attachmentData)
	return proof
}
