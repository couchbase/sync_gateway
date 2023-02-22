//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package document

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// AttVersion1 attachments are persisted to the bucket based on attachment body digest.
	AttVersion1 int = 1

	// AttVersion2 attachments are persisted to the bucket based on docID and body digest.
	AttVersion2 int = 2
)

// AttachmentData holds the attachment key and value bytes.
type AttachmentData map[string][]byte

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

// ErrAttachmentTooLarge is returned when an attempt to attach an oversize attachment is made.
var ErrAttachmentTooLarge = errors.New("attachment too large")

// DeleteAttachmentVersion removes attachment versions from the AttachmentsMeta map specified.
func DeleteAttachmentVersion(attachments AttachmentsMeta) {
	for _, value := range attachments {
		meta := value.(map[string]interface{})
		delete(meta, "ver")
	}
}

func GetAttachmentVersion(meta map[string]interface{}) (int, bool) {
	ver, ok := meta["ver"]
	if !ok {
		return AttVersion1, true
	}
	val, ok := base.ToInt64(ver)
	return int(val), ok
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
	Digest  string
	Version int
}

// ToAttachmentStorageMeta returns a slice of AttachmentStorageMeta, which is contains the
// necessary metadata properties to build the key for attachment storage and retrieval.
func ToAttachmentStorageMeta(attachments AttachmentsMeta) []AttachmentStorageMeta {
	meta := make([]AttachmentStorageMeta, 0, len(attachments))
	for _, att := range attachments {
		if attMap, ok := att.(map[string]interface{}); ok {
			if digest, ok := attMap["digest"]; ok {
				if digestString, ok := digest.(string); ok {
					version, _ := GetAttachmentVersion(attMap)
					m := AttachmentStorageMeta{
						Digest:  digestString,
						Version: version,
					}
					meta = append(meta, m)
				}
			}
		}
	}
	return meta
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
