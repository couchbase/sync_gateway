//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package document

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/exp/maps"
)

const (
	// AttVersion1 attachments are persisted to the bucket based on attachment body digest.
	AttVersion1 int = 1

	// AttVersion2 attachments are persisted to the bucket based on docID and body digest.
	AttVersion2 int = 2
)

// A struct which models an attachment.  Currently only used by test code, however
// new code or refactoring in the main codebase should try to use where appropriate.
type DocAttachment struct {
	ContentType   string `json:"content_type,omitempty"`   // MIME type
	Digest        string `json:"digest,omitempty"`         // Base64 SHA-1 digest
	EncodedLength int    `json:"encoded_length,omitempty"` // Length of encoded data in bytes
	Encoding      string `json:"encoding,omitempty"`       // Optional data encoding like gzip
	Follows       bool   `json:"follows,omitempty"`        // Used only in REST multipart bodies
	Length        int    `json:"length,omitempty"`         // Length of fully-decoded data in bytes
	Revpos        int    `json:"revpos,omitempty"`         // Gen# of last revision changed in
	Stub          bool   `json:"stub,omitempty"`           // True if data has been omitted
	Version       int    `json:"ver,omitempty"`            // Versioning of this metadata
	Data          []byte `json:"data,omitempty"`           // Actual data
}

// A pseudonym for the generic map that a single attachment parses to by default
type DocAttachmentJSON map[string]any

// A document's `_attachments` map parsed into a map of DocAttachment structs.
type AttachmentsMeta map[string]*DocAttachment

// A pseudonym for the generic map that `_attachments` parses to by default
type AttachmentsMetaJSON map[string]any

var (
	// ErrAttachmentVersion is thrown in case of any error in parsing version from the attachment meta.
	ErrAttachmentVersion = base.HTTPErrorf(http.StatusBadRequest, "invalid version found in attachment meta")

	// ErrAttachmentMeta returned when the document contains invalid _attachments metadata properties.
	ErrAttachmentMeta = base.HTTPErrorf(http.StatusBadRequest, "Invalid _attachments")
)

//-------- DOC_ATTACHMENT:

// Converts a generically-unmarshaled object to a DocAttachment.
func DocAttachmentFromMap(att DocAttachmentJSON) (*DocAttachment, error) {
	if att == nil {
		return nil, nil
	}
	// TODO: Replace all this with Marshal + Unmarshal?
	docatt := DocAttachment{}
	var bad string
	var ok bool

	if val, ok := att["content_type"]; ok {
		if docatt.ContentType, ok = val.(string); !ok {
			bad = "content_type"
		}
	}
	if ct, ok := att["digest"]; ok {
		if docatt.Digest, ok = ct.(string); !ok {
			bad = "digest"
		}
	}
	if ct, ok := att["encoding"]; ok {
		if docatt.Encoding, ok = ct.(string); !ok {
			bad = "encoding"
		}
	}
	if ct, ok := att["encoded_length"]; ok {
		if docatt.EncodedLength, ok = base.ToInt(ct); !ok {
			bad = "encoded_length"
		}
	}
	if ct, ok := att["length"]; ok {
		if docatt.Length, ok = base.ToInt(ct); !ok {
			bad = "length"
		}
	}
	if ct, ok := att["revpos"]; ok {
		if docatt.Revpos, ok = base.ToInt(ct); !ok {
			bad = "revpos"
		}
	}
	if val, ok := att["stub"]; ok {
		if docatt.Stub, ok = val.(bool); !ok {
			bad = "stub"
		}
	}
	if docatt.Version, ok = GetAttachmentVersion(att); !ok {
		bad = "ver"
	}
	if b64data, ok := att["data"]; ok {
		var err error
		if docatt.Data, err = DecodeAttachment(b64data); err != nil {
			return nil, err
		}
	}

	if bad == "" {
		return &docatt, nil
	} else {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Invalid %q value in document attachment", bad)
	}
}

// Converts an `any` value to a `*DocAttachment“.
// Recognizes `*DocAttachment` and `map[string]any`.
func DocAttachmentFromAny(x any) (*DocAttachment, error) {
	switch att := x.(type) {
	case DocAttachmentJSON:
	case map[string]any:
		return DocAttachmentFromMap(att)
	case *DocAttachment:
		return att, nil
	case nil:
		return nil, nil
	default:
	}
	return nil, ErrAttachmentMeta
}

// Creates a plain map version of the attachment, with keys "digest", etc.
func (att *DocAttachment) AsMap() AttachmentsMetaJSON {
	data, _ := json.Marshal(att)
	var result AttachmentsMetaJSON
	_ = base.JSONUnmarshal(data, &result)
	return result
}

//-------- ATTACHMENTS_META:

// Converts a `map[string]any` to `AttachmentsMeta` (map of `DocAttachment`)
func AttachmentsMetaFromJSON(atts AttachmentsMetaJSON) (AttachmentsMeta, error) {
	if atts == nil {
		return nil, nil
	}
	meta := make(AttachmentsMeta, len(atts))
	for key, val := range atts {
		if att, err := DocAttachmentFromAny(val); err == nil {
			meta[key] = att
		} else {
			return nil, err
		}
	}
	return meta, nil
}

func AttachmentsMetaFromAny(x any) (AttachmentsMeta, error) {
	switch atts := x.(type) {
	case AttachmentsMeta:
		return atts, nil
	case AttachmentsMetaJSON:
	case map[string]any:
		return AttachmentsMetaFromJSON(atts)
	case nil:
		return nil, nil
	default:
	}
	return nil, ErrAttachmentMeta
}

// Creates a plain map-of-maps version of the attachments.
func (attachments AttachmentsMeta) AsMap() AttachmentsMetaJSON {
	if attachments == nil {
		return nil
	}
	result := make(AttachmentsMetaJSON, len(attachments))
	for key, att := range attachments {
		result[key] = att.AsMap()
	}
	return result
}

// Copies the outer map but not the DocAttachment structs.
func (attachments AttachmentsMeta) ShallowCopy() AttachmentsMeta {
	if attachments == nil {
		return attachments
	}
	return maps.Clone(attachments)
}

// Copies both the outer map and the DocAttachment structs.
func (attachments AttachmentsMeta) DeepCopy() AttachmentsMeta {
	if attachments == nil {
		return attachments
	}
	result := make(AttachmentsMeta, len(attachments))
	for key, att := range attachments {
		copiedAtt := *att
		result[key] = &copiedAtt
	}
	return result
}

func (attachments AttachmentsMeta) String() string {
	strs := []string{}
	for key, att := range attachments {
		strs = append(strs, fmt.Sprintf("%q: %#v", key, *att))
	}
	return "AttachmentsMeta{" + strings.Join(strs, ", ") + "}"
}

// DeleteAttachmentVersion removes attachment versions from the AttachmentsMeta map specified.
func (attachments AttachmentsMeta) DeleteAttachmentVersions() {
	for _, value := range attachments {
		value.Version = 0
	}
}

//-------- ATTACHMENT_STORAGE_META:

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
		if att.Digest != "" {
			m := AttachmentStorageMeta{
				Digest:  att.Digest,
				Version: att.Version,
			}
			meta = append(meta, m)
		}
	}
	return meta
}

//-------- UTILITIES:

// Decodes attachment data. A string is base64 decoded, a byte array is returned unchanged,
// anything else is an error.
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

func GetAttachmentVersion(meta DocAttachmentJSON) (int, bool) {
	ver, ok := meta["ver"]
	if !ok {
		return AttVersion1, true
	}
	val, ok := base.ToInt64(ver)
	return int(val), ok
}
