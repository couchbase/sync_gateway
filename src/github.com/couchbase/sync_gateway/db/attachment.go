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
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/textproto"
	"regexp"

	"github.com/couchbase/sync_gateway/base"
)

// Key for retrieving an attachment from Couchbase.
type AttachmentKey struct {
	Digest   string // "sha1-" followed by a hex SHA-1 digest.
	Encoding string // empty or "gzip"
}

func attachmentKeyFromMeta(meta map[string]interface{}) AttachmentKey {
	digest, _ := meta["digest"].(string)
	encoding, _ := meta["encoding"].(string)
	return AttachmentKey{Digest: digest, Encoding: encoding}
}

// Returns an AttachmentKey for an attachment body, based on its SHA-1 digest.
func SHA1DigestKey(data []byte) AttachmentKey {
	digester := sha1.New()
	digester.Write(data)
	return AttachmentKey{Digest: "sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))}
}

func (key *AttachmentKey) HasGZipEncoding() bool {
	return key.Encoding == "gzip"
}

func (key *AttachmentKey) EncodedString() string {
	if key.HasGZipEncoding() {
		return "Z" + key.Digest
	} else {
		return "-" + key.Digest
	}
}

// The Couchbase bucket key under which to store an attachment
func (key *AttachmentKey) bucketKey() string {
	return "_sync:att:" + key.Digest
}

// Represents an attachment. Contains a references to the metadata map in the Body, and can
// change it as needed.
type Attachment struct {
	Name                 string                 // Filename (key in _attachments map)
	followingData        []byte                 // Data to appear in MIME part
	possibleDeltaSources []AttachmentKey        // Possible attachments to use as delta source
	deltaSource          *AttachmentKey         // Delta source attachment ID
	meta                 map[string]interface{} // Points inside the Body's _attachments map
	db                   *Database              // Database to load the data from
}

// The MIME content type of the attachment, or an empty string if not set
func (a *Attachment) ContentType() string {
	value, _ := a.meta["content_type"].(string)
	return value
}

func (a *Attachment) IsEncoded() bool {
	return a.meta["encoding"] != nil
}

// The attachment digest as stored in the "digest" metadata property.
func (a *Attachment) Key() AttachmentKey {
	return attachmentKeyFromMeta(a.meta)
}

// The attachment's MIME headers. If `full` is true, adds headers appropriate for a top-level
// MIME body, else adds ones appropriate for a nested part.
func (a *Attachment) Headers(full bool) textproto.MIMEHeader {
	h := textproto.MIMEHeader{}
	if a.IsDelta() {
		h.Set("Content-Encoding", "zdelta")
		h.Set("X-Delta-Source", string(a.deltaSource.Digest))
	} else if encoding, _ := a.meta["encoding"].(string); encoding != "" {
		h.Set("Content-Encoding", encoding)
	}
	if full {
		if contentType := a.ContentType(); contentType != "" {
			h.Set("Content-Type", contentType)
		}
	} else {
		h.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", a.Name))
	}
	return h
}

// The raw data of the attachment, if already loaded. May be gzipped, may be a delta.
func (a *Attachment) RawData() []byte {
	data := a.followingData
	if data == nil {
		data, _ = a.meta["data"].([]byte)
	}
	return data
}

func (a *Attachment) IsDelta() bool {
	return a.deltaSource != nil
}

// Loads the data of an attachment (inline).
// If `deltaOK` is true, and a.possibleDeltaSources is set, may load a delta.
func (a *Attachment) LoadData(deltaOK bool) ([]byte, error) {
	data := a.RawData()
	var err error
	if data == nil {
		var sourceKeys []AttachmentKey
		if deltaOK && a.possibleDeltaSources != nil && a.Compressible() {
			sourceKeys = a.possibleDeltaSources
		}

		var deltaSource *AttachmentKey
		data, deltaSource, err = a.db.GetAttachmentMaybeAsDelta(a.Key(), sourceKeys)
		if err == nil {
			a.meta["data"] = data
			a.possibleDeltaSources = nil
			a.deltaSource = deltaSource
			if deltaSource != nil {
				a.meta["zdeltasrc"] = deltaSource.Digest
			}
			delete(a.meta, "stub")
			delete(a.meta, "encoded_length")
		}
	}
	return data, err
}

// Is an attachment's data to be stored outside the JSON body (i.e. in a MIME part)?
func (a *Attachment) Follows() bool {
	return a.meta["follows"] == true
}

// Converts an attachment from inline to following
func (a *Attachment) SetFollows() {
	data := a.meta["data"]
	if data != nil {
		a.followingData, _ = decodeIfBase64(data)
		delete(a.meta, "data")
		delete(a.meta, "zdeltasrc")
		a.meta["follows"] = true
	}
}

var kCompressedTypes, kGoodTypes, kBadTypes, kBadFilenames *regexp.Regexp

func init() {
	// MIME types that explicitly indicate they're compressed:
	kCompressedTypes, _ = regexp.Compile(`(?i)\bg?zip\b`)
	// MIME types that are compressible:
	kGoodTypes, _ = regexp.Compile(`(?i)(^text)|(xml\b)|(\b(html|json|yaml)\b)`)
	// ... or generally uncompressible:
	kBadTypes, _ = regexp.Compile(`(?i)^(audio|image|video)/`)
	// An interesting type is SVG (image/svg+xml) which matches _both_! (It's compressible.)
	// See <http://www.iana.org/assignments/media-types/media-types.xhtml>

	// Filename extensions of uncompressible types:
	kBadFilenames, _ = regexp.Compile(`(?i)\.(zip|t?gz|rar|7z|jpe?g|png|gif|svgz|mp3|m4a|ogg|wav|aiff|mp4|mov|avi|theora)$`)
}

// Returns true if this attachment is worth trying to compress.
func (a *Attachment) Compressible() bool {
	if a.IsDelta() {
		return false // leave delta'd attachment alone
	} else if kBadFilenames.MatchString(a.Name) {
		return false
	} else if contentType := a.ContentType(); contentType != "" {
		return !kCompressedTypes.MatchString(contentType) &&
			(kGoodTypes.MatchString(contentType) ||
				!kBadTypes.MatchString(contentType))
	}
	return true // be optimistic by default
}

//////// LOADING ATTACHMENTS:

// Goes through a revisions '_attachments' map and creates an Attachment object for each
// attachment. Also updates the Body to be safely mutable.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are returned.
func (db *Database) findAttachments(body Body, minRevpos int, deltaSrcKeys map[string]AttachmentKey) (Body, []*Attachment) {
	body = body.ImmutableAttachmentsCopy()
	var attachments []*Attachment
	for name, value := range body.Attachments() {
		meta := value.(map[string]interface{})
		revpos, ok := base.ToInt64(meta["revpos"])
		if ok && revpos >= int64(minRevpos) {
			var possibleDeltas []AttachmentKey
			if src, ok := deltaSrcKeys[name]; ok {
				possibleDeltas = []AttachmentKey{src}
			}
			attachments = append(attachments, &Attachment{
				Name:                 name,
				meta:                 meta,
				db:                   db,
				possibleDeltaSources: possibleDeltas,
			})
		}
	}
	return body, attachments
}

// Retrieves an attachment's body given its key. Does not decode GZip-encoded attachments.
func (db *Database) GetAttachment(key AttachmentKey) ([]byte, error) {
	return db.Bucket.GetRaw(key.bucketKey())
}

func unzip(input []byte) (data []byte, err error) {
	reader := bytes.NewReader(input)
	var gz *gzip.Reader
	if gz, err = gzip.NewReader(reader); err != nil {
		return nil, err
	}
	return ioutil.ReadAll(gz)
}

// Retrieves an attachment's body, preferably as a delta from one of the versions specified
// in `sourceKeys`
func (db *Database) GetAttachmentMaybeAsDelta(key AttachmentKey, sourceKeys []AttachmentKey) (result []byte, sourceKey *AttachmentKey, err error) {
	base.TEMP("GetAttachmentMaybeAsDelta: key=%v, sourceKeys=%v", key, sourceKeys)
	// First, attempt to reuse a cached delta without even having to load the attachment:
	for _, possibleSourceKey := range sourceKeys {
		if result = db.getCachedAttachmentZDelta(possibleSourceKey, key); result != nil {
			// Found a cached delta
			if len(result) > 0 {
				sourceKey = &possibleSourceKey
			} else {
				// ... but it's not worth using
				result, err = db.GetAttachment(key)
			}
			return
		}
	}

	// No cached deltas. First get the current attachment:
	target, err := db.GetAttachment(key)
	if err != nil {
		return
	}

	if len(sourceKeys) > 0 {
		// Going to find a source version to delta it with, but first decode it if needed:
		decodedTarget := target
		if key.HasGZipEncoding() {
			if decodedTarget, _ = unzip(target); decodedTarget == nil {
				base.Warn("GetAttachmentMaybeAsDelta: Couldn't decode gzip-encoded target attachment %s",
					key.Digest)
				return target, nil, nil // Won't decode; just give up & return raw
			}
		}

		for _, possibleSourceKey := range sourceKeys {
			if src, _ := db.Bucket.GetRaw(possibleSourceKey.bucketKey()); src != nil {
				// Found a previous revision; generate a delta:
				if possibleSourceKey.HasGZipEncoding() {
					if src, err = unzip(src); err != nil {
						base.Warn("GetAttachmentMaybeAsDelta: Couldn't decode gzip-encoded source attachment %s",
							possibleSourceKey.Digest)
						continue
					}
				}
				result = db.generateAttachmentZDelta(src, decodedTarget, possibleSourceKey, key)
				base.TEMP("Generated delta: %x", result)
				if result != nil {
					if len(result) > 0 {
						sourceKey = &possibleSourceKey
						return
					} else {
						// ... but it's not worth using
						break
					}
				}
			}
		}
	}

	// No previous attachments available so return entire (maybe-encoded) body:
	result = target
	return
}

//////// STORING ATTACHMENTS:

// Given a CouchDB document body about to be stored in the database, goes through the _attachments
// dict, finds attachments with inline bodies, copies the bodies into the Couchbase db, and replaces
// the bodies with the 'digest' attributes which are the keys to retrieving them.
func (db *Database) storeAttachments(doc *document, body Body, generation int, parentRev string) error {
	var parentAttachments map[string]interface{}
	atts := body.Attachments()
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
			attachment, err := decodeIfBase64(data)
			if err != nil {
				return err
			}
			key, err := db.storeAttachment(attachment)
			if err != nil {
				return err
			}

			newMeta := map[string]interface{}{
				"stub":   true,
				"digest": key.Digest,
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

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *Database) storeAttachment(attachment []byte) (AttachmentKey, error) {
	key := SHA1DigestKey(attachment)
	_, err := db.Bucket.AddRaw(key.bucketKey(), 0, attachment)
	if err == nil {
		base.LogTo("Attach", "\tAdded attachment %q", key)
	}
	return key, err
}

//////// HELPERS:

// Returns the "_attachments" property as a map.
func (body Body) Attachments() map[string]interface{} {
	atts, _ := body["_attachments"].(map[string]interface{})
	return atts
}

// Returns the digests of all attachments in a Body, as a map of attachment names to keys.
func (body Body) AttachmentDigests() map[string]AttachmentKey {
	keys := map[string]AttachmentKey{}
	for name, value := range body.Attachments() {
		meta, _ := value.(map[string]interface{})
		if key := attachmentKeyFromMeta(meta); key.Digest != "" {
			keys[name] = key
		}
	}
	return keys
}

// Base64-decodes attachment data if it's present as a string
func decodeIfBase64(data interface{}) ([]byte, error) {
	switch data := data.(type) {
	case []byte:
		return data, nil
	case string:
		return base64.StdEncoding.DecodeString(data)
	default:
		return nil, base.HTTPErrorf(400, "invalid attachment data (type %T)", data)
	}
}
