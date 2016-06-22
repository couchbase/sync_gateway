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
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// Attachments shorter than this will be left in the JSON as base64 rather than being a separate
// MIME part.
const kMaxInlineAttachmentSize = 200

// JSON bodies smaller than this won't be GZip-encoded.
const kMinCompressedJSONSize = 300

// Key for retrieving an attachment from Couchbase.
type AttachmentKey string
type AttachmentData map[AttachmentKey][]byte

// Given a CouchDB document body about to be stored in the database, goes through the _attachments
// dict, finds attachments with inline bodies, copies the bodies into the Couchbase db, and replaces
// the bodies with the 'digest' attributes which are the keys to retrieving them.
func (db *Database) storeAttachments(doc *document, body Body, generation int, parentRev string, docHistory []string) (AttachmentData, error) {
	var parentAttachments map[string]interface{}
	newAttachmentData := make(AttachmentData, 0)
	atts := BodyAttachments(body)
	if atts == nil && body["_attachments"] != nil {
		return nil, base.HTTPErrorf(400, "Invalid _attachments")
	}
	for name, value := range atts {
		meta, ok := value.(map[string]interface{})
		if !ok {
			return nil, base.HTTPErrorf(400, "Invalid _attachments")
		}
		data, exists := meta["data"]
		if exists {
			// Attachment contains data, so store it in the db:
			attachment, err := decodeAttachment(data)
			if err != nil {
				return nil, err
			}
			key := AttachmentKey(sha1DigestKey(attachment))
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

// Attempts to retrieve ancestor attachments for a document.  First attempts to find and use a non-pruned ancestor.
// If no non-pruned ancestor is available, checks whether the currently active doc has a common ancestor with the new revision.
// If it does, can use the attachments on the active revision with revpos earlier than that common ancestor.
func (db *Database) retrieveAncestorAttachments(doc *document, parentRev string, docHistory []string) map[string]interface{} {

	var parentAttachments map[string]interface{}
	// Attempt to find a non-pruned parent or ancestor
	parent, _ := db.getAvailableRev(doc, parentRev)
	if parent != nil {
		parentAttachments, _ = parent["_attachments"].(map[string]interface{})
	} else {
		// No non-pruned ancestor is available
		commonAncestor := doc.History.findAncestorFromSet(doc.CurrentRev, docHistory)
		if commonAncestor != "" {
			parentAttachments = make(map[string]interface{})
			commonAncestorGen, _ := base.ToInt64(genOfRevID(commonAncestor))
			for name, activeAttachment := range BodyAttachments(doc.body) {
				attachmentMeta, ok := activeAttachment.(map[string]interface{})
				if ok {
					activeRevpos, ok := base.ToInt64(attachmentMeta["revpos"])
					if ok && activeRevpos <= commonAncestorGen {
						parentAttachments[name] = activeAttachment
					}
				}
			}
		}
	}
	return parentAttachments
}

// Goes through a revisions '_attachments' map, loads attachments (by their 'digest' properties)
// and adds 'data' properties containing the data. The data is added as raw []byte; the JSON
// marshaler will convert that to base64.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are loaded.
func (db *Database) loadBodyAttachments(body Body, minRevpos int) (Body, error) {

	body = body.ImmutableAttachmentsCopy()
	for _, value := range BodyAttachments(body) {
		meta := value.(map[string]interface{})
		revpos, ok := base.ToInt64(meta["revpos"])
		if ok && revpos >= int64(minRevpos) {
			key := AttachmentKey(meta["digest"].(string))
			data, err := db.GetAttachment(key)
			if err != nil {
				return nil, err
			}
			meta["data"] = data
			delete(meta, "stub")
		}
	}
	return body, nil
}

// Retrieves an attachment, base64-encoded, given its key.
func (db *Database) GetAttachment(key AttachmentKey) ([]byte, error) {
	v, _, err := db.Bucket.GetRaw(attachmentKeyToString(key))
	return v, err
}

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *Database) setAttachment(attachment []byte) (AttachmentKey, error) {
	key := AttachmentKey(sha1DigestKey(attachment))
	_, err := db.Bucket.AddRaw(attachmentKeyToString(key), 0, attachment)
	if err == nil {
		base.LogTo("Attach", "\tAdded attachment %q", key)
	}
	return key, err
}

func (db *Database) setAttachments(attachments AttachmentData) error {
	for key, data := range attachments {
		_, err := db.Bucket.AddRaw(attachmentKeyToString(key), 0, data)
		if err == nil {
			base.LogTo("Attach", "\tAdded attachment %q", key)
		} else {
			return err
		}
	}
	return nil
}

//////// MIME MULTIPART:

// Parses a JSON MIME body, unmarshaling it into "into".
func ReadJSONFromMIME(headers http.Header, input io.Reader, into interface{}) error {
	contentType := headers.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "application/json") {
		return base.HTTPErrorf(http.StatusUnsupportedMediaType, "Invalid content type %s", contentType)
	}

	switch headers.Get("Content-Encoding") {
	case "gzip":
		var err error
		if input, err = gzip.NewReader(input); err != nil {
			return err
		}
	case "":
		break
	default:
		return base.HTTPErrorf(http.StatusUnsupportedMediaType, "Unsupported Content-Encoding; use gzip")
	}

	decoder := json.NewDecoder(input)
	if err := decoder.Decode(into); err != nil {
		base.Warn("Couldn't parse JSON in HTTP request: %v", err)
		return base.HTTPErrorf(http.StatusBadRequest, "Bad JSON")
	}
	return nil
}

type attInfo struct {
	name        string
	contentType string
	data        []byte
}

func writeJSONPart(writer *multipart.Writer, contentType string, body Body, compressed bool) (err error) {
	bytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if len(bytes) < kMinCompressedJSONSize {
		compressed = false
	}

	partHeaders := textproto.MIMEHeader{}
	partHeaders.Set("Content-Type", contentType)
	if compressed {
		partHeaders.Set("Content-Encoding", "gzip")
	}
	part, err := writer.CreatePart(partHeaders)
	if err != nil {
		return err
	}

	if compressed {
		gz := gzip.NewWriter(part)
		_, err = gz.Write(bytes)
		gz.Close()
	} else {
		_, err = part.Write(bytes)
	}
	return
}

// Writes a revision to a MIME multipart writer, encoding large attachments as separate parts.
func (db *Database) WriteMultipartDocument(body Body, writer *multipart.Writer, compress bool) {
	// First extract the attachments that should follow:
	following := []attInfo{}
	for name, value := range BodyAttachments(body) {
		meta := value.(map[string]interface{})
		if meta["stub"] != true {
			var err error
			var info attInfo
			info.contentType, _ = meta["content_type"].(string)
			info.data, err = decodeAttachment(meta["data"])
			if info.data == nil {
				base.Warn("Couldn't decode attachment %q of doc %q: %v", name, body["_id"], err)
				meta["stub"] = true
				delete(meta, "data")
			} else if len(info.data) > kMaxInlineAttachmentSize {
				info.name = name
				following = append(following, info)
				meta["follows"] = true
				delete(meta, "data")
			}
		}
	}

	// Write the main JSON body:
	writeJSONPart(writer, "application/json", body, compress)

	// Write the following attachments
	for _, info := range following {
		partHeaders := textproto.MIMEHeader{}
		if info.contentType != "" {
			partHeaders.Set("Content-Type", info.contentType)
		}
		partHeaders.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", info.name))
		part, _ := writer.CreatePart(partHeaders)
		part.Write(info.data)
	}
}

// Adds a new part to the given multipart writer, containing the given revision.
// The revision will be written as a nested multipart body if it has attachments.
func (db *Database) WriteRevisionAsPart(revBody Body, isError bool, compressPart bool, writer *multipart.Writer) error {
	partHeaders := textproto.MIMEHeader{}
	docID, _ := revBody["_id"].(string)
	revID, _ := revBody["_rev"].(string)
	if len(docID) > 0 {
		partHeaders.Set("X-Doc-ID", docID)
		partHeaders.Set("X-Rev-ID", revID)
	}

	if hasInlineAttachments(revBody) {
		// Write as multipart, including attachments:
		// OPT: Find a way to do this w/o having to buffer the MIME body in memory!
		var buffer bytes.Buffer
		docWriter := multipart.NewWriter(&buffer)
		contentType := fmt.Sprintf("multipart/related; boundary=%q",
			docWriter.Boundary())
		partHeaders.Set("Content-Type", contentType)
		db.WriteMultipartDocument(revBody, docWriter, compressPart)
		docWriter.Close()
		content := bytes.TrimRight(buffer.Bytes(), "\r\n")

		part, err := writer.CreatePart(partHeaders)
		if err == nil {
			_, err = part.Write(content)
		}
		return err
	} else {
		// Write as JSON:
		contentType := "application/json"
		if isError {
			contentType += `; error="true"`
		}
		return writeJSONPart(writer, contentType, revBody, compressPart)
	}
}

func ReadMultipartDocument(reader *multipart.Reader) (Body, error) {
	// First read the main JSON document body:
	mainPart, err := reader.NextPart()
	if err != nil {
		return nil, err
	}
	var body Body
	err = ReadJSONFromMIME(http.Header(mainPart.Header), mainPart, &body)
	mainPart.Close()
	if err != nil {
		return nil, err
	}

	// Collect the attachments with a "follows" property, which will appear as MIME parts:
	followingAttachments := map[string]map[string]interface{}{}
	for name, value := range BodyAttachments(body) {
		if meta := value.(map[string]interface{}); meta["follows"] == true {
			followingAttachments[name] = meta
		}
	}

	// Subroutine to look up a following attachment given its digest. (I used to precompute a
	// map from digest->name, which was faster, but that broke down if there were multiple
	// attachments with the same contents! See #96)
	findFollowingAttachment := func(withDigest string) (string, map[string]interface{}) {
		for name, meta := range followingAttachments {
			if meta["follows"] == true {
				if digest, ok := meta["digest"].(string); ok && digest == withDigest {
					return name, meta
				}
			}
		}
		return "", nil
	}

	// Read the parts one by one:
	for i := 0; i < len(followingAttachments); i++ {
		part, err := reader.NextPart()
		if err != nil {
			if err == io.EOF {
				err = base.HTTPErrorf(http.StatusBadRequest,
					"Too few MIME parts: expected %d attachments, got %d",
					len(followingAttachments), i)
			}
			return nil, err
		}
		data, err := ioutil.ReadAll(part)
		part.Close()
		if err != nil {
			return nil, err
		}

		// Look up the attachment by its digest:
		digest := sha1DigestKey(data)
		name, meta := findFollowingAttachment(digest)
		if meta == nil {
			name, meta = findFollowingAttachment(md5DigestKey(data))
			if meta == nil {
				return nil, base.HTTPErrorf(http.StatusBadRequest,
					"MIME part #%d doesn't match any attachment", i+2)
			}
		}

		length, ok := base.ToInt64(meta["encoded_length"])
		if !ok {
			length, ok = base.ToInt64(meta["length"])
		}
		if ok {
			if length != int64(len(data)) {
				return nil, base.HTTPErrorf(http.StatusBadRequest, "Attachment length mismatch for %q: read %d bytes, should be %g", name, len(data), length)
			}
		}

		// Stuff the data into the attachment metadata and remove the "follows" property:
		delete(meta, "follows")
		meta["data"] = data
		meta["digest"] = digest
	}

	// Make sure there are no unused MIME parts:
	if _, err = reader.NextPart(); err != io.EOF {
		if err == nil {
			err = base.HTTPErrorf(http.StatusBadRequest,
				"Too many MIME parts (expected %d)", len(followingAttachments)+1)
		}
		return nil, err
	}

	return body, nil
}

//////// HELPERS:

func sha1DigestKey(data []byte) string {
	digester := sha1.New()
	digester.Write(data)
	return "sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}

func md5DigestKey(data []byte) string {
	digester := md5.New()
	digester.Write(data)
	return "md5-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}

func BodyAttachments(body Body) map[string]interface{} {
	atts, _ := body["_attachments"].(map[string]interface{})
	return atts
}

func hasInlineAttachments(body Body) bool {
	for _, value := range BodyAttachments(body) {
		if meta, ok := value.(map[string]interface{}); ok && meta["data"] != nil {
			return true
		}
	}
	return false
}

func attachmentKeyToString(key AttachmentKey) string {
	return "_sync:att:" + string(key)
}

func decodeAttachment(att interface{}) ([]byte, error) {
	switch att := att.(type) {
	case []byte:
		return att, nil
	case string:
		return base64.StdEncoding.DecodeString(att)
	default:
		return nil, base.HTTPErrorf(400, "invalid attachment data (type %T)", att)
	}
}
