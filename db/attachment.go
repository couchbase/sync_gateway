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
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// AttachmentsMeta shorter than this will be left in the JSON as base64 rather than being a separate
// MIME part.
const kMaxInlineAttachmentSize = 200

// JSON bodies smaller than this won't be GZip-encoded.
const kMinCompressedJSONSize = 300

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
			attachment, err := decodeAttachment(data)
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

// Attempts to retrieve ancestor attachments for a document.  First attempts to find and use a non-pruned ancestor.
// If no non-pruned ancestor is available, checks whether the currently active doc has a common ancestor with the new revision.
// If it does, can use the attachments on the active revision with revpos earlier than that common ancestor.
func (db *Database) retrieveAncestorAttachments(doc *Document, parentRev string, docHistory []string) map[string]interface{} {

	// FIXME: parent contains _attachment property
	// Attempt to find a non-pruned parent or ancestor
	if parent, _ := db.getAvailable1xRev(doc, parentRev); parent != nil {
		// exit early if we know we have no attachments with a simple byte-contains check
		if !bytes.Contains(parent, []byte(BodyAttachments)) {
			return nil
		}

		// Otherwise, unmarshal attachments into struct
		var parentAttachmentsStruct struct {
			Attachments AttachmentsMeta `json:"_attachments"`
		}
		if err := base.JSONUnmarshal(parent, &parentAttachmentsStruct); err != nil {
			base.Warnf(base.KeyAll, "Error unmarshaling attachments metadata: %s", err)
			return nil
		}

		return parentAttachmentsStruct.Attachments
	}

	// No non-pruned ancestor is available
	if commonAncestor := doc.History.findAncestorFromSet(doc.CurrentRev, docHistory); commonAncestor != "" {
		parentAttachments := make(map[string]interface{})
		for name, activeAttachment := range GetBodyAttachments(doc.Body()) {
			if attachmentMeta, ok := activeAttachment.(map[string]interface{}); ok {
				activeRevpos, ok := base.ToInt64(attachmentMeta["revpos"])
				if ok && activeRevpos <= int64(genOfRevID(commonAncestor)) {
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
			db.DbStats.CblReplicationPush().Add(base.StatKeyAttachmentPushCount, 1)
			db.DbStats.CblReplicationPush().Add(base.StatKeyAttachmentPushBytes, attachmentSize)
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

	decoder := base.JSONDecoder(input)
	decoder.UseNumber()
	if err := decoder.Decode(into); err != nil {
		base.Warnf(base.KeyAll, "Couldn't parse JSON in HTTP request: %v", err)
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
	bytes, err := base.JSONMarshal(body)
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
	for name, value := range GetBodyAttachments(body) {
		meta := value.(map[string]interface{})
		if meta["stub"] != true {
			var err error
			var info attInfo
			info.contentType, _ = meta["content_type"].(string)
			info.data, err = decodeAttachment(meta["data"])
			if info.data == nil {
				base.WarnfCtx(db.Ctx, base.KeyAll, "Couldn't decode attachment %q of doc %q: %v", base.UD(name), base.UD(body[BodyId]), err)
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
		db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyAttachmentPullCount, 1)
		db.DatabaseContext.DbStats.StatsCblReplicationPull().Add(base.StatKeyAttachmentPullBytes, int64(len(info.data)))
		part.Write(info.data)

	}
}

// Adds a new part to the given multipart writer, containing the given revision.
// The revision will be written as a nested multipart body if it has attachments.
func (db *Database) WriteRevisionAsPart(revBody Body, isError bool, compressPart bool, writer *multipart.Writer) error {
	partHeaders := textproto.MIMEHeader{}
	docID, _ := revBody[BodyId].(string)
	revID, _ := revBody[BodyRev].(string)
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
	for name, value := range GetBodyAttachments(body) {
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
		digest := Sha1DigestKey(data)
		name, meta := findFollowingAttachment(digest)
		if meta == nil {
			name, meta = findFollowingAttachment(Md5DigestKey(data))
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
				return nil, base.HTTPErrorf(http.StatusBadRequest, "Attachment length mismatch for %q: read %d bytes, should be %d", name, len(data), length)
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

func GenerateProofOfAttachment(attachmentData []byte) (nonce []byte, proof string) {
	nonce = make([]byte, 20)
	if n, err := rand.Read(nonce); n < len(nonce) {
		base.Panicf(base.KeyAll, "Failed to generate random data: %s", err)
	}
	digester := sha1.New()
	digester.Write([]byte{byte(len(nonce))})
	digester.Write(nonce)
	digester.Write(attachmentData)
	proof = "sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
	return
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

func hasInlineAttachments(body Body) bool {
	for _, value := range GetBodyAttachments(body) {
		if meta, ok := value.(map[string]interface{}); ok && meta["data"] != nil {
			return true
		}
	}
	return false
}

func attachmentKeyToString(key AttachmentKey) string {
	return base.AttPrefix + string(key)
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

func Sha1DigestKey(data []byte) string {
	digester := sha1.New()
	digester.Write(data)
	return "sha1-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}

// This is only here for backwards compatibility.  Otherwise should be avoided.
func Md5DigestKey(data []byte) string {
	digester := md5.New()
	digester.Write(data)
	return "md5-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}
