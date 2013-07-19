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

	"github.com/couchbaselabs/sync_gateway/base"
)

const kMaxInlineAttachmentSize = 200

// Key for retrieving an attachment from Couchbase.
type AttachmentKey string

// Given a CouchDB document body about to be stored in the database, goes through the _attachments
// dict, finds attachments with inline bodies, copies the bodies into the Couchbase db, and replaces
// the bodies with the 'digest' attributes which are the keys to retrieving them.
func (db *Database) storeAttachments(doc *document, body Body, generation int, parentRev string) error {
	var parentAttachments map[string]interface{}
	atts := BodyAttachments(body)
	for name, value := range atts {
		meta := value.(map[string]interface{})
		data, exists := meta["data"]
		if exists {
			// Attachment contains data, so store it in the db:
			attachment, err := decodeAttachment(data)
			if err != nil {
				return err
			}
			key, err := db.setAttachment(attachment)
			if err != nil {
				return err
			}
			delete(meta, "data")
			meta["stub"] = true
			meta["length"] = len(attachment)
			meta["digest"] = string(key)
			meta["revpos"] = generation
		} else {
			// No data given; look it up from the parent revision.
			if parentAttachments == nil {
				parent, err := db.getAvailableRev(doc, parentRev)
				if err != nil {
					base.Warn("storeAttachments: no such parent rev %q to find %v", parentRev, meta)
					return err
				}
				parentAttachments, exists = parent["_attachments"].(map[string]interface{})
				if !exists {
					return &base.HTTPError{400, "Unknown attachment " + name}
				}
			}
			parentAttachment := parentAttachments[name]
			if parentAttachment == nil {
				return &base.HTTPError{400, "Unknown attachment " + name}
			}
			atts[name] = parentAttachment
		}
	}
	return nil
}

// Goes through a revisions '_attachments' map, loads attachments (by their 'digest' properties)
// and adds 'data' properties containing the data. The data is added as raw []byte; the JSON
// marshaler will convert that to base64.
// If minRevpos is > 0, then only attachments that have been changed in a revision of that
// generation or later are loaded.
func (db *Database) loadBodyAttachments(body Body, minRevpos int) error {
	for _, value := range BodyAttachments(body) {
		meta := value.(map[string]interface{})
		revpos, ok := base.ToInt64(meta["revpos"])
		if ok && revpos >= int64(minRevpos) {
			key := AttachmentKey(meta["digest"].(string))
			data, err := db.GetAttachment(key)
			if err != nil {
				return err
			}
			meta["data"] = data
			delete(meta, "stub")
		}
	}
	return nil
}

// Retrieves an attachment, base64-encoded, given its key.
func (db *Database) GetAttachment(key AttachmentKey) ([]byte, error) {
	return db.Bucket.GetRaw(attachmentKeyToString(key))
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

//////// MIME MULTIPART:

// Parses a JSON request body, unmarshaling it into "into".
func ReadJSONFromMIME(headers http.Header, input io.Reader, into interface{}) error {
	contentType := headers.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "application/json") {
		return &base.HTTPError{http.StatusUnsupportedMediaType, "Invalid content type " + contentType}
	}
	body, err := ioutil.ReadAll(input)
	if err != nil {
		return &base.HTTPError{http.StatusBadRequest, ""}
	}
	err = json.Unmarshal(body, into)
	if err != nil {
		base.Warn("Couldn't parse JSON:\n%s", body)
		return &base.HTTPError{http.StatusBadRequest, "Bad JSON"}
	}
	return nil
}

type attInfo struct {
	name        string
	contentType string
	data        []byte
}

// Writes a revision to a MIME multipart writer, encoding large attachments as separate parts.
func (db *Database) WriteMultipartDocument(body Body, writer *multipart.Writer) {
	// First extract the attachments that should follow:
	following := []attInfo{}
	for name, value := range BodyAttachments(body) {
		meta := value.(map[string]interface{})
		var info attInfo
		info.contentType, _ = meta["content_type"].(string)
		info.data, _ = decodeAttachment(meta["data"])
		if info.data != nil && len(info.data) > kMaxInlineAttachmentSize {
			info.name = name
			following = append(following, info)
			delete(meta, "data")
			meta["follows"] = true
		}
	}

	// Write the main JSON body:
	jsonOut, _ := json.Marshal(body)
	partHeaders := textproto.MIMEHeader{}
	partHeaders.Set("Content-Type", "application/json")
	part, _ := writer.CreatePart(partHeaders)
	part.Write(jsonOut)

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

	digestIndex := map[string]string{} // maps digests -> names

	// Now look for "following" attachments:
	attachments := BodyAttachments(body)
	for name, value := range attachments {
		meta := value.(map[string]interface{})
		if meta["follows"] == true {
			digest, ok := meta["digest"].(string)
			if !ok {
				return nil, &base.HTTPError{http.StatusBadRequest, "Missing digest in attachment"}
			}
			digestIndex[digest] = name
		}
	}

	// Read the parts one by one:
	for i := 0; i < len(digestIndex); i++ {
		part, err := reader.NextPart()
		if err != nil {
			if err == io.EOF {
				err = &base.HTTPError{http.StatusBadRequest, "Too few MIME parts"}
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
		name, ok := digestIndex[digest]
		if !ok {
			name, ok = digestIndex[md5DigestKey(data)]
		}
		if !ok {
			return nil, &base.HTTPError{http.StatusBadRequest,
				fmt.Sprintf("MIME part #%d doesn't match any attachment", i+2)}
		}

		meta := attachments[name].(map[string]interface{})
		length, ok := base.ToInt64(meta["encoded_length"])
		if !ok {
			length, ok = base.ToInt64(meta["length"])
		}
		if ok {
			if length != int64(len(data)) {
				return nil, &base.HTTPError{http.StatusBadRequest, fmt.Sprintf("Attachment length mismatch for %q: read %d bytes, should be %g", name, len(data), length)}
			}
		}

		delete(meta, "follows")
		meta["data"] = data
		meta["digest"] = digest
	}

	// Make sure there are no unused MIME parts:
	_, err = reader.NextPart()
	if err != io.EOF {
		return nil, &base.HTTPError{http.StatusBadRequest, "Too many MIME parts"}
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

func attachmentKeyToString(key AttachmentKey) string {
	return "_sync:att:" + string(key)
}

func decodeAttachment(att interface{}) ([]byte, error) {
	switch att := att.(type) {
	case string:
		return base64.StdEncoding.DecodeString(att)
	case []byte:
		return att, nil
	}
	return nil, &base.HTTPError{400, "invalid attachment data"}
}
