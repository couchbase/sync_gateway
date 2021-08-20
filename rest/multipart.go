/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/couchbase/sync_gateway/db"
	"github.com/pkg/errors"

	"github.com/couchbase/sync_gateway/base"
)

// AttachmentsMeta shorter than this will be left in the JSON as base64 rather than being a separate
// MIME part.
const kMaxInlineAttachmentSize = 200

// JSON bodies smaller than this won't be GZip-encoded.
const kMinCompressedJSONSize = 300

// ReadJSONFromMIME parses a JSON MIME body, unmarshalling it into "into".
// Closes the input io.ReadCloser once done.
func ReadJSONFromMIME(headers http.Header, input io.ReadCloser, into interface{}) error {
	err := ReadJSONFromMIMERawErr(headers, input, into)
	if err != nil {
		err = base.WrapJSONUnknownFieldErr(err)
		if errors.Cause(err) == base.ErrUnknownField {
			err = base.HTTPErrorf(http.StatusBadRequest, "JSON Unknown Field: %s", err.Error())
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Bad JSON: %s", err.Error())
		}
	}
	return err
}

func ReadJSONFromMIMERawErr(headers http.Header, input io.ReadCloser, into interface{}) error {
	input, err := processContentEncoding(headers, input, "application/json")
	if err != nil {
		return err
	}

	// Decode the body bytes into target structure.
	decoder := base.JSONDecoder(input)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	err = decoder.Decode(into)
	_ = input.Close()

	return err
}

// processContentEncoding performs the Content-Type validation and Content-Encoding check.
func processContentEncoding(headers http.Header, input io.ReadCloser, expectedContentTypeMime string) (io.ReadCloser, error) {
	contentType := headers.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, expectedContentTypeMime) {
		return input, base.HTTPErrorf(http.StatusUnsupportedMediaType, "Invalid content type %s - expected %s", contentType, expectedContentTypeMime)
	}
	switch headers.Get("Content-Encoding") {
	case "gzip":
		var err error
		if input, err = gzip.NewReader(input); err != nil {
			return input, err
		}
	case "":
		break
	default:
		return input, base.HTTPErrorf(http.StatusUnsupportedMediaType, "Unsupported Content-Encoding; use gzip")
	}
	return input, nil
}

type attInfo struct {
	name        string
	contentType string
	data        []byte
}

func writeJSONPart(writer *multipart.Writer, contentType string, body db.Body, compressed bool) (err error) {
	bytes, err := base.JSONMarshalCanonical(body)
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
		_ = gz.Close()
	} else {
		_, err = part.Write(bytes)
	}
	return
}

// Writes a revision to a MIME multipart writer, encoding large attachments as separate parts.
func WriteMultipartDocument(ctx context.Context, cblReplicationPullStats *base.CBLReplicationPullStats, body db.Body, writer *multipart.Writer, compress bool) {
	// First extract the attachments that should follow:
	following := []attInfo{}
	for name, value := range db.GetBodyAttachments(body) {
		meta := value.(map[string]interface{})
		if meta["stub"] != true {
			var err error
			var info attInfo
			info.contentType, _ = meta["content_type"].(string)
			info.data, err = db.DecodeAttachment(meta["data"])
			if info.data == nil {
				base.WarnfCtx(ctx, "Couldn't decode attachment %q of doc %q: %v", base.UD(name), base.UD(body[db.BodyId]), err)
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
	_ = writeJSONPart(writer, "application/json", body, compress)

	// Write the following attachments
	for _, info := range following {
		partHeaders := textproto.MIMEHeader{}
		if info.contentType != "" {
			partHeaders.Set("Content-Type", info.contentType)
		}
		partHeaders.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", info.name))
		part, _ := writer.CreatePart(partHeaders)
		if cblReplicationPullStats != nil {
			cblReplicationPullStats.AttachmentPullCount.Add(1)
			cblReplicationPullStats.AttachmentPullBytes.Add(int64(len(info.data)))
		}
		_, _ = part.Write(info.data)

	}
}

func hasInlineAttachments(body db.Body) bool {
	for _, value := range db.GetBodyAttachments(body) {
		if meta, ok := value.(map[string]interface{}); ok && meta["data"] != nil {
			return true
		}
	}
	return false
}

// Adds a new part to the given multipart writer, containing the given revision.
// The revision will be written as a nested multipart body if it has attachments.
func WriteRevisionAsPart(ctx context.Context, cblReplicationPullStats *base.CBLReplicationPullStats, revBody db.Body, isError bool, compressPart bool, writer *multipart.Writer) error {
	partHeaders := textproto.MIMEHeader{}
	docID, _ := revBody[db.BodyId].(string)
	revID, _ := revBody[db.BodyRev].(string)
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
		WriteMultipartDocument(ctx, cblReplicationPullStats, revBody, docWriter, compressPart)
		_ = docWriter.Close()
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

func ReadMultipartDocument(reader *multipart.Reader) (db.Body, error) {
	// First read the main JSON document body:
	mainPart, err := reader.NextPart()
	if err != nil {
		return nil, err
	}
	var body db.Body
	err = ReadJSONFromMIME(http.Header(mainPart.Header), mainPart, &body)
	if err != nil {
		return nil, err
	}

	// Collect the attachments with a "follows" property, which will appear as MIME parts:
	followingAttachments := map[string]map[string]interface{}{}
	for name, value := range db.GetBodyAttachments(body) {
		if meta := value.(map[string]interface{}); meta["follows"] == true {
			followingAttachments[name] = meta
		}
	}

	// Subroutine to look up a following attachment given its digest. (I used to pre-compute a
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
		_ = part.Close()
		if err != nil {
			return nil, err
		}

		// Look up the attachment by its digest:
		digest := db.Sha1DigestKey(data)
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

// This is only here for backwards compatibility.  Otherwise should be avoided.
func md5DigestKey(data []byte) string {
	digester := md5.New()
	digester.Write(data)
	return "md5-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}
