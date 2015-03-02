package rest

import (
	"compress/gzip"
	"crypto/md5"
	"crypto/rand"
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
	"github.com/couchbase/sync_gateway/db"
	"github.com/snej/zdelta-go"
)

// Attachments shorter than this will be left in the JSON as base64 rather than being a separate
// MIME part.
var MaxInlineAttachmentSize = 200

// MIME parts (JSON bodies or attachments) smaller than this won't be GZip-encoded.
var MinCompressiblePartSize = 300

//////// WRITING:

// Writes a revision to a MIME multipart writer, encoding large attachments as separate parts.
func WriteMultipartDocument(r db.RevResponse, writer *multipart.Writer, compress bool) error {
	// First extract the attachments that should follow:
	for _, att := range r.Attachments {
		if data, err := att.LoadData(true); err != nil {
			return err
		} else if len(data) > MaxInlineAttachmentSize || att.IsDelta() {
			att.SetFollows()
		}
	}

	// Write the main JSON body:
	if err := writeJSONPart(r, "application/json", compress, writer); err != nil {
		return err
	}

	// Write the following attachments
	for _, att := range r.Attachments {
		if att.Follows() {
			err := writePart(att.Data(), compress && att.Compressible(), att.Headers(false), writer)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Adds a new part to the given multipart writer, containing the given revision.
// The revision will be written as a nested multipart body if it has attachments.
func WriteRevisionAsPart(r db.RevResponse, isError bool, compress bool, writer *multipart.Writer) error {
	partHeaders := textproto.MIMEHeader{}
	docID, _ := r.Body["_id"].(string)
	revID, _ := r.Body["_rev"].(string)
	if len(docID) > 0 {
		partHeaders.Set("X-Doc-ID", docID)
		partHeaders.Set("X-Rev-ID", revID)
	}

	if hasInlineAttachments(r.Body) {
		// Write doc as multipart, including attachments:
		docWriter, err := createNestedMultipart(writer, "related", partHeaders)
		if err == nil {
			err = WriteMultipartDocument(r, docWriter, compress)
			if err == nil {
				err = docWriter.Close()
			}
		}
		return err
	} else {
		// Write doc as a single JSON part:
		contentType := "application/json"
		if isError {
			contentType += `; error="true"`
			r.OldRevJSON = nil // disable delta compression
			compress = false   // and gzip compression
		}
		return writeJSONPart(r, contentType, compress, writer)
	}
}

// Writes the JSON body of a revision as a part to a multipart writer.
func writeJSONPart(r db.RevResponse, contentType string, gzipCompress bool, writer *multipart.Writer) error {
	bytes, err := json.Marshal(r.Body)
	if err != nil {
		return err
	}

	partHeaders := textproto.MIMEHeader{}
	partHeaders.Set("Content-Type", contentType)

	if r.OldRevJSON != nil && len(bytes) > db.MinDeltaSavings {
		gzipCompress = false
		delta, err := zdelta.CreateDelta(r.OldRevJSON, bytes)
		if err == nil && len(delta)+db.MinDeltaSavings < len(bytes) {
			bytes = delta
			partHeaders.Set("Content-Encoding", "zdelta")
			partHeaders.Set("X-Delta-Source", r.OldRevID)
		}
	}
	return writePart(bytes, gzipCompress, partHeaders, writer)
}

func writePart(bytes []byte, gzipCompress bool, partHeaders textproto.MIMEHeader, writer *multipart.Writer) error {
	if gzipCompress {
		if len(bytes) < MinCompressiblePartSize {
			gzipCompress = false
		} else {
			partHeaders.Set("Content-Encoding", "gzip")
		}
	}

	part, err := writer.CreatePart(partHeaders)
	if err != nil {
		return err
	}

	if gzipCompress {
		gz := gzip.NewWriter(part)
		_, err = gz.Write(bytes)
		gz.Close()
	} else {
		_, err = part.Write(bytes)
	}
	return err
}

//////// READING:

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

// Reads a document from a multipart MIME body: first the JSON part, then any attachments
func ReadMultipartDocument(reader *multipart.Reader) (db.Body, error) {
	// First read the main JSON document body:
	mainPart, err := reader.NextPart()
	if err != nil {
		return nil, err
	}
	var body db.Body
	err = ReadJSONFromMIME(http.Header(mainPart.Header), mainPart, &body)
	mainPart.Close()
	if err != nil {
		return nil, err
	}

	// Collect the attachments with a "follows" property, which will appear as MIME parts:
	followingAttachments := map[string]map[string]interface{}{}
	for name, value := range body.Attachments() {
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
		digest := string(db.SHA1DigestKey(data))
		name, meta := findFollowingAttachment(digest)
		if meta == nil {
			name, meta = findFollowingAttachment(md5DigestKey(data)) // CouchDB uses MD5 :-p
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

///////// HELPERS:

// CouchDB-format attachment digest string
func md5DigestKey(data []byte) string {
	digester := md5.New()
	digester.Write(data)
	return "md5-" + base64.StdEncoding.EncodeToString(digester.Sum(nil))
}

// Does this Body contain any attachments with a "data" property?
func hasInlineAttachments(body db.Body) bool {
	for _, value := range body.Attachments() {
		if meta, ok := value.(map[string]interface{}); ok && meta["data"] != nil {
			return true
		}
	}
	return false
}

// Creates a multipart writer as a nested part in another writer.
func createNestedMultipart(mpWriter *multipart.Writer, multipartSubType string, headers textproto.MIMEHeader) (*multipart.Writer, error) {
	boundary := randomBoundary()
	headers.Set("Content-Type", fmt.Sprintf("multipart/%s; boundary=%q", multipartSubType, boundary))
	part, err := mpWriter.CreatePart(headers)
	if err != nil {
		return nil, err
	}
	partWriter := multipart.NewWriter(part)
	partWriter.SetBoundary(boundary)
	return partWriter, nil
}

// copied from Go source: src/mime/multipart/writer.go
func randomBoundary() string {
	var buf [30]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf[:])
}
