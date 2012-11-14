//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channelsync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
)

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

// If set to true, HTTP requests will be logged
var LogRequests bool = true
var LogRequestsVerbose bool = false

var kNotFoundError = &HTTPError{http.StatusNotFound, "missing"}
var kBadMethodError = &HTTPError{http.StatusMethodNotAllowed, "Method Not Allowed"}

// Encapsulates the state of handling an HTTP request.
type handler struct {
	rq       *http.Request
	response http.ResponseWriter
	bucket   *couchbase.Bucket
	db       *Database
}

// Creates an http.Handler that will handle the REST API for the given bucket.
func NewRESTHandler(bucket *couchbase.Bucket) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		h := &handler{rq: rq, response: r, bucket: bucket}
		h.run()
	})
}

// Returns the integer value of a URL query, defaulting to 0 if missing or unparseable
func (h *handler) getIntQuery(query string, defaultValue uint64) (value uint64) {
	value = defaultValue
	q := h.rq.URL.Query().Get(query)
	if q != "" {
		value, _ = strconv.ParseUint(q, 10, 64)
	}
	return
}

// Parses a JSON request body, unmarshaling it into "into".
func readJSONInto(headers http.Header, input io.Reader, into interface{}) error {
	contentType := headers.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "application/json") {
		return &HTTPError{http.StatusUnsupportedMediaType, "Invalid content type " + contentType}
	}
	body, err := ioutil.ReadAll(input)
	if err != nil {
		return &HTTPError{http.StatusBadRequest, ""}
	}
	err = json.Unmarshal(body, into)
	if err != nil {
		log.Printf("WARNING: Couldn't parse JSON:\n%s", body)
		return &HTTPError{http.StatusBadRequest, "Bad JSON"}
	}
	return nil
}

// Parses a JSON request body, returning it as a Body map.
func (h *handler) readJSON() (Body, error) {
	var body Body
	return body, readJSONInto(h.rq.Header, h.rq.Body, &body)
}

func (h *handler) readDocument() (Body, error) {
	contentType, attrs, _ := mime.ParseMediaType(h.rq.Header.Get("Content-Type"))
	switch contentType {
	case "", "application/json":
		return h.readJSON()
	case "multipart/related":
		reader := multipart.NewReader(h.rq.Body, attrs["boundary"])
		return readMultipartDocument(reader)
	}
	return nil, &HTTPError{http.StatusUnsupportedMediaType, "Invalid content type " + contentType}
}

func (h *handler) requestAccepts(mimetype string) bool {
	accept := h.rq.Header.Get("Accept")
	return accept == "" || strings.Contains(accept, mimetype) || strings.Contains(accept, "*/*")
}

func (h *handler) setHeader(name string, value string) {
	h.response.Header().Set(name, value)
}

func (h *handler) logStatus(status int) {
	if LogRequestsVerbose {
		var message string
		if status >= 300 {
			message = "*** "
		}
		log.Printf("\t--> %d %s", status, message)
	}
}

// Writes an object to the response in JSON format.
func (h *handler) writeJSONStatus(status int, value interface{}) {
	if !h.requestAccepts("application/json") {
		log.Printf("WARNING: Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	jsonOut, err := json.Marshal(value)
	if err != nil {
		log.Printf("WARNING: Couldn't serialize JSON for %v", value)
		h.writeStatus(http.StatusInternalServerError, "JSON serialization failed")
		return
	}
	if PrettyPrint {
		var buffer bytes.Buffer
		json.Indent(&buffer, jsonOut, "", "  ")
		jsonOut = append(buffer.Bytes(), '\n')
	}
	h.setHeader("Content-Type", "application/json")
	if h.rq.Method != "HEAD" {
		h.setHeader("Content-Length", fmt.Sprintf("%d", len(jsonOut)))
		if status > 0 {
			h.response.WriteHeader(status)
			h.logStatus(status)
		}
		h.response.Write(jsonOut)
	} else if status > 0 {
		h.response.WriteHeader(status)
		h.logStatus(status)
	}
}

func (h *handler) writeJSON(value interface{}) {
	h.writeJSONStatus(0, value)
}

func (h *handler) writeMultipart(callback func(*multipart.Writer) error) error {
	if !h.requestAccepts("multipart/") {
		return &HTTPError{Status: http.StatusNotAcceptable}
	}
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	h.setHeader("Content-Type",
		fmt.Sprintf("multipart/related; boundary=%q", writer.Boundary()))

	err := callback(writer)
	writer.Close()

	if err == nil {
		// Trim trailing newline; CouchDB is allergic to it:
		_, err = h.response.Write(bytes.TrimRight(buffer.Bytes(), "\r\n"))
	}
	return err
}

func (h *handler) writeln(line []byte) error {
	_, err := h.response.Write(line)
	if err == nil {
		_, err = h.response.Write([]byte("\r\n"))
	}
	if err == nil {
		switch r := h.response.(type) {
		case http.Flusher:
			r.Flush()
		}
	}
	return err
}

// If the error parameter is non-nil, sets the response status code appropriately and
// writes a CouchDB-style JSON description to the body.
func (h *handler) writeError(err error) {
	if err != nil {
		status, message := ErrorAsHTTPStatus(err)
		h.writeStatus(status, message)
	}
}

// Writes the response status code, and if it's an error writes a JSON description to the body.
func (h *handler) writeStatus(status int, message string) {
	if status < 300 {
		h.response.WriteHeader(status)
		h.logStatus(status)
		return
	}
	var errorStr string
	switch status {
	case http.StatusNotFound:
		errorStr = "not_found"
	case http.StatusConflict:
		errorStr = "conflict"
	default:
		errorStr = http.StatusText(status)
		if errorStr == "" {
			errorStr = fmt.Sprintf("%d", status)
		}
	}

	h.setHeader("Content-Type", "application/json")
	h.response.WriteHeader(status)
	h.logStatus(status)
	jsonOut, _ := json.Marshal(Body{"error": errorStr, "reason": message})
	h.response.Write(jsonOut)
}
