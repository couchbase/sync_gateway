//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

// If set to true, JSON output will be pretty-printed.
var PrettyPrint bool = false

// If set to true, diagnostic data will be dumped if there's a problem with MIME multipart data
var DebugMultipart bool = false

var lastSerialNum uint64 = 0

func init() {
	DebugMultipart = (os.Getenv("GatewayDebugMultipart") != "")
}

var kNotFoundError = &base.HTTPError{http.StatusNotFound, "missing"}
var kBadMethodError = &base.HTTPError{http.StatusMethodNotAllowed, "Method Not Allowed"}
var kBadRequestError = &base.HTTPError{http.StatusMethodNotAllowed, "Bad Request"}

// Encapsulates the state of handling an HTTP request.
type handler struct {
	server       *serverContext
	context      *context
	rq           *http.Request
	response     http.ResponseWriter
	db           *db.Database
	user         auth.User
	admin        bool
	startTime    time.Time
	serialNumber uint64
}

type handlerMethod func(*handler) error

// Creates an http.Handler that will run a handler with the given method
func makeAdminHandler(server *serverContext, method handlerMethod) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		h := newHandler(server, r, rq)
		h.admin = true
		err := h.invoke(method)
		h.writeError(err)
	})
}

// Creates an http.Handler that will run a handler with the given method
func makeHandler(server *serverContext, method handlerMethod) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		h := newHandler(server, r, rq)
		err := h.invoke(method)
		h.writeError(err)
	})
}

func newHandler(server *serverContext, r http.ResponseWriter, rq *http.Request) *handler {
	h := &handler{
		server:       server,
		rq:           rq,
		response:     r,
		serialNumber: atomic.AddUint64(&lastSerialNum, 1),
	}
	if base.LogKeys["HTTP+"] {
		h.startTime = time.Now()
	}
	return h
}

// Top-level handler call. It's passed a pointer to the specific method to run.
func (h *handler) invoke(method handlerMethod) error {
	base.LogTo("HTTP", " #%03d: %s %s", h.serialNumber, h.rq.Method, h.rq.URL)
	h.setHeader("Server", VersionString)

	// If there is a "db" path variable, look up the database context:
	if dbname, ok := h.PathVars()["db"]; ok {
		h.context = h.server.databases[dbname]
		if h.context == nil {
			return &base.HTTPError{http.StatusNotFound, "no such database"}
		}
	}

	// Authenticate; admin handlers can ignore missing credentials
	if err := h.checkAuth(); err != nil {
		if !h.admin {
			return err
		}
	}

	// Now look up the database:
	if h.context != nil {
		var err error
		h.db, err = db.GetDatabase(h.context.dbcontext, h.user)
		if err != nil {
			return err
		}
	}

	return method(h) // Call the actual handler code
}

func (h *handler) checkAuth() error {
	h.user = nil
	if h.context == nil || h.context.auth == nil {
		return nil
	}

	// Check cookie first, then HTTP auth:
	var err error
	h.user, err = h.context.auth.AuthenticateCookie(h.rq)
	if err != nil {
		return err
	}
	var userName, password string
	if h.user == nil {
		userName, password = h.getBasicAuth()
		h.user = h.context.auth.AuthenticateUser(userName, password)
	}

	if h.user == nil && !h.admin {
		cookie, _ := h.rq.Cookie(auth.CookieName)
		base.Log("Auth failed for username=%q, cookie=%q", userName, cookie)
		h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
		return &base.HTTPError{http.StatusUnauthorized, "Invalid login"}
	}
	return nil
}

func (h *handler) PathVars() map[string]string {
	return mux.Vars(h.rq)
}

func (h *handler) getQuery(query string) string {
	return h.rq.URL.Query().Get(query)
}

func (h *handler) getBoolQuery(query string) bool {
	return h.getQuery(query) == "true"
}

// Returns the integer value of a URL query, defaulting to 0 if missing or unparseable
func (h *handler) getIntQuery(query string, defaultValue uint64) (value uint64) {
	value = defaultValue
	q := h.getQuery(query)
	if q != "" {
		value, _ = strconv.ParseUint(q, 10, 64)
	}
	return
}

// Parses a JSON request body, returning it as a Body map.
func (h *handler) readJSON() (db.Body, error) {
	var body db.Body
	return body, db.ReadJSONFromMIME(h.rq.Header, h.rq.Body, &body)
}

func (h *handler) readDocument() (db.Body, error) {
	contentType, attrs, _ := mime.ParseMediaType(h.rq.Header.Get("Content-Type"))
	switch contentType {
	case "", "application/json":
		return h.readJSON()
	case "multipart/related":
		if DebugMultipart {
			raw, err := ioutil.ReadAll(h.rq.Body)
			if err != nil {
				return nil, err
			}
			reader := multipart.NewReader(bytes.NewReader(raw), attrs["boundary"])
			body, err := db.ReadMultipartDocument(reader)
			if err != nil {
				ioutil.WriteFile("GatewayPUT.mime", raw, 0600)
				base.Warn("Error reading MIME data: copied to file GatewayPUT.mime")
			}
			return body, err
		} else {
			reader := multipart.NewReader(h.rq.Body, attrs["boundary"])
			return db.ReadMultipartDocument(reader)
		}
	}
	return nil, &base.HTTPError{http.StatusUnsupportedMediaType, "Invalid content type " + contentType}
}

func (h *handler) requestAccepts(mimetype string) bool {
	accept := h.rq.Header.Get("Accept")
	return accept == "" || strings.Contains(accept, mimetype) || strings.Contains(accept, "*/*")
}

func (h *handler) getBasicAuth() (username string, password string) {
	auth := h.rq.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Basic ") {
		decoded, err := base64.StdEncoding.DecodeString(auth[6:])
		if err == nil {
			components := strings.SplitN(string(decoded), ":", 2)
			if len(components) == 2 {
				return components[0], components[1]
			}
		}
	}
	return
}

//////// RESPONSES:

func (h *handler) setHeader(name string, value string) {
	h.response.Header().Set(name, value)
}

func (h *handler) logStatus(status int, message string) {
	if base.LogKeys["HTTP+"] {
		duration := float64(time.Since(h.startTime)) / float64(time.Millisecond)
		base.LogTo("HTTP+", "#%03d:     --> %d %s  (%.1f ms)",
			h.serialNumber, status, message, duration)
	}
}

// Writes an object to the response in JSON format.
// If status is nonzero, the header will be written with that status.
func (h *handler) writeJSONStatus(status int, value interface{}) {
	if !h.requestAccepts("application/json") {
		base.Warn("Client won't accept JSON, only %s", h.rq.Header.Get("Accept"))
		h.writeStatus(http.StatusNotAcceptable, "only application/json available")
		return
	}

	jsonOut, err := json.Marshal(value)
	if err != nil {
		base.Warn("Couldn't serialize JSON for %v", value)
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
			h.logStatus(status, "")
		}
		h.response.Write(jsonOut)
	} else if status > 0 {
		h.response.WriteHeader(status)
		h.logStatus(status, "")
	}
}

func (h *handler) writeJSON(value interface{}) {
	h.writeJSONStatus(http.StatusOK, value)
}

func (h *handler) addJSON(value interface{}) {
	jsonOut, err := json.Marshal(value)
	if err != nil {
		base.Warn("Couldn't serialize JSON for %v", value)
		panic("JSON serialization failed")
	}
	h.response.Write(jsonOut)
}

func (h *handler) writeMultipart(callback func(*multipart.Writer) error) error {
	if !h.requestAccepts("multipart/") {
		return &base.HTTPError{Status: http.StatusNotAcceptable}
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

func (h *handler) write(line []byte) error {
	_, err := h.response.Write(line)
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
		status, message := base.ErrorAsHTTPStatus(err)
		h.writeStatus(status, message)
	}
}

// Writes the response status code, and if it's an error writes a JSON description to the body.
func (h *handler) writeStatus(status int, message string) {
	if status < 300 {
		h.response.WriteHeader(status)
		h.logStatus(status, message)
		return
	}
	// Got an error:
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
	base.LogTo("HTTP", " #%03d:     --> %d %s", h.serialNumber, status, message)
	jsonOut, _ := json.Marshal(db.Body{"error": errorStr, "reason": message})
	h.response.Write(jsonOut)
}
