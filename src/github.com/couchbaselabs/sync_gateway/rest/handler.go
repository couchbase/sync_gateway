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
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
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

var restExpvars = expvar.NewMap("syncGateway_rest")

func init() {
	DebugMultipart = (os.Getenv("GatewayDebugMultipart") != "")
}

var kNotFoundError = base.HTTPErrorf(http.StatusNotFound, "missing")
var kBadMethodError = base.HTTPErrorf(http.StatusMethodNotAllowed, "Method Not Allowed")
var kBadRequestError = base.HTTPErrorf(http.StatusMethodNotAllowed, "Bad Request")

// Encapsulates the state of handling an HTTP request.
type handler struct {
	server         *ServerContext
	rq             *http.Request
	response       http.ResponseWriter
	status         int
	statusMessage  string
	requestBody    io.ReadCloser
	db             *db.Database
	user           auth.User
	privs          handlerPrivs
	startTime      time.Time
	serialNumber   uint64
	loggedDuration bool
}

type handlerPrivs int

const (
	regularPrivs = iota // Handler requires authentication
	publicPrivs         // Handler checks auth but doesn't require it
	adminPrivs          // Handler ignores auth, always runs with root/admin privs
)

type handlerMethod func(*handler) error

// Creates an http.Handler that will run a handler with the given method
func makeHandler(server *ServerContext, privs handlerPrivs, method handlerMethod) http.Handler {
	return http.HandlerFunc(func(r http.ResponseWriter, rq *http.Request) {
		h := newHandler(server, privs, r, rq)
		err := h.invoke(method)
		h.writeError(err)
		h.logDuration(true)
	})
}

func newHandler(server *ServerContext, privs handlerPrivs, r http.ResponseWriter, rq *http.Request) *handler {
	return &handler{
		server:       server,
		privs:        privs,
		rq:           rq,
		response:     r,
		status:       http.StatusOK,
		serialNumber: atomic.AddUint64(&lastSerialNum, 1),
		startTime:    time.Now(),
	}
}

// Top-level handler call. It's passed a pointer to the specific method to run.
func (h *handler) invoke(method handlerMethod) error {
	restExpvars.Add("requests_total", 1)
	restExpvars.Add("requests_active", 1)
	defer restExpvars.Add("requests_active", -1)

	var err error
	if h.server.config.CompressResponses == nil || *h.server.config.CompressResponses {
		if encoded := NewEncodedResponseWriter(h.response, h.rq); encoded != nil {
			h.response = encoded
			defer encoded.Close()
		}
	}

	switch h.rq.Header.Get("Content-Encoding") {
	case "":
		h.requestBody = h.rq.Body
	case "gzip":
		if h.requestBody, err = gzip.NewReader(h.rq.Body); err != nil {
			return err
		}
		h.rq.Header.Del("Content-Encoding") // to prevent double decoding later on
	default:
		return base.HTTPErrorf(http.StatusUnsupportedMediaType, "Unsupported Content-Encoding; use gzip")
	}

	h.setHeader("Server", VersionString)

	// If there is a "db" path variable, look up the database context:
	var dbContext *db.DatabaseContext
	if dbname := h.PathVar("db"); dbname != "" {
		if dbContext, err = h.server.GetDatabase(dbname); err != nil {
			h.logRequestLine()
			return err
		}
	}

	// Authenticate, if not on admin port:
	if h.privs != adminPrivs {
		if err = h.checkAuth(dbContext); err != nil {
			h.logRequestLine()
			return err
		}
	}

	h.logRequestLine()

	// Now set the request's Database (i.e. context + user)
	if dbContext != nil {
		h.db, err = db.GetDatabase(dbContext, h.user)
		if err != nil {
			return err
		}
	}

	return method(h) // Call the actual handler code
}

func (h *handler) logRequestLine() {
	if !base.LogKeys["HTTP"] {
		return
	}
	as := ""
	if h.privs == adminPrivs {
		as = "  (ADMIN)"
	} else if h.user != nil && h.user.Name() != "" {
		as = fmt.Sprintf("  (as %s)", h.user.Name())
	}
	base.LogTo("HTTP", " #%03d: %s %s%s", h.serialNumber, h.rq.Method, h.rq.URL, as)
}

func (h *handler) logDuration(realTime bool) {
	if h.loggedDuration {
		return
	}
	h.loggedDuration = true

	var duration time.Duration
	if realTime {
		duration = time.Since(h.startTime)
		bin := int(duration/(100*time.Millisecond)) * 100
		restExpvars.Add(fmt.Sprintf("requests_%04dms", bin), 1)
	}

	logKey := "HTTP+"
	if h.status >= 300 {
		logKey = "HTTP"
	}
	base.LogTo(logKey, "#%03d:     --> %d %s  (%.1f ms)",
		h.serialNumber, h.status, h.statusMessage,
		float64(duration)/float64(time.Millisecond))
}

// Used for indefinitely-long handlers like _changes that we don't want to track duration of
func (h *handler) logStatus(status int, message string) {
	h.setStatus(status, message)
	h.logDuration(false) // don't track actual time
}

func (h *handler) checkAuth(context *db.DatabaseContext) error {
	h.user = nil
	if context == nil {
		return nil
	}

	// Check cookie first:
	var err error
	h.user, err = context.Authenticator().AuthenticateCookie(h.rq)
	if err != nil {
		return err
	} else if h.user != nil {
		return nil
	}

	// If no cookie, check HTTP auth:
	if userName, password := h.getBasicAuth(); userName != "" {
		h.user = context.Authenticator().AuthenticateUser(userName, password)
		if h.user == nil {
			base.Log("HTTP auth failed for username=%q", userName)
			h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
			return base.HTTPErrorf(http.StatusUnauthorized, "Invalid login")
		}
		return nil
	}

	// No auth given -- check guest access
	if h.user, err = context.Authenticator().GetUser(""); err != nil {
		return err
	}
	if h.privs == regularPrivs && h.user.Disabled() {
		h.response.Header().Set("WWW-Authenticate", `Basic realm="Couchbase Sync Gateway"`)
		return base.HTTPErrorf(http.StatusUnauthorized, "Login required")
	}

	return nil
}

func (h *handler) assertAdminOnly() {
	if h.privs != adminPrivs {
		panic("Admin-only handler called without admin privileges, on " + h.rq.RequestURI)
	}
}

func (h *handler) PathVar(name string) string {
	v := mux.Vars(h.rq)[name]
	// Before routing the URL we explicitly disabled expansion of %-escapes in the path
	// (see function fixQuotedSlashes). So we have to unescape them now.
	v, _ = url.QueryUnescape(v)
	return v
}

func (h *handler) SetPathVar(name string, value string) {
	mux.Vars(h.rq)[name] = url.QueryEscape(value)
}

func (h *handler) getQuery(query string) string {
	return h.rq.URL.Query().Get(query)
}

func (h *handler) getBoolQuery(query string) bool {
	return h.getQuery(query) == "true"
}

// Returns the integer value of a URL query, defaulting to 0 if unparseable
func (h *handler) getIntQuery(query string, defaultValue uint64) (value uint64) {
	return h.getRestrictedIntQuery(query, defaultValue, 0, 0)
}

// Returns the integer value of a URL query, restricted to a min and max value,
// but returning 0 if missing or unparseable
func (h *handler) getRestrictedIntQuery(query string, defaultValue, minValue, maxValue uint64) uint64 {
	value := defaultValue
	q := h.getQuery(query)
	if q != "" {
		var err error
		value, err = strconv.ParseUint(q, 10, 64)
		if err != nil {
			value = 0
		} else if value < minValue {
			value = minValue
		} else if value > maxValue && maxValue > 0 {
			value = maxValue
		}
	}
	return value
}

func (h *handler) userAgentIs(agent string) bool {
	userAgent := h.rq.Header.Get("User-Agent")
	return len(userAgent) > len(agent) && userAgent[len(agent)] == '/' && strings.HasPrefix(userAgent, agent)
}

// Returns the request body as a raw byte array.
func (h *handler) readBody() ([]byte, error) {
	return ioutil.ReadAll(h.requestBody)
}

// Parses a JSON request body, returning it as a Body map.
func (h *handler) readJSON() (db.Body, error) {
	var body db.Body
	return body, h.readJSONInto(&body)
}

// Parses a JSON request body into a custom structure.
func (h *handler) readJSONInto(into interface{}) error {
	return db.ReadJSONFromMIME(h.rq.Header, h.requestBody, into)
}

// Reads & parses the request body, handling either JSON or multipart.
func (h *handler) readDocument() (db.Body, error) {
	contentType, attrs, _ := mime.ParseMediaType(h.rq.Header.Get("Content-Type"))
	switch contentType {
	case "", "application/json":
		return h.readJSON()
	case "multipart/related":
		if DebugMultipart {
			raw, err := h.readBody()
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
			reader := multipart.NewReader(h.requestBody, attrs["boundary"])
			return db.ReadMultipartDocument(reader)
		}
	default:
		return nil, base.HTTPErrorf(http.StatusUnsupportedMediaType, "Invalid content type %s", contentType)
	}
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

// Registers a new user account based on the given verified email address.
// Username will be the same as the verified email address. Password will be random.
// The user will have access to no channels.
func (h *handler) registerNewUser(email string) (auth.User, error) {
	user, err := h.db.Authenticator().NewUser(email, base.GenerateRandomSecret(), base.Set{})
	if err != nil {
		return nil, err
	}
	user.SetEmail(email)
	err = h.db.Authenticator().Save(user)
	if err != nil {
		return nil, err
	}
	return user, err
}

//////// RESPONSES:

func (h *handler) setHeader(name string, value string) {
	h.response.Header().Set(name, value)
}

func (h *handler) setStatus(status int, message string) {
	h.status = status
	h.statusMessage = message
}

func (h *handler) disableResponseCompression() {
	switch r := h.response.(type) {
	case *EncodedResponseWriter:
		r.disableCompression()
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
		if len(jsonOut) < 1000 {
			h.disableResponseCompression()
		}
		h.setHeader("Content-Length", fmt.Sprintf("%d", len(jsonOut)))
		if status > 0 {
			h.response.WriteHeader(status)
			h.setStatus(status, "")
		}
		h.response.Write(jsonOut)
	} else if status > 0 {
		h.response.WriteHeader(status)
		h.setStatus(status, "")
	}
}

func (h *handler) writeJSON(value interface{}) {
	h.writeJSONStatus(http.StatusOK, value)
}

func (h *handler) addJSON(value interface{}) {
	encoder := json.NewEncoder(h.response)
	err := encoder.Encode(value)
	if err != nil {
		base.Warn("Couldn't serialize JSON for %v", value)
		panic("JSON serialization failed")
	}
}

func (h *handler) writeMultipart(subtype string, callback func(*multipart.Writer) error) error {
	if !h.requestAccepts("multipart/") {
		return base.HTTPErrorf(http.StatusNotAcceptable, "Response is multipart")
	}

	// Get the output stream. Due to a CouchDB bug, if we're sending to it we need to buffer the
	// output in memory so we can trim the final bytes.
	var output io.Writer
	var buffer bytes.Buffer
	if h.userAgentIs("CouchDB") {
		output = &buffer
	} else {
		output = h.response
	}

	writer := multipart.NewWriter(output)
	h.setHeader("Content-Type",
		fmt.Sprintf("multipart/%s; boundary=%q", subtype, writer.Boundary()))

	err := callback(writer)
	writer.Close()

	if err == nil && output == &buffer {
		// Trim trailing newline; CouchDB is allergic to it:
		_, err = h.response.Write(bytes.TrimRight(buffer.Bytes(), "\r\n"))
	}
	return err
}

func (h *handler) flush() {
	switch r := h.response.(type) {
	case http.Flusher:
		r.Flush()
	}
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
		h.setStatus(status, message)
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

	h.disableResponseCompression()
	h.setHeader("Content-Type", "application/json")
	h.response.WriteHeader(status)
	h.setStatus(status, message)
	jsonOut, _ := json.Marshal(db.Body{"error": errorStr, "reason": message})
	h.response.Write(jsonOut)
}
